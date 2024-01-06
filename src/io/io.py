import os
from abc import ABC
from pyspark.sql import SparkSession
import yaml
from src.ds.ds import DataSource
from src.ds.ds_hive import DataSourceHive
from src.ds.ds_kudu import DataSourceKudu
from src.utils.constants import SOURCE, SCHEMA, TABLE_NAME, HIVE, KUDU, KUDU_MASTERS
from src.utils.exceptions import InvalidDataSourceError, TableNotFoundError


class IO(ABC):

    def __init__(self, spark: SparkSession, name: str, env: str, config: dict):
        self._spark: SparkSession = spark

        self.name: str = name
        self._env: str = env

        self._source: str = config[SOURCE].lower()
        self.schema: str = config[SCHEMA].lower()
        self.table: str = config[TABLE_NAME].lower()
        self.schema_table: str = f"{config[SCHEMA].lower()}.{config[TABLE_NAME].lower()}"

        with open(os.path.join(os.getcwd(), "src", "conf", "envs.yaml")) as f:
            env_values = yaml.safe_load(f)

        self._km: str = env_values[env][KUDU_MASTERS]

        self._data_source: DataSource = self._get_data_source()

    def _get_data_source(self):
        if hasattr(self, '_data_source'):
            return self._data_source

        if self._source == HIVE:
            return DataSourceHive(spark=self._spark, schema=self.schema, table=self.table)
        elif self._source == KUDU:
            return DataSourceKudu(spark=self._spark, schema=self.schema, table=self.table, kudu_masters=self._km)
        else:
            raise InvalidDataSourceError(data_source=self._source)

    def exists(self) -> bool:
        return self._data_source.exists()

    def get_partitions(self) -> list:
        if not self.exists():
            raise TableNotFoundError(table_name=self.table)

        return self._data_source.get_partitions()

    def __repr__(self):
        return f"{self.__class__.__name__}: {self.name} -> {self.schema_table}"

    def __str__(self):
        return self.__repr__()
