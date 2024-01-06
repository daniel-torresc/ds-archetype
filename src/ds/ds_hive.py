import logging
from src.ds.ds import DataSource
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataSourceHive(DataSource):

    def __init__(self, spark: SparkSession, schema: str, table: str):
        super().__init__(spark=spark, schema=schema, table=table)

    def read(self, filter_by: str = None, columns: list = None, sample: float = None) -> DataFrame:
        logger.info(f"Read from {self.schema_table}")

        df = self.spark.table(tableName=self.schema_table)
        df = self._filter_select_sample(df=df, filter_by=filter_by, columns=columns, sample=sample)

        return df

    def _write_create(self, df: DataFrame, partition_cols: list = None):
        if partition_cols:
            df.write.saveAsTable(name=self.schema_table, mode='errorifexists', partitionBy=partition_cols)
        else:
            df.write.saveAsTable(name=self.schema_table, mode='errorifexists')

    def _write_overwrite(self, df: DataFrame, partition_cols: list = None):
        if partition_cols:
            df.write.saveAsTable(name=self.schema_table, mode='overwrite', partitionBy=partition_cols)
        else:
            df.write.saveAsTable(name=self.schema_table, mode='overwrite')

    def _write_overwrite_partition(self, df: DataFrame, partition_cols: list = None):
        if self.exists():
            df.write \
                .mode('append') \
                .insertInto(tableName=self.schema_table, overwrite=True)
        else:
            self._write_create(df=df, partition_cols=partition_cols)

    def _write_append(self, df: DataFrame, partition_cols: list = None):
        if self.exists():
            df.write \
                .mode('append') \
                .insertInto(tableName=self.schema_table, overwrite=False)
        else:
            self._write_create(df=df, partition_cols=partition_cols)

    def drop(self):
        logger.info(f"Drop table {self.schema_table}")

        query = f"DROP TABLE IF EXISTS {self.schema_table} PURGE"
        self.spark.sql(query)

    def drop_partition(self, partition_value_clause: dict):
        clauses = [f"{k} = {v}" if isinstance(v, (int, float)) else f"{k} = {v}" for k, v in partition_value_clause.items()]
        partition_clause = ", ".join(clauses)

        logger.info(f"{self.__repr__()} - Drop partition {partition_clause}")

        query = f"ALTER TABLE {self.schema_table} DROP IF EXISTS PARTITION ({partition_clause}) PURGE"
        self.spark.sql(query)

    def exists(self) -> bool:
        self.spark.sql(f"USE {self.schema}")
        return self.spark.sql(f"SHOW TABLES LIKE '{self.table}'").count() > 0

    def get_partitions(self) -> list[dict]:
        try:
            query = f"SHOW PARTITIONS {self.schema_table}"
            partitions = self.spark.sql(query).collect()
        except AnalysisException:
            logger.warning(f"Table {self.schema_table} has no partitions")
            return []

        partition_list = []
        for partition in partitions:
            clause_str = partition.partition.split("/")
            clause_dict = {}
            for value in clause_str:
                values = value.split("=")
                clause_dict[values[0]] = values[1]

            partition_list.append(clause_dict)

        return partition_list
