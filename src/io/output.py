from pyspark.sql import SparkSession, DataFrame
from src.io.io import IO
from src.utils.constants import PARTITION_COLS, MODE
from src.utils.exceptions import TableNotFoundError
from src.utils.utils import convert_to_list


class Output(IO):

    def __init__(self, spark: SparkSession, name: str, env: str, config: dict):
        super().__init__(spark=spark, name=name, env=env, config=config)

        self.partition_cols: list = convert_to_list(config.get(PARTITION_COLS))
        self.mode: str = config.get(MODE)

    def write(self, df: DataFrame):
        return self._data_source.write(df=df, mode=self.mode, partition_cols=self.partition_cols)

    def drop(self):
        return self._data_source.drop()

    def drop_partition(self, partition_value_clause: dict):
        if not self.exists():
            raise TableNotFoundError(table_name=self.table)

        self._data_source.drop_partition(partition_value_clause=partition_value_clause)
