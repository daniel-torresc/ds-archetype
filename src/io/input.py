from pyspark.sql import SparkSession, DataFrame
from src.io.io import IO
from src.utils.constants import COLUMNS, FILTER, SAMPLE
from src.utils.exceptions import TableNotFoundError
from src.utils.utils import convert_to_list


class Input(IO):

    def __init__(self, spark: SparkSession, name: str, env: str, config: dict):
        super().__init__(spark=spark, name=name, env=env, config=config)

        self.columns: list = convert_to_list(config.get(COLUMNS))
        self.filter: str = config.get(FILTER)
        self.sample: float = config.get(SAMPLE)

        self.df: DataFrame = self._read()

    def _read(self) -> DataFrame:
        if not self.exists():
            raise TableNotFoundError(table_name=self.table)

        return self._data_source.read(filter_by=self.filter, columns=self.columns, sample=self.sample)
