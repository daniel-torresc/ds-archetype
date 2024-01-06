import logging
from abc import ABC, abstractmethod
from src.utils.decorators import timeit
from src.utils.constants import CREATE, OVERWRITE, OVERWRITE_PARTITION, APPEND
from pyspark.sql import DataFrame, SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataSource(ABC):
    """
    A base class for data sources.

    Args:
        spark (SparkSession): The SparkSession object.
        schema (str): The schema name.
        table (str): The table name.

    Attributes:
        spark (SparkSession): The SparkSession object.
        schema (str): The schema name.
        table (str): The table name.
        schema_table (str): The fully qualified table name.

    Methods:
        read(filter_by=None, columns=None, sample=None) -> DataFrame:
            Reads data from the data source and returns a DataFrame.
        write(df, mode=APPEND, partition_cols=None):
            Writes the DataFrame to the data source with the specific mode.
        _write_create(df, partition_cols):
            Writes the DataFrame creating the data source.
        _write_overwrite(df, partition_cols):
            Writes the DataFrame overwriting the data source.
        _write_overwrite_partition(df, partition_cols):
            Writes the DataFrame overwriting a data source's partition.
        _write_append(df, partition_cols):
            Writes the DataFrame appending it to the existing data source.
        drop():
            Drops the data source.
        drop_partition(partition_value_clause):
            Drops a partition from the data source.
        exists() -> bool:
            Checks if the data source exists.
        get_partitions() -> list[dict]:
            Returns a list of existing partitions in the data source.

    Static Methods:
        _filter_select_sample(df, filter_by, columns, sample) -> DataFrame:
            Filters, selects columns, and samples the DataFrame.

    Magic Methods:
        __repr__():
            Returns a string representation of the data source.
        __str__():
            Returns a string representation of the data source.
    """

    def __init__(self, spark: SparkSession, schema: str, table: str):
        """
        Initializes a DataSource object.

        Args:
            spark (SparkSession): The SparkSession object.
            schema (str): The schema name.
            table (str): The table name.
        """
        self.spark: SparkSession = spark
        self.schema: str = schema
        self.table: str = table
        self.schema_table: str = f"{schema}.{table}"

    @timeit
    @abstractmethod
    def read(self, filter_by: str = None, columns: list = None, sample: float = None) -> DataFrame:
        """
        Reads data from a source and returns a DataFrame.

        Args:
            filter_by (str, optional): A string to filter the data by. Defaults to None.
            columns (list, optional): A list of columns to include in the DataFrame. Defaults to None.
            sample (float, optional): The fraction of data to sample. Defaults to None.

        Returns:
            DataFrame: The DataFrame containing the read data.

        Raises:
            NotImplementedError: This method is not implemented and should be overridden by subclasses.
        """
        raise NotImplementedError

    @timeit
    def write(self, df: DataFrame, mode: str = APPEND, partition_cols: list = None):
        """
        Writes the DataFrame to the data source.

        Args:
            df (DataFrame): The DataFrame to be written.
            mode (str, optional): The write mode. Defaults to APPEND.
            partition_cols (list, optional): A list of partition columns. Defaults to None.
        """
        logger.info(f"Write into '{self.schema_table} with mode '{mode}'")

        # Columns must always have the same specific order to persist.
        # Otherwise, values are changed between variables.

        if self.exists():
            df = df.select(self.read().columns)

        switcher = {
            CREATE: self._write_create,
            OVERWRITE: self._write_overwrite,
            OVERWRITE_PARTITION: self._write_overwrite_partition,
            APPEND: self._write_append,
        }

        switcher[mode](df=df, partition_cols=partition_cols)

    @abstractmethod
    def _write_create(self, df: DataFrame, partition_cols: list = None):
        """
        Writes the DataFrame creating the data source.

        Args:
            df (DataFrame): The DataFrame to be written.
            partition_cols (list): A list of partition columns.

        Raises:
            NotImplementedError: This method is not implemented and should be overridden by subclasses.
        """
        raise NotImplementedError

    @abstractmethod
    def _write_overwrite(self, df: DataFrame, partition_cols: list = None):
        """
        Writes the DataFrame overwriting the data source.

        Args:
            df (DataFrame): The DataFrame to be written.
            partition_cols (list): A list of partition columns.

        Raises:
            NotImplementedError: This method is not implemented and should be overridden by subclasses.
        """
        raise NotImplementedError

    @abstractmethod
    def _write_overwrite_partition(self, df: DataFrame, partition_cols: list = None):
        """
        Writes the DataFrame overwriting a data source's partition.

        Args:
            df (DataFrame): The DataFrame to be written.
            partition_cols (list): A list of partition columns.

        Raises:
            NotImplementedError: This method is not implemented and should be overridden by subclasses.
        """
        raise NotImplementedError

    @abstractmethod
    def _write_append(self, df: DataFrame, partition_cols: list = None):
        """
        Writes the DataFrame appending it to the existing data source.

        Args:
            df (DataFrame): The DataFrame to be written.
            partition_cols (list): A list of partition columns.

        Raises:
            NotImplementedError: This method is not implemented and should be overridden by subclasses.
        """
        raise NotImplementedError

    @timeit
    @abstractmethod
    def drop(self):
        """
        Drops the data source.

        Raises:
            NotImplementedError: This method is not implemented and should be overridden by subclasses.
        """
        raise NotImplementedError

    @timeit
    @abstractmethod
    def drop_partition(self, partition_value_clause: dict):
        """
        Drops a partition from the data source.

        Args:
            partition_value_clause (dict): A dictionary representing the partition value clause.

        Raises:
            NotImplementedError: This method is not implemented and should be overridden by subclasses.
        """
        raise NotImplementedError

    @abstractmethod
    def exists(self) -> bool:
        """
        Checks if the data source exists.

        Returns:
            bool: True if the data source exists, False otherwise.

        Raises:
            NotImplementedError: This method is not implemented and should be overridden by subclasses.
        """
        raise NotImplementedError

    @timeit
    @abstractmethod
    def get_partitions(self) -> list[dict]:
        """
        Returns a list of partitions in the data source.

        Returns:
            list[dict]: A list of dictionaries representing the partitions.

        Raises:
            NotImplementedError: This method is not implemented and should be overridden by subclasses.
        """
        raise NotImplementedError

    @staticmethod
    def _filter_select_sample(df: DataFrame, filter_by: str, columns: list, sample: float) -> DataFrame:
        """
        Filters, selects columns, and samples the DataFrame.

        Args:
            df (DataFrame): The DataFrame to be processed.
            filter_by (str): A string to filter the DataFrame by.
            columns (list): A list of columns to select from the DataFrame.
            sample (float): The fraction of data to sample.

        Returns:
            DataFrame: The processed DataFrame.
        """
        if filter_by:
            logger.info(f"FILTER: '{filter_by}'")
            df = df.filter(condition=filter_by)

        if columns:
            logger.info(f"COLUMNS: '{columns}'")
            df = df.select(columns)

        if sample:
            logger.info(f"SAMPLE: {sample}")
            df = df.sample(fraction=sample, seed=42)

        return df

    def __repr__(self):
        return f"{self.__class__.__name__}: {self.schema_table}"

    def __str__(self):
        return self.__repr__()
