import logging
from src.ds.ds import DataSource
from pyspark.sql import SparkSession, DataFrame
from py4j.protocol import Py4JJavaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataSourceKudu(DataSource):
    """
    Data source implementation for Kudu.

    This class provides functionality to read and write data from and to Kudu tables. It extends the base DataSource
    class.

    Args:
        spark: The SparkSession object.
        schema: The name of the schema.
        table: The name of the table.
        kudu_masters: The Kudu masters address.
        kudu_context: The Kudu context.

    Returns:
        The initialized DataSourceKudu object.

    Raises:
        NotImplementedError: If any of the non-implemented abstract methods are called.
    """

    def __init__(self, spark: SparkSession, schema: str, table: str, kudu_masters: str = None, kudu_context: str = None):
        """
        Initialize a DataSourceKudu object.

        This method initializes a DataSourceKudu object with the provided SparkSession, schema, table, Kudu masters
        address, and Kudu context.

        Args:
            spark: The SparkSession object.
            schema: The name of the schema.
            table: The name of the table.
            kudu_masters: The Kudu masters address.
            kudu_context: The Kudu context.
        """
        super().__init__(spark=spark, schema=schema, table=table)
        self.km = kudu_masters
        self.kc = kudu_context

    def read(self, filter_by: str = None, columns: list = None, sample: float = None) -> DataFrame:
        """
        Read data from the Kudu table.

        This method reads data from the Kudu table specified by the schema and table name. It applies an optional
        filter, selects specific columns, and samples the data if specified.

        Args:
            filter_by: The filter condition to apply to the data (default: None).
            columns: The list of columns to select from the data (default: None).
            sample: The fraction of data to sample (default: None).

        Returns:
            The DataFrame containing the read data.
        """
        logger.info(f"Read from {self.schema_table}")

        try:
            df = self.spark.read \
                        .format("org.apache.kudu.spark.kudu") \
                        .option("kudu.master", self.km) \
                        .option("kudu.table", f"impala::{self.schema_table}") \
                        .load()
        except Py4JJavaError:
            df = self.spark.read \
                .format("org.apache.kudu.spark.kudu") \
                .option("kudu.master", self.km) \
                .option("kudu.table", self.schema_table) \
                .load()

        df = self._filter_select_sample(df=df, filter_by=filter_by, columns=columns, sample=sample)

        return df

    def _write_create(self, df: DataFrame, partition_cols: list = None):
        """
        Create operation for writing data to the Kudu table.

        This method is responsible for performing the create operation to write data to the Kudu table. It takes a
        DataFrame as input and optional partition columns.

        Args:
            df: The DataFrame containing the data to be written.
            partition_cols: The list of partition columns (default: None).

        Raises:
            NotImplementedError: The method is not implemented in the derived class yet.
        """
        raise NotImplementedError

    def _write_overwrite(self, df: DataFrame, partition_cols: list = None):
        raise NotImplementedError

    def _write_overwrite_partition(self, df: DataFrame, partition_cols: list = None):
        raise NotImplementedError

    def _write_append(self, df: DataFrame, partition_cols: list = None):
        if not self.exists():
            pass

        if partition_cols:
            df.write \
                .format("org.apache.kudu.spark.kudu") \
                .option("kudu.master", self.km) \
                .option("kudu.table", f"impala::{self.schema_table}") \
                .partitionBy(partition_cols) \
                .mode('append') \
                .save()
        else:
            df.write \
                .format("org.apache.kudu.spark.kudu") \
                .option("kudu.master", self.km) \
                .option("kudu.table", f"impala::{self.schema_table}") \
                .mode('append') \
                .save()

    def drop(self):
        raise NotImplementedError

    def drop_partition(self, partition_value_clause: dict):
        raise NotImplementedError

    def exists(self) -> bool:
        raise NotImplementedError

    def get_partitions(self) -> list[dict]:
        raise NotImplementedError
