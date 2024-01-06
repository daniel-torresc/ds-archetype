import getpass
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def create_audit(df: DataFrame):
    username = getpass.getuser()
    timestamp = F.current_timestamp()

    return df \
        .withColumn('insert_time', F.lit(timestamp)) \
        .withColumn('update_time', F.lit(timestamp)) \
        .withColumn('insert_user', F.lit(username)) \
        .withColumn('update_user', F.lit(username))


def update_audit(df: DataFrame):
    username = getpass.getuser()
    timestamp = F.current_timestamp()

    return df \
        .withColumn('update_time', F.lit(timestamp)) \
        .withColumn('update_user', F.lit(username))


def remove_audit(df: DataFrame):
    return df.drop('insert_time', 'update_time', 'insert_user', 'update_user')
