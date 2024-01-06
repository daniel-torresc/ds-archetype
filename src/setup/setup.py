from pyspark.sql import SparkSession
from src.spark import spark_session
from src.utils.constants import INPUT, OUTPUT, PRO
from src.io.input import Input
from src.io.output import Output


def _in_setup(spark: SparkSession, in_config: dict, env: str) -> dict[str, Input]:
    return {name: Input(spark=spark, name=name, env=env, config=in_config[name]) for name in in_config}


def _out_setup(spark: SparkSession, out_config: dict, env: str) -> dict[str, Output]:
    if not out_config:
        return {}

    return {name: Output(spark=spark, name=name, env=env, config=out_config[name]) for name in out_config}


def workspace_setup(spark_config: dict, io_config: dict, env: str = PRO) -> tuple[SparkSession, dict[str, Input], dict[str, Output]]:
    spark = spark_session.get_or_create(config=spark_config)

    in_ds = _in_setup(spark=spark, in_config=io_config[INPUT], env=env)

    out_ds = _out_setup(spark=spark, out_config=io_config[OUTPUT], env=env)

    return spark, in_ds, out_ds
