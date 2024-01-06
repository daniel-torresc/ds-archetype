import os
from pyspark.sql import SparkSession
from datetime import datetime
import logging
import yaml
from src.utils.constants import MASTER, APP_NAME, CONFIG, PROPERTIES, TIMESTAMP_FORMAT, RESOURCES, DEFAULT


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _get_spark_conf(config: dict) -> dict:
    with open(os.path.join("src", "conf", "spark.yaml")) as f:
        spark_conf = yaml.safe_load(f)

    final_config = {
        APP_NAME: config[APP_NAME],
        MASTER: config.get(MASTER, spark_conf[MASTER]),
        CONFIG: spark_conf[RESOURCES][DEFAULT],
    }

    if isinstance(config[PROPERTIES], str):
        final_config[CONFIG].update(spark_conf[config[PROPERTIES]])
    elif isinstance(config[PROPERTIES], dict):
        final_config[CONFIG].update(config[PROPERTIES])
    else:
        raise TypeError(f"{PROPERTIES} value type '{type(config[PROPERTIES])}' not supported")

    return final_config


def _log_spark_conf(spark_conf: dict):
    logger.info(f"## {APP_NAME.upper()}: {spark_conf[APP_NAME]}")
    logger.info(f"## {MASTER.upper()}: {spark_conf[MASTER]}")
    logger.info(f"## {datetime.now().strftime(TIMESTAMP_FORMAT)}")

    conf_log = "## SPARK SESSION REQUESTED CONFIGURATION\n"
    for key, value in spark_conf[CONFIG].items():
        conf_log += f"{CONFIG} {key}={value}\n"

    logger.info(conf_log)


def get_or_create(config: dict) -> SparkSession:
    spark_conf = _get_spark_conf(config=config)

    spark_builder = SparkSession.builder \
        .master(spark_conf[MASTER]) \
        .appName(spark_conf[APP_NAME]) \
        .enableHiveSupport()

    for config_key, config_val in spark_conf[CONFIG].items():
        spark_builder.config(config_key, config_val)

    spark = spark_builder.getOrCreate()

    _log_spark_conf(spark_conf=spark_conf)

    return spark
