from pyspark.sql import SparkSession
import os
import sys
from pyspark.sql import SparkSession
from src.main.utility.logging_config import logger


def spark_session():
    jdbc_jar_path = "D:\\mysql-connector-java-8.0.26\\mysql-connector-java-8.0.26\\mysql-connector-java-8.0.26.jar"
    spark = SparkSession.builder \
        .appName("Shop_Project") \
        .config("spark.jars", jdbc_jar_path) \
        .config("spark.ui.port", "8089")\
        .getOrCreate()
    logger.info("spark session %s",spark)
    return spark

