"""Shared Spark session setup."""

from __future__ import annotations

from pyspark.sql import SparkSession

MYSQL_JDBC_PACKAGE = "com.mysql:mysql-connector-j:8.4.0"


def create_spark_session(app_name: str = "iris-etl") -> SparkSession:
    """Create a local Spark session with the MySQL JDBC driver."""

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.jars.packages", MYSQL_JDBC_PACKAGE)
        .getOrCreate()
    )
