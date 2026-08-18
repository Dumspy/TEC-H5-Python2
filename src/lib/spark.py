"""Shared Spark session setup."""

from __future__ import annotations

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "iris-etl") -> SparkSession:
    """Create a local Spark session for the transformation stage."""

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
