"""Filter the extracted Iris CSV with PySpark."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .dataset import IRIS_DATASET


def transform_iris(
    spark: SparkSession,
    input_path: Path = IRIS_DATASET.input_path,
) -> DataFrame:
    """Read the source CSV and keep only Iris-setosa rows."""

    input_path = Path(input_path)
    if not input_path.is_file():
        raise FileNotFoundError(f"Input CSV does not exist: {input_path}")

    source = (
        spark.read.option("header", "true")
        .option("mode", "FAILFAST")
        .schema(IRIS_DATASET.spark_schema)
        .csv(str(input_path))
    )
    return source.filter(
        F.col(IRIS_DATASET.species_column) == IRIS_DATASET.species_filter
    ).select(*IRIS_DATASET.columns)
