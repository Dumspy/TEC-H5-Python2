"""Filter the extracted Iris CSV with PySpark."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from .constants import IRIS_COLUMNS, IRIS_SETOSA

DEFAULT_INPUT_PATH = Path("Input_dir/iris.csv")
IRIS_SCHEMA = StructType(
    [
        StructField("sepal_length", DoubleType(), nullable=False),
        StructField("sepal_width", DoubleType(), nullable=False),
        StructField("petal_length", DoubleType(), nullable=False),
        StructField("petal_width", DoubleType(), nullable=False),
        StructField("species", StringType(), nullable=False),
    ]
)


def transform_iris(
    spark: SparkSession,
    input_path: Path = DEFAULT_INPUT_PATH,
) -> DataFrame:
    """Read the source CSV and keep only Iris-setosa rows."""

    input_path = Path(input_path)
    if not input_path.is_file():
        raise FileNotFoundError(f"Input CSV does not exist: {input_path}")

    source = (
        spark.read.option("header", "true")
        .option("mode", "FAILFAST")
        .schema(IRIS_SCHEMA)
        .csv(str(input_path))
    )
    return source.filter(F.col("species") == IRIS_SETOSA).select(*IRIS_COLUMNS)
