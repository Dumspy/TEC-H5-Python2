"""Transform the extracted Iris CSV with PySpark."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

IRIS_SETOSA = "Iris-setosa"
DEFAULT_INPUT_PATH = Path("Input_dir/iris.csv")
MYSQL_JDBC_PACKAGE = "com.mysql:mysql-connector-j:8.4.0"

IRIS_SCHEMA = StructType(
    [
        StructField("sepal_length", DoubleType(), nullable=False),
        StructField("sepal_width", DoubleType(), nullable=False),
        StructField("petal_length", DoubleType(), nullable=False),
        StructField("petal_width", DoubleType(), nullable=False),
        StructField("species", StringType(), nullable=False),
    ]
)


def create_spark_session(app_name: str = "iris-etl") -> SparkSession:
    """Create a local Spark session for the ETL application.

    The explicit driver binding makes local Spark startup predictable on
    macOS, where hostname resolution can otherwise cause gateway failures.
    """

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        # Spark needs Connector/J available before the JVM starts so that
        # transformed DataFrames can be loaded into MySQL through JDBC.
        .config("spark.jars.packages", MYSQL_JDBC_PACKAGE)
        .getOrCreate()
    )


def transform_iris(
    spark: SparkSession,
    input_path: Path = DEFAULT_INPUT_PATH,
) -> DataFrame:
    """Read the source CSV and keep only Iris-setosa observations."""

    input_path = Path(input_path)
    if not input_path.is_file():
        raise FileNotFoundError(f"Input CSV does not exist: {input_path}")

    source_df = (
        spark.read.option("header", "true")
        .option("mode", "FAILFAST")
        .schema(IRIS_SCHEMA)
        .csv(str(input_path))
    )

    return source_df.filter(F.col("species") == IRIS_SETOSA).select(
        "sepal_length",
        "sepal_width",
        "petal_length",
        "petal_width",
        "species",
    )
