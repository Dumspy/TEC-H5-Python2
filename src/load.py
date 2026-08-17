"""Assignment-facing load operations."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame

from .lib.mysql import DatabaseConfig, query_dataframe, write_dataframe
from .lib.spark_io import DEFAULT_OUTPUT_PATH, write_single_csv


def write_transformed_csv(
    dataframe: DataFrame,
    output_path: Path = DEFAULT_OUTPUT_PATH,
) -> Path:
    """Write the transformed DataFrame to one CSV file."""

    return write_single_csv(dataframe, output_path)


def write_transformed_mysql(
    dataframe: DataFrame,
    config: DatabaseConfig | None = None,
) -> str:
    """Create or refresh the MySQL table with the transformed DataFrame."""

    return write_dataframe(dataframe, config)


def query_transformed_data(
    config: DatabaseConfig | None = None,
) -> pd.DataFrame:
    """Read transformed rows from MySQL for visualization."""

    return query_dataframe(config)
