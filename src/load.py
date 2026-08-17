"""Load transformed data into the local CSV output and MySQL table."""

from __future__ import annotations

import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile

import mysql.connector
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import DataFrame

DEFAULT_OUTPUT_PATH = Path("Output_dir/transform_iris.csv")
DEFAULT_TABLE = "iris_setosa"
EXPECTED_COLUMNS = (
    "sepal_length",
    "sepal_width",
    "petal_length",
    "petal_width",
    "species",
)
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class DatabaseConfig:
    """Connection settings loaded from the local .env file."""

    host: str
    port: int
    database: str
    user: str
    password: str
    table: str

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:mysql://{self.host}:{self.port}/{self.database}"
            "?serverTimezone=UTC&allowPublicKeyRetrieval=true&useSSL=false"
        )

    @property
    def jdbc_properties(self) -> dict[str, str]:
        return {
            "user": self.user,
            "password": self.password,
            "driver": "com.mysql.cj.jdbc.Driver",
        }


def get_database_config() -> DatabaseConfig:
    """Read the simple local database configuration from .env."""

    load_dotenv()
    return DatabaseConfig(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "3306")),
        database=os.getenv("DB_NAME", "iris_db"),
        user=os.getenv("DB_USER", "iris_app"),
        password=os.getenv("DB_PASSWORD", "change_this_application_password"),
        table=os.getenv("DB_TABLE", DEFAULT_TABLE),
    )


def _validated_table_name(table: str) -> str:
    """Validate the configured SQL identifier before interpolating it."""

    if not _IDENTIFIER_PATTERN.fullmatch(table):
        raise ValueError(f"Invalid MySQL table name: {table!r}")
    return table


def write_transformed_csv(
    dataframe: DataFrame,
    output_path: Path = DEFAULT_OUTPUT_PATH,
) -> Path:
    """Write a single header-containing CSV file and replace old output."""

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = output_path.parent / f".{output_path.stem}.spark-output"
    temporary_file: Path | None = None

    shutil.rmtree(temporary_dir, ignore_errors=True)
    try:
        (
            dataframe.select(*EXPECTED_COLUMNS)
            .coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(str(temporary_dir))
        )

        part_files = sorted(temporary_dir.glob("part-*.csv"))
        if len(part_files) != 1:
            raise RuntimeError(
                f"Expected one Spark CSV part file, found {len(part_files)}"
            )

        with NamedTemporaryFile(
            prefix=f".{output_path.name}.",
            suffix=".tmp",
            dir=output_path.parent,
            delete=False,
        ) as temporary:
            temporary_file = Path(temporary.name)

        shutil.copyfile(part_files[0], temporary_file)
        temporary_file.replace(output_path)
        temporary_file = None
        return output_path
    finally:
        if temporary_file is not None:
            temporary_file.unlink(missing_ok=True)
        shutil.rmtree(temporary_dir, ignore_errors=True)


def _ensure_table(config: DatabaseConfig) -> None:
    """Create the destination table and remove rows from the previous run."""

    table = f"`{_validated_table_name(config.table)}`"
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            sepal_length DOUBLE NOT NULL,
            sepal_width DOUBLE NOT NULL,
            petal_length DOUBLE NOT NULL,
            petal_width DOUBLE NOT NULL,
            species VARCHAR(50) NOT NULL
        )
    """
    truncate_table_sql = f"TRUNCATE TABLE {table}"

    connection = mysql.connector.connect(
        host=config.host,
        port=config.port,
        database=config.database,
        user=config.user,
        password=config.password,
    )
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(create_table_sql)
            cursor.execute(truncate_table_sql)
            connection.commit()
        finally:
            cursor.close()
    finally:
        connection.close()


def query_transformed_data(
    config: DatabaseConfig | None = None,
) -> pd.DataFrame:
    """Read the transformed rows from MySQL into a pandas DataFrame."""

    config = config or get_database_config()
    table = f"`{_validated_table_name(config.table)}`"
    query = f"""
        SELECT sepal_length, sepal_width, petal_length, petal_width, species
        FROM {table}
        ORDER BY sepal_length, sepal_width, petal_length, petal_width
    """

    connection = mysql.connector.connect(
        host=config.host,
        port=config.port,
        database=config.database,
        user=config.user,
        password=config.password,
    )
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        finally:
            cursor.close()
    finally:
        connection.close()

    return pd.DataFrame.from_records(rows, columns=EXPECTED_COLUMNS)


def write_transformed_mysql(
    dataframe: DataFrame,
    config: DatabaseConfig | None = None,
) -> str:
    """Create/refresh the table and append the transformed Spark DataFrame."""

    config = config or get_database_config()
    _ensure_table(config)

    table = _validated_table_name(config.table)
    (
        dataframe.select(*EXPECTED_COLUMNS)
        .write.mode("append")
        .jdbc(
            url=config.jdbc_url,
            table=table,
            properties=config.jdbc_properties,
        )
    )
    return config.table
