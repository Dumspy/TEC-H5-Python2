"""Load transformed data into CSV and MySQL."""

from __future__ import annotations

import os
import re
import shutil
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import cast

import mysql.connector
from mysql.connector.connection import MySQLConnection
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import DataFrame

from .constants import IRIS_COLUMNS

DEFAULT_OUTPUT_PATH = Path("Output_dir/transform_iris.csv")
DEFAULT_TABLE = "iris_setosa"
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class DatabaseConfig:
    """MySQL connection settings."""

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
    """Read database settings from .env."""

    load_dotenv()
    return DatabaseConfig(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "3306")),
        database=os.getenv("DB_NAME", "iris_db"),
        user=os.getenv("DB_USER", "iris_app"),
        password=os.getenv("DB_PASSWORD", "change_this_application_password"),
        table=os.getenv("DB_TABLE", DEFAULT_TABLE),
    )


def _table_name(name: str, quoted: bool = False) -> str:
    if not _IDENTIFIER_PATTERN.fullmatch(name):
        raise ValueError(f"Invalid MySQL table name: {name!r}")
    return f"`{name}`" if quoted else name


@contextmanager
def _connection(config: DatabaseConfig) -> Generator[MySQLConnection, None, None]:
    connection = cast(
        MySQLConnection,
        mysql.connector.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password,
        ),
    )
    try:
        yield connection
    finally:
        connection.close()


def _select_columns(dataframe: DataFrame) -> DataFrame:
    return dataframe.select(*IRIS_COLUMNS)


def write_transformed_csv(
    dataframe: DataFrame,
    output_path: Path = DEFAULT_OUTPUT_PATH,
) -> Path:
    """Write one header-containing CSV file and replace old output."""

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = output_path.parent / f".{output_path.stem}.spark-output"
    temporary_file: Path | None = None

    shutil.rmtree(temporary_dir, ignore_errors=True)
    try:
        (
            _select_columns(dataframe)
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
    """Create the table if needed and clear the previous data."""

    table = _table_name(config.table, quoted=True)
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            sepal_length DOUBLE NOT NULL,
            sepal_width DOUBLE NOT NULL,
            petal_length DOUBLE NOT NULL,
            petal_width DOUBLE NOT NULL,
            species VARCHAR(50) NOT NULL
        )
    """

    with _connection(config) as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(create_sql)
            cursor.execute(f"TRUNCATE TABLE {table}")
            connection.commit()
        finally:
            cursor.close()


def write_transformed_mysql(
    dataframe: DataFrame,
    config: DatabaseConfig | None = None,
) -> str:
    """Create/refresh the table and append the transformed Spark DataFrame."""

    config = config or get_database_config()
    _ensure_table(config)
    _select_columns(dataframe).write.mode("append").jdbc(
        url=config.jdbc_url,
        table=_table_name(config.table),
        properties=config.jdbc_properties,
    )
    return config.table


def query_transformed_data(
    config: DatabaseConfig | None = None,
) -> pd.DataFrame:
    """Read the transformed rows from MySQL into pandas."""

    config = config or get_database_config()
    table = _table_name(config.table, quoted=True)
    columns = ", ".join(IRIS_COLUMNS)
    query = f"""
        SELECT {columns}
        FROM {table}
        ORDER BY sepal_length, sepal_width, petal_length, petal_width
    """

    with _connection(config) as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        finally:
            cursor.close()

    return pd.DataFrame.from_records(rows, columns=IRIS_COLUMNS)
