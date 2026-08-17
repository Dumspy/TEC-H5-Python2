"""MySQL configuration, table management, loading, and queries."""

from __future__ import annotations

import os
import re
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import cast

import mysql.connector
from mysql.connector.connection import MySQLConnection
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import DataFrame

from ..dataset import IRIS_DATASET

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


def _ensure_table(config: DatabaseConfig) -> None:
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


def write_dataframe(
    dataframe: DataFrame,
    config: DatabaseConfig | None = None,
) -> str:
    """Create or refresh the configured table with a Spark DataFrame."""

    config = config or get_database_config()
    _ensure_table(config)
    dataframe.select(*IRIS_DATASET.columns).write.mode("append").jdbc(
        url=config.jdbc_url,
        table=_table_name(config.table),
        properties=config.jdbc_properties,
    )
    return config.table


def query_dataframe(
    config: DatabaseConfig | None = None,
) -> pd.DataFrame:
    """Read the configured table into pandas."""

    config = config or get_database_config()
    table = _table_name(config.table, quoted=True)
    columns = ", ".join(IRIS_DATASET.columns)
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

    return pd.DataFrame.from_records(rows, columns=IRIS_DATASET.columns)
