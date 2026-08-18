"""MySQL configuration and encrypted-column storage."""

from __future__ import annotations

import os
import re
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from typing import cast

import mysql.connector
from dotenv import load_dotenv
from mysql.connector.connection import MySQLConnection

from ..dataset import IRIS_DATASET

DEFAULT_TABLE = "iris_setosa_encrypted"
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


def ensure_encrypted_table(config: DatabaseConfig) -> None:
    """Create and clear the Part 2 table without touching the Part 1 table."""

    table = _table_name(config.table, quoted=True)
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            sepal_length VARBINARY(255) NOT NULL,
            sepal_width VARBINARY(255) NOT NULL,
            petal_length VARBINARY(255) NOT NULL,
            petal_width VARBINARY(255) NOT NULL,
            species VARBINARY(255) NOT NULL
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


def write_encrypted_rows(
    rows: Iterable[tuple[bytes, ...]],
    config: DatabaseConfig | None = None,
    batch_size: int = 100,
) -> str:
    """Insert encrypted rows into the separate Part 2 table in batches."""

    config = config or get_database_config()
    ensure_encrypted_table(config)
    table = _table_name(config.table, quoted=True)
    insert_sql = f"""
        INSERT INTO {table}
        (sepal_length, sepal_width, petal_length, petal_width, species)
        VALUES (%s, %s, %s, %s, %s)
    """

    with _connection(config) as connection:
        cursor = connection.cursor()
        try:
            batch: list[tuple[bytes, ...]] = []
            for row in rows:
                batch.append(row)
                if len(batch) >= batch_size:
                    cursor.executemany(insert_sql, batch)
                    batch.clear()
            if batch:
                cursor.executemany(insert_sql, batch)
            connection.commit()
        finally:
            cursor.close()
    return config.table


def query_encrypted_batches(
    config: DatabaseConfig | None = None,
    batch_size: int = 100,
) -> Generator[tuple[bytes, ...], None, None]:
    """Yield encrypted columns from MySQL in batches for decryption."""

    config = config or get_database_config()
    table = _table_name(config.table, quoted=True)
    columns = ", ".join(IRIS_DATASET.columns)
    query = f"SELECT {columns} FROM {table} ORDER BY id"

    with _connection(config) as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            while batch := cursor.fetchmany(batch_size):
                for row in batch:
                    yield cast(tuple[bytes, ...], row)
        finally:
            cursor.close()
