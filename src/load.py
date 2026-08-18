"""Assignment-facing encrypted load operations."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame

from .dataset import IRIS_DATASET
from .lib.mysql import (
    DatabaseConfig,
    query_encrypted_batches,
    write_encrypted_rows,
)
from .lib.serialization import (
    decrypted_rows,
    encrypted_rows,
)
from .lib.serialization import (
    write_encrypted_csv as write_csv_payload,
)
from .security import EncryptionMethod


def write_encrypted_csv(
    dataframe: DataFrame,
    method: EncryptionMethod,
    key: bytes,
    output_path: Path = IRIS_DATASET.encrypted_output_path,
) -> Path:
    """Write the transformed columns encrypted to a CSV file."""

    return write_csv_payload(dataframe, output_path, method, key)


def write_encrypted_mysql(
    dataframe: DataFrame,
    method: EncryptionMethod,
    key: bytes,
    config: DatabaseConfig | None = None,
) -> str:
    """Encrypt transformed columns and store them in MySQL batches."""

    return write_encrypted_rows(encrypted_rows(dataframe, method, key), config)


def query_decrypted_data(
    method: EncryptionMethod,
    key: bytes,
    config: DatabaseConfig | None = None,
    batch_size: int = 100,
) -> pd.DataFrame:
    """Read encrypted MySQL columns in batches and decrypt them for plotting."""

    rows = decrypted_rows(
        query_encrypted_batches(config, batch_size),
        method,
        key,
    )
    dataframe = pd.DataFrame.from_records(rows, columns=IRIS_DATASET.columns)
    for column in IRIS_DATASET.measurement_columns:
        dataframe[column] = pd.to_numeric(dataframe[column])
    return dataframe
