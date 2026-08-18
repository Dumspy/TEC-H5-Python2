"""Serialize and encrypt transformed DataFrame rows."""

from __future__ import annotations

import base64
import csv
from collections.abc import Iterator
from pathlib import Path
from tempfile import NamedTemporaryFile

from pyspark.sql import DataFrame

from ..dataset import IRIS_DATASET
from ..security import EncryptionMethod, decrypt, encrypt


def encrypted_rows(
    dataframe: DataFrame,
    method: EncryptionMethod,
    key: bytes,
) -> Iterator[tuple[bytes, ...]]:
    """Yield one encrypted byte payload per dataset column."""

    for row in dataframe.select(*IRIS_DATASET.columns).toLocalIterator():
        yield tuple(
            encrypt(str(row[column]).encode("utf-8"), method, key)
            for column in IRIS_DATASET.columns
        )


def decrypted_rows(
    rows: Iterator[tuple[bytes, ...]],
    method: EncryptionMethod,
    key: bytes,
) -> Iterator[tuple[str, ...]]:
    """Decrypt database rows back into string values."""

    for row in rows:
        yield tuple(
            decrypt(value, method, key).decode("utf-8")
            for value in row
        )


def write_encrypted_csv(
    dataframe: DataFrame,
    output_path: Path,
    method: EncryptionMethod,
    key: bytes,
) -> Path:
    """Write headers and Base64-encoded encrypted values to a CSV file."""

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None

    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            prefix=f".{output_path.name}.",
            suffix=".tmp",
            dir=output_path.parent,
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            writer = csv.writer(temporary)
            writer.writerow(IRIS_DATASET.columns)
            for row in encrypted_rows(dataframe, method, key):
                writer.writerow(base64.b64encode(value).decode("ascii") for value in row)

        temporary_path.replace(output_path)
        temporary_path = None
        return output_path
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
