"""Spark DataFrame file output helpers."""

from __future__ import annotations

import shutil
from pathlib import Path
from tempfile import NamedTemporaryFile

from pyspark.sql import DataFrame

from ..dataset import IRIS_DATASET


def write_single_csv(
    dataframe: DataFrame,
    output_path: Path = IRIS_DATASET.transformed_output_path,
) -> Path:
    """Write a Spark DataFrame as one header-containing CSV file."""

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = output_path.parent / f".{output_path.stem}.spark-output"
    temporary_file: Path | None = None

    shutil.rmtree(temporary_dir, ignore_errors=True)
    try:
        (
            dataframe.select(*IRIS_DATASET.columns)
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
