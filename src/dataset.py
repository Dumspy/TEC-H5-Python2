"""Shared definition of the Iris dataset and its project paths."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit


@dataclass(frozen=True)
class DatasetDefinition:
    """Dataset metadata shared by the ETL stages."""

    source_url: str = (
        "https://raw.githubusercontent.com/jbrownlee/Datasets/master/iris.csv"
    )
    input_dir: Path = Path("Input_dir")
    output_dir: Path = Path("Output_dir")
    species_column: str = "species"
    species_filter: str = "Iris-setosa"
    measurement_columns: tuple[str, ...] = (
        "sepal_length",
        "sepal_width",
        "petal_length",
        "petal_width",
    )
    output_prefix: str = "transform_"

    def filename_for(self, url: str) -> str:
        filename = Path(unquote(urlsplit(url).path)).name
        if not filename:
            raise ValueError("The dataset URL does not contain a filename")
        return filename

    @property
    def filename(self) -> str:
        return self.filename_for(self.source_url)

    @property
    def columns(self) -> tuple[str, ...]:
        return self.measurement_columns + (self.species_column,)

    @property
    def csv_headers(self) -> str:
        return ",".join(self.columns)

    @property
    def input_path(self) -> Path:
        return self.input_dir / self.filename

    @property
    def transformed_output_path(self) -> Path:
        return self.output_dir / f"{self.output_prefix}{self.filename}"

    @property
    def encrypted_output_path(self) -> Path:
        return self.output_dir / f"{self.output_prefix}{self.filename}.enc"

    @property
    def plots_dir(self) -> Path:
        return self.output_dir / "plots"

    @property
    def spark_schema(self):
        """Build the PySpark schema without importing Spark at module load."""

        from pyspark.sql.types import DoubleType, StringType, StructField, StructType

        fields = [
            StructField(column, DoubleType(), nullable=False)
            for column in self.measurement_columns
        ]
        fields.append(StructField(self.species_column, StringType(), nullable=False))
        return StructType(fields)


IRIS_DATASET = DatasetDefinition()
