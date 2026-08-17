"""Entry point for the Iris ETL pipeline."""

from __future__ import annotations

from pathlib import Path

from .extract import extract
from .load import (
    get_database_config,
    query_transformed_data,
    write_transformed_csv,
    write_transformed_mysql,
)
from .transform import create_spark_session, transform_iris
from .visualization import (
    create_measurement_boxplots,
    create_petal_width_histogram,
    create_scatter_plot,
)

INPUT_DIR = Path("Input_dir")
OUTPUT_DIR = Path("Output_dir")


def main() -> None:
    """Run Extract -> Transform -> Load -> Visualize."""

    # requests is selected because it is cross-platform and gives direct
    # control over HTTPS certificate verification, streaming, timeouts, and
    # retries without invoking a shell.  wget has less direct control over
    # these behaviors, while an unsafe curl implementation could expose the
    # application to command-line injection.  Our curl implementation avoids
    # that risk with shell=False and a list of arguments.
    input_path = extract(method="requests", destination_dir=INPUT_DIR)
    database_config = get_database_config()
    spark = create_spark_session()

    try:
        transformed_dataframe = transform_iris(spark, input_path)
        write_transformed_csv(
            transformed_dataframe,
            output_path=OUTPUT_DIR / "transform_iris.csv",
        )
        write_transformed_mysql(transformed_dataframe, database_config)

        # Visualizations deliberately use data queried from MySQL rather than
        # the CSV or the pre-load Spark DataFrame.
        database_dataframe = query_transformed_data(database_config)
        create_scatter_plot(database_dataframe, OUTPUT_DIR / "plots")
        create_petal_width_histogram(database_dataframe, OUTPUT_DIR / "plots")
        create_measurement_boxplots(database_dataframe, OUTPUT_DIR / "plots")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
