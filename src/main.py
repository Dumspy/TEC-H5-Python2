"""Run the Iris ETL pipeline."""

from __future__ import annotations

from .dataset import IRIS_DATASET
from .extract import extract
from .lib.mysql import get_database_config
from .lib.spark import create_spark_session
from .load import (
    query_transformed_data,
    write_transformed_csv,
    write_transformed_mysql,
)
from .transform import transform_iris
from .visualization import (
    create_measurement_boxplots,
    create_petal_width_histogram,
    create_scatter_plot,
)


def main() -> None:
    """Run Extract -> Transform -> Load -> Visualize."""

    # I chose requests as the method used by the main pipeline.
    #
    # Why requests is the best choice here:
    # - It is cross-platform and runs directly in Python, so it does not start
    #   a shell or expose the URL to command-line injection.
    # - HTTPS certificate verification is enabled by default. It also checks
    #   HTTP status codes, redirects, and timeouts directly in the code.
    # - stream=True downloads the file in small chunks instead of loading the
    #   entire response into memory.
    # - The shared extraction code writes to a temporary file and only renames
    #   it after the download and headers are complete. Partial data therefore
    #   cannot replace a valid input file.
    #
    # Why the other methods are not selected:
    # - wget does not use a shell, but it gives us less control over TLS,
    #   redirects, response errors, streaming, and timeouts.
    # - curl needs an external program to be installed. Our implementation is
    #   safe because it passes the URL as a separate argument with shell=False.
    #   Using shell=True or building a command string instead could allow a
    #   malicious URL to cause command-line injection.

    input_path = extract(
        method="requests",
        url=IRIS_DATASET.source_url,
        destination_dir=IRIS_DATASET.input_dir,
    )
    database_config = get_database_config()
    spark = create_spark_session()

    try:
        transformed = transform_iris(spark, input_path)
        transformed_csv_path = write_transformed_csv(
            transformed,
            output_path=IRIS_DATASET.transformed_output_path,
        )
        table_name = write_transformed_mysql(transformed, database_config)

        # Plot data comes from SQL queries against MySQL.
        database_data = query_transformed_data(database_config)
        scatter_plot_path = create_scatter_plot(
            database_data,
            IRIS_DATASET.plots_dir,
        )
        histogram_path = create_petal_width_histogram(
            database_data,
            IRIS_DATASET.plots_dir,
        )
        boxplot_path = create_measurement_boxplots(
            database_data,
            IRIS_DATASET.plots_dir,
        )

        print(f"Transformed CSV written to: {transformed_csv_path}")
        print(f"MySQL table populated: {table_name}")
        print(f"Scatter plot: {scatter_plot_path}")
        print(f"Histogram: {histogram_path}")
        print(f"Boxplots: {boxplot_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
