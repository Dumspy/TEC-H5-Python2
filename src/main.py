"""Run the Iris ETL pipeline."""

from __future__ import annotations

from .dataset import IRIS_DATASET
from .extract import extract
from .load import (
    query_transformed_data,
    write_transformed_csv,
    write_transformed_mysql,
)
from .lib.mysql import get_database_config
from .lib.spark import create_spark_session
from .transform import transform_iris
from .visualization import (
    create_measurement_boxplots,
    create_petal_width_histogram,
    create_scatter_plot,
)


def main() -> None:
    """Run Extract -> Transform -> Load -> Visualize."""

    # requests is selected for cross-platform HTTPS, streaming, timeouts, and
    # avoiding shell execution.
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
