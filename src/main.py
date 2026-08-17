"""Run the Iris ETL pipeline."""

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
PLOTS_DIR = OUTPUT_DIR / "plots"


def main() -> None:
    """Run Extract -> Transform -> Load -> Visualize."""

    # requests is selected for cross-platform HTTPS, streaming, timeouts,
    # retries, and avoiding shell execution.
    input_path = extract(method="requests", destination_dir=INPUT_DIR)
    database_config = get_database_config()
    spark = create_spark_session()

    try:
        transformed = transform_iris(spark, input_path)
        transformed_csv_path = write_transformed_csv(
            transformed,
            output_path=OUTPUT_DIR / "transform_iris.csv",
        )
        table_name = write_transformed_mysql(transformed, database_config)

        # Plot data comes from SQL queries against MySQL.
        database_data = query_transformed_data(database_config)
        scatter_plot_path = create_scatter_plot(database_data, PLOTS_DIR)
        histogram_path = create_petal_width_histogram(database_data, PLOTS_DIR)
        boxplot_path = create_measurement_boxplots(database_data, PLOTS_DIR)

        print(f"Transformed CSV written to: {transformed_csv_path}")
        print(f"MySQL table populated: {table_name}")
        print(f"Scatter plot: {scatter_plot_path}")
        print(f"Histogram: {histogram_path}")
        print(f"Boxplots: {boxplot_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
