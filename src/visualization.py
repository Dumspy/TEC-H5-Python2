"""Generate plots from data retrieved from MySQL."""

from __future__ import annotations

from pathlib import Path

import matplotlib

# Use a non-interactive backend so the pipeline works consistently from a
# terminal and can save plots on systems without a graphical display.
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

DEFAULT_PLOTS_DIR = Path("Output_dir/plots")
MEASUREMENT_COLUMNS = (
    "sepal_length",
    "sepal_width",
    "petal_length",
    "petal_width",
)


def _prepare_output(directory: Path, filename: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    return directory / filename


def create_scatter_plot(
    dataframe: pd.DataFrame,
    output_dir: Path = DEFAULT_PLOTS_DIR,
) -> Path:
    """Create a sepal-length/petal-length scatter plot."""

    output_path = _prepare_output(output_dir, "scatter_plot.png")
    figure, axis = plt.subplots(figsize=(8, 6))
    axis.scatter(
        dataframe["sepal_length"],
        dataframe["petal_length"],
        alpha=0.8,
        color="tab:green",
        edgecolors="black",
        linewidths=0.4,
    )
    axis.set_xlabel("Sepal length")
    axis.set_ylabel("Petal length")
    axis.set_title("Iris-setosa: Sepal Length vs Petal Length")
    axis.grid(alpha=0.25)
    figure.tight_layout()
    figure.savefig(output_path, dpi=150)
    plt.close(figure)
    return output_path


def create_petal_width_histogram(
    dataframe: pd.DataFrame,
    output_dir: Path = DEFAULT_PLOTS_DIR,
) -> Path:
    """Create a ten-bin histogram for petal width."""

    output_path = _prepare_output(output_dir, "petal_width_histogram.png")
    figure, axis = plt.subplots(figsize=(8, 6))
    axis.hist(
        dataframe["petal_width"],
        bins=10,
        color="tab:blue",
        edgecolor="black",
    )
    axis.set_xlabel("Petal width")
    axis.set_ylabel("Number of observations")
    axis.set_title("Iris-setosa: Distribution of Petal Width")
    axis.grid(axis="y", alpha=0.25)
    figure.tight_layout()
    figure.savefig(output_path, dpi=150)
    plt.close(figure)
    return output_path


def create_measurement_boxplots(
    dataframe: pd.DataFrame,
    output_dir: Path = DEFAULT_PLOTS_DIR,
) -> Path:
    """Create a 2x2 layout of boxplots for all four measurements."""

    output_path = _prepare_output(output_dir, "measurement_boxplots.png")
    figure, axes = plt.subplots(2, 2, figsize=(10, 8))

    for axis, column in zip(axes.flat, MEASUREMENT_COLUMNS):
        axis.boxplot(dataframe[column].dropna())
        axis.set_xticks([1])
        axis.set_xticklabels([column])
        axis.set_ylabel("Measurement")
        axis.set_title(column.replace("_", " ").title())
        axis.grid(axis="y", alpha=0.25)

    figure.suptitle("Iris-setosa: Measurement Distributions", fontsize=14)
    figure.tight_layout()
    figure.savefig(output_path, dpi=150)
    plt.close(figure)
    return output_path
