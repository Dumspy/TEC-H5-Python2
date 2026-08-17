"""Create plots from data read from MySQL."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # Save plots consistently in terminal environments.
import matplotlib.pyplot as plt
import pandas as pd

from .constants import MEASUREMENT_COLUMNS

DEFAULT_PLOTS_DIR = Path("Output_dir/plots")


def _save_figure(
    figure: plt.Figure,
    output_dir: Path,
    filename: str,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    figure.tight_layout()
    figure.savefig(output_path, dpi=150)
    plt.close(figure)
    return output_path


def create_scatter_plot(
    dataframe: pd.DataFrame,
    output_dir: Path = DEFAULT_PLOTS_DIR,
) -> Path:
    """Create a sepal-length/petal-length scatter plot."""

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
    return _save_figure(figure, output_dir, "scatter_plot.png")


def create_petal_width_histogram(
    dataframe: pd.DataFrame,
    output_dir: Path = DEFAULT_PLOTS_DIR,
) -> Path:
    """Create a ten-bin histogram for petal width."""

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
    return _save_figure(figure, output_dir, "petal_width_histogram.png")


def create_measurement_boxplots(
    dataframe: pd.DataFrame,
    output_dir: Path = DEFAULT_PLOTS_DIR,
) -> Path:
    """Create one boxplot with the four measurements sharing a single axis."""

    figure, axis = plt.subplots(figsize=(10, 6))
    data = [dataframe[column].dropna() for column in MEASUREMENT_COLUMNS]
    labels = [column.replace("_", " ").title() for column in MEASUREMENT_COLUMNS]
    axis.boxplot(data, tick_labels=labels)
    axis.set_ylabel("Measurement")
    axis.set_title("Iris-setosa: Measurement Distributions")
    axis.grid(axis="y", alpha=0.25)
    return _save_figure(figure, output_dir, "measurement_boxplots.png")
