"""Run the encrypted Iris ETL pipeline."""

from __future__ import annotations

from .dataset import IRIS_DATASET
from .extract import extract
from .lib.mysql import get_database_config
from .lib.spark import create_spark_session
from .load import (
    query_decrypted_data,
    write_encrypted_csv,
    write_encrypted_mysql,
)
from .security import EncryptionMethod, get_encryption_key
from .transform import transform_iris
from .visualization import (
    create_measurement_boxplots,
    create_petal_width_histogram,
    create_scatter_plot,
)


def main() -> None:
    """Run Extract -> Transform -> Encrypt/Load -> Visualize."""

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

    # I chose AES-GCM for the encrypted data because it provides both
    # confidentiality and authentication. This means that tampering with an
    # encrypted value is detected when it is decrypted.
    #
    # Raw AES-CBC only provides confidentiality and would need a separate HMAC
    # to detect manipulation. Fernet is safer than raw CBC because it includes
    # authentication, but AES-GCM is the clearer AEAD choice for this dataset.
    encryption_method: EncryptionMethod = "aes-gcm"
    encryption_key = get_encryption_key()
    database_config = get_database_config()
    spark = create_spark_session()

    try:
        transformed = transform_iris(spark, input_path)
        encrypted_csv_path = write_encrypted_csv(
            transformed,
            encryption_method,
            encryption_key,
            output_path=IRIS_DATASET.encrypted_output_path,
        )
        table_name = write_encrypted_mysql(
            transformed,
            encryption_method,
            encryption_key,
            database_config,
        )

        # The SQL query returns encrypted columns. They are decrypted in
        # batches before being passed to the existing visualization methods.
        database_data = query_decrypted_data(
            encryption_method,
            encryption_key,
            database_config,
        )
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

        print(f"Encrypted CSV written to: {encrypted_csv_path}")
        print(f"Encrypted MySQL table populated: {table_name}")
        print(f"Scatter plot: {scatter_plot_path}")
        print(f"Histogram: {histogram_path}")
        print(f"Boxplots: {boxplot_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
