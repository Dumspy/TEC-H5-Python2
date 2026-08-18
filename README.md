# Iris ETL Pipeline

A local macOS ETL application for extracting Iris flower data, filtering it with PySpark, loading it into CSV and MySQL, and generating visualizations from SQL data.

## Architecture

```text
GitHub HTTPS source
        |
        v
src/extract.py       Extract with requests, wget, or curl
        |
        v
Input_dir/iris.csv
        |
        v
src/transform.py     PySpark filter: species == Iris-setosa
        |
        +--> Output_dir/transform_iris.csv.enc
        |
        +--> MySQL iris_db.iris_setosa_encrypted
                         |
                         v
                 SQL query from Python
                         |
                         v
                 Matplotlib visualizations
```

## Project structure

```text
src/
├── __init__.py
├── main.py
├── extract.py
├── transform.py
├── security.py
├── load.py
├── visualization.py
├── dataset.py
└── lib/
    ├── __init__.py
    ├── mysql.py
    ├── serialization.py
    ├── spark.py
    └── spark_io.py
```

## Requirements

The project uses:

- macOS
- `devenv` with `uv`
- Python
- JDK 17 for the PySpark JVM runtime
- Docker Desktop with Docker Compose

PySpark is a Python API to Apache Spark, but Spark itself runs on the JVM. JDK 17 is therefore required even though the application code is Python.

## Initial setup

Enter or reload the devenv environment:

```bash
devenv shell
```

Verify the tools:

```bash
python --version
uv --version
java -version
docker --version
docker compose version
```

Install the locked Python dependencies:

```bash
uv sync
```

Create the local environment file. The project currently uses the example local values:

```bash
cp .env.example .env
```

The `.env` file is ignored by Git. Generate a local encryption key for Part 2:

```bash
uv run python -c "import base64, secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())"
```

Copy the printed value into `.env` as `ENCRYPTION_KEY=...`. Do not commit the key.

## Start MySQL

Start the MySQL 8.4 container:

```bash
docker compose up -d
```

Check the service status:

```bash
docker compose ps
```

The Python application connects through the host-published port:

```text
Host:     127.0.0.1
Port:     3306
Database: iris_db
User:     iris_app
Table:    iris_setosa
```

Stop the service while preserving its data:

```bash
docker compose down
```

Delete the database volume and all stored MySQL data:

```bash
docker compose down -v
```

## Run the pipeline

Start MySQL first, then run:

```bash
uv run python -m src.main
```

The pipeline performs:

1. Extract the source file over HTTPS with `requests`.
2. Save it as `Input_dir/iris.csv` and add the column headers.
3. Filter the data using PySpark so only `Iris-setosa` remains.
4. Encrypt and save the transformed data as `Output_dir/transform_iris.csv.enc`.
5. Create the separate encrypted MySQL table if it does not exist.
6. Truncate old encrypted rows and load the new encrypted columns.
7. Query encrypted columns from MySQL and decrypt them in batches.
8. Generate the plots from the decrypted database DataFrame.

## Output files

```text
Input_dir/iris.csv
Output_dir/transform_iris.csv.enc
Output_dir/plots/scatter_plot.png
Output_dir/plots/petal_width_histogram.png
Output_dir/plots/measurement_boxplots.png
```

The source dataset does not contain headers. The application adds:

```text
sepal_length,sepal_width,petal_length,petal_width,species
```

## Python modules

### `src/extract.py`

Contains three extraction implementations:

- `requests`
- Python `wget`
- `subprocess` with external `curl`

The main pipeline selects `requests` because it is cross-platform and provides direct control over HTTPS certificate verification, streaming, timeouts, and response errors.

All download methods:

- Require an HTTPS URL.
- Derive the destination filename from the URL.
- Write to a temporary file first.
- Use timeouts and temporary files so failed downloads do not replace valid input.
- Replace the destination only after a successful download.

The `requests` method uses `stream=True` and writes small chunks instead of loading the entire response into memory. Certificate verification remains enabled.

The `curl` implementation uses an argument list and `shell=False`. It does not construct a shell command string, which prevents the URL from being interpreted as shell syntax and reduces command-line injection risk.

### `src/dataset.py`

Defines the shared Iris dataset metadata in one structure: source URL, derived filenames, paths, column names, species filter, CSV headers, and the PySpark schema.

### `src/transform.py`

Receives a local PySpark session and applies an explicit schema:

```text
sepal_length: double
sepal_width:  double
petal_length: double
petal_width:  double
species:      string
```

The transformation keeps only rows where:

```text
species == Iris-setosa
```

### `src/load.py`

Provides the assignment-facing encrypted load functions while delegating CSV serialization and MySQL details to the reusable `lib` modules.

### `src/security.py`

Contains AES-GCM, AES-CBC, and Fernet encryption/decryption methods. The main pipeline selects AES-GCM because it provides authenticated encryption and detects tampering.

### `src/lib/spark.py`

Creates the local Spark session used by the transformation stage.

### `src/lib/spark_io.py`

Contains the Part 1 Spark CSV output helper. Part 2 uses `lib/serialization.py` to write encrypted CSV values instead.

### `src/lib/serialization.py`

Converts transformed rows into encrypted CSV values and provides lazy row decryption for encrypted database batches.

### `src/lib/mysql.py`

Owns database configuration, MySQL connections, encrypted-column table creation, batch inserts, and batch queries. Part 2 uses `iris_setosa_encrypted` and does not modify the Part 1 table `iris_setosa`.

### `src/visualization.py`

Contains three DataFrame-based visualization methods:

- Scatter plot of `sepal_length` against `petal_length`.
- Approximately ten-bin histogram of `petal_width`.
- 2x2 boxplot layout for all four numeric measurements.

Each figure has axis labels and an appropriate title and is saved as a PNG file.

### `src/main.py`

Coordinates the complete Extract -> Transform -> Load -> Visualize process and stops the Spark session when processing is complete.

## Security and robustness considerations

- HTTPS and default TLS certificate verification protect the data during transport.
- Downloads use timeouts so an unstable connection cannot hang indefinitely.
- Downloads are streamed in chunks.
- Temporary files prevent incomplete downloads from replacing valid data.
- Filenames are derived safely from the URL basename.
- The curl implementation uses `shell=False` and separate command arguments.
- Database credentials are loaded from `.env` rather than hardcoded in Python.
- The MySQL container is intended for local development through the host-published port.

## Part 2: encrypted storage

Part 2 stores the transformed values encrypted in both storage targets:

```text
Output_dir/transform_iris.csv.enc
MySQL table: iris_setosa_encrypted
```

The CSV keeps the original headers, but every value is stored as a Base64-encoded encrypted payload. The MySQL table stores each encrypted column as `VARBINARY` values and has a separate `id` column so row counts can still be queried with SQL.

The pipeline implements three methods in `src/security.py`:

- AES-GCM
- AES-CBC
- Fernet

AES-GCM is selected because it provides authenticated encryption. Raw AES-CBC does not detect manipulation without a separate MAC, while Fernet provides authentication but is a higher-level format based on AES-CBC and HMAC.

The encryption key is loaded from `ENCRYPTION_KEY` in `.env`. It must not be committed or stored in MySQL. Each AES-GCM value receives its own random nonce, which is stored together with that value and is not secret.

Encrypted database rows are fetched in batches, decrypted in Python, converted back into numeric values, and then passed to the visualization methods. SQL can still count rows, but it cannot perform normal numeric comparisons or averages on encrypted values.

## Relation to the learning objectives

| Learning objective | Implementation |
|---|---|
| ETL pipeline | `main.py` coordinates Extract, Transform, Load, and visualization stages. |
| Reusable Python modules | ETL responsibilities are separated into the main modules, with shared infrastructure in `src/lib/`. |
| Secure internet extraction | HTTPS, certificate verification, streaming, timeouts, temporary files, and safe subprocess usage. |
| Data processing | PySpark DataFrames and filtering are used for transformation. |
| Cryptographic protection | AES-GCM encrypts transformed CSV and MySQL column values at rest; HTTPS/TLS protects data in transit. |
| SQL and databases | Python creates and queries the encrypted MySQL table; row counts remain possible through SQL. |
| Graphical visualization | Matplotlib creates the scatter plot, histogram, and boxplots from database data. |
