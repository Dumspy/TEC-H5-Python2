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
        +--> Output_dir/transform_iris.csv
        |
        +--> MySQL iris_db.iris_setosa
                         |
                         v
                 SQL query from Python
                         |
                         v
                 Matplotlib visualizations
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

The `.env` file is ignored by Git.

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
4. Save the transformed data as `Output_dir/transform_iris.csv`.
5. Create the MySQL table if it does not exist.
6. Truncate old table rows and load the new transformed data through JDBC.
7. Query the transformed rows from MySQL using SQL.
8. Generate the plots from the queried database DataFrame.

## Output files

```text
Input_dir/iris.csv
Output_dir/transform_iris.csv
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

The main pipeline selects `requests` because it is cross-platform and provides direct control over HTTPS certificate verification, streaming, timeouts, retries, and response errors.

All download methods:

- Require an HTTPS URL.
- Derive the destination filename from the URL.
- Write to a temporary file first.
- Use bounded retry handling.
- Replace the destination only after a successful download.

The `requests` method uses `stream=True` and writes small chunks instead of loading the entire response into memory. Certificate verification remains enabled.

The `curl` implementation uses an argument list and `shell=False`. It does not construct a shell command string, which prevents the URL from being interpreted as shell syntax and reduces command-line injection risk.

### `src/constants.py`

Stores the shared Iris column names and species value used by the ETL modules.

### `src/transform.py`

Uses a local PySpark session with an explicit schema:

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

The Spark session is also configured with MySQL Connector/J 8.4.0 so Spark can write the DataFrame through JDBC.

### `src/load.py`

The CSV loader produces one named CSV file instead of leaving only Spark's generated `part-*.csv` directory.

The MySQL loader:

1. Connects to the database initialized by Docker Compose.
2. Executes `CREATE TABLE IF NOT EXISTS iris_setosa`.
3. Executes `TRUNCATE TABLE iris_setosa`.
4. Loads the transformed PySpark DataFrame through JDBC.

This means a repeated application run replaces the table contents instead of appending duplicate data.

The same module queries MySQL using `mysql-connector-python` and returns a pandas DataFrame for visualization.

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
- Bounded retries handle temporary network failures.
- Temporary files prevent incomplete downloads from replacing valid data.
- Filenames are derived safely from the URL basename.
- The curl implementation uses `shell=False` and separate command arguments.
- Database credentials are loaded from `.env` rather than hardcoded in Python.
- The MySQL container is intended for local development through the host-published port.

## Relation to the learning objectives

| Learning objective | Implementation |
|---|---|
| ETL pipeline | `main.py` coordinates Extract, Transform, Load, and visualization stages. |
| Reusable Python modules | Functionality is separated into `extract.py`, `transform.py`, `load.py`, and `visualization.py`. |
| Secure internet extraction | HTTPS, certificate verification, streaming, timeouts, retries, and safe subprocess usage. |
| Data processing | PySpark DataFrames and filtering are used for transformation. |
| Cryptographic protection | HTTPS/TLS protects the data while it is transported from GitHub. |
| SQL and databases | Python executes SQL against MySQL and Spark loads the DataFrame through JDBC. |
| Graphical visualization | Matplotlib creates the scatter plot, histogram, and boxplots from database data. |
