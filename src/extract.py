"""Download the source dataset safely into the local input directory.

The module exposes three extraction implementations as required by the
assignment.  ``requests`` is selected by the application because it gives us
more direct control over TLS verification, streaming, timeouts, and retries.
"""

from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Callable
from urllib.parse import unquote, urlsplit

import requests
import wget

SOURCE_URL = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/iris.csv"
CSV_HEADERS = "sepal_length,sepal_width,petal_length,petal_width,species"
DEFAULT_INPUT_DIR = Path("Input_dir")
DOWNLOAD_CHUNK_SIZE = 64 * 1024
MAX_ATTEMPTS = 3


def _validate_url(url: str) -> str:
    """Validate the download URL before giving it to any HTTP client.

    HTTPS is required so that the transport is protected by TLS.  The URL is
    also parsed instead of being inserted into a shell command or a path.
    """

    parsed = urlsplit(url)
    if parsed.scheme.lower() != "https" or not parsed.netloc:
        raise ValueError("The dataset URL must be a valid HTTPS URL")
    return url


def _filename_from_url(url: str) -> str:
    """Return a safe filename derived from the URL path."""

    parsed = urlsplit(_validate_url(url))
    filename = Path(unquote(parsed.path)).name
    if not filename or filename in {".", ".."}:
        raise ValueError("The dataset URL does not contain a filename")
    return filename


def _destination_path(url: str, destination_dir: Path) -> Path:
    destination_dir.mkdir(parents=True, exist_ok=True)
    # Only the basename is used.  A remote path cannot escape destination_dir.
    return destination_dir / _filename_from_url(url)


def _write_headers_and_replace(raw_path: Path, destination: Path) -> Path:
    """Prepend the known Iris headers and atomically replace the destination."""

    if not raw_path.is_file() or raw_path.stat().st_size == 0:
        raise RuntimeError("The downloaded dataset is empty")

    with NamedTemporaryFile(
        mode="wb",
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
        delete=False,
    ) as output:
        output_path = Path(output.name)
        output.write(f"{CSV_HEADERS}\n".encode("utf-8"))
        with raw_path.open("rb") as source:
            shutil.copyfileobj(source, output, length=DOWNLOAD_CHUNK_SIZE)

    raw_path.unlink(missing_ok=True)
    output_path.replace(destination)
    return destination


def _download_with_retries(
    download_once: Callable[[Path], None],
    destination: Path,
    method_name: str,
) -> Path:
    """Run a downloader with bounded retries and safe finalization."""

    last_error: Exception | None = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        raw_path: Path | None = None
        try:
            with NamedTemporaryFile(
                prefix=f".{destination.name}.",
                suffix=".download",
                dir=destination.parent,
                delete=False,
            ) as temporary:
                raw_path = Path(temporary.name)

            download_once(raw_path)
            return _write_headers_and_replace(raw_path, destination)
        except Exception as error:  # Re-raise a useful error after final retry.
            last_error = error
            if raw_path is not None:
                raw_path.unlink(missing_ok=True)
            if attempt < MAX_ATTEMPTS:
                time.sleep(attempt)

    raise RuntimeError(
        f"{method_name} failed after {MAX_ATTEMPTS} attempts"
    ) from last_error


def download_with_requests(
    url: str = SOURCE_URL,
    destination_dir: Path = DEFAULT_INPUT_DIR,
) -> Path:
    """Download the dataset with streaming HTTPS requests.

    ``stream=True`` prevents the entire response from being held in memory.
    Requests verifies the HTTPS certificate by default; deliberately do not
    use ``verify=False``.  A connect/read timeout and bounded retries handle
    unstable network connections.  The response is first written to a
    temporary file, so an interrupted transfer cannot replace a valid file.
    """

    _validate_url(url)
    destination = _destination_path(url, destination_dir)

    def download_once(raw_path: Path) -> None:
        with requests.get(
            url,
            stream=True,
            timeout=(10, 60),
            allow_redirects=True,
        ) as response:
            response.raise_for_status()
            if urlsplit(response.url).scheme.lower() != "https":
                raise RuntimeError("The final redirected URL is not HTTPS")
            with raw_path.open("wb") as output:
                for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                    if chunk:
                        output.write(chunk)

    return _download_with_retries(download_once, destination, "requests")


def download_with_wget(
    url: str = SOURCE_URL,
    destination_dir: Path = DEFAULT_INPUT_DIR,
) -> Path:
    """Download the dataset with the Python ``wget`` package.

    The URL is validated and the output is written to a temporary path.  The
    package has less control over streaming and timeout configuration than
    requests, so bounded retries and atomic finalization are especially
    important here.  TLS verification is left enabled by the underlying
    Python URL implementation.
    """

    _validate_url(url)
    destination = _destination_path(url, destination_dir)

    def download_once(raw_path: Path) -> None:
        wget.download(url, out=str(raw_path), bar=None)
        print()  # wget's progress output does not end with a newline.

    return _download_with_retries(download_once, destination, "wget")


def download_with_curl(
    url: str = SOURCE_URL,
    destination_dir: Path = DEFAULT_INPUT_DIR,
) -> Path:
    """Download the dataset by safely executing the external curl program.

    ``subprocess.run`` receives a list of arguments and uses ``shell=False``.
    Therefore the URL is not interpreted as shell syntax, which prevents
    command-line injection.  curl is restricted to HTTPS, keeps certificate
    verification enabled, retries transient failures, and writes to a
    temporary file before the final CSV is replaced.
    """

    _validate_url(url)
    destination = _destination_path(url, destination_dir)
    curl_path = shutil.which("curl")
    if curl_path is None:
        raise RuntimeError("curl is not available on PATH")

    def download_once(raw_path: Path) -> None:
        command = [
            curl_path,
            "--fail",
            "--silent",
            "--show-error",
            "--location",
            "--proto",
            "=https",
            "--proto-redir",
            "=https",
            "--retry",
            "2",
            "--retry-delay",
            "1",
            "--connect-timeout",
            "10",
            "--max-time",
            "120",
            "--output",
            str(raw_path),
            url,
        ]
        subprocess.run(
            command,
            check=True,
            shell=False,
            timeout=180,
        )

    return _download_with_retries(download_once, destination, "curl")


def extract(
    method: str = "requests",
    url: str = SOURCE_URL,
    destination_dir: Path = DEFAULT_INPUT_DIR,
) -> Path:
    """Run one of the available extraction implementations."""

    methods = {
        "requests": download_with_requests,
        "wget": download_with_wget,
        "curl": download_with_curl,
    }
    try:
        downloader = methods[method.lower()]
    except KeyError as error:
        available = ", ".join(sorted(methods))
        raise ValueError(f"Unknown extraction method; choose one of {available}") from error
    return downloader(url=url, destination_dir=destination_dir)
