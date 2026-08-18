"""Download the Iris CSV with one of three extraction methods."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Callable, Literal
from urllib.parse import urlsplit

import requests
import wget

from .dataset import IRIS_DATASET

CHUNK_SIZE = 64 * 1024
type ExtractionMethod = Literal["requests", "wget", "curl"]
type DownloadFunction = Callable[[str, Path], None]


def download_with_requests(url: str, raw_path: Path) -> None:
    """Stream a dataset with requests."""

    # DATA SECURITY AND DATA INTEGRITY:
    # - requests uses Python's HTTP APIs, so it does not create a shell or
    #   expose the URL to command-line injection.
    # - HTTPS, certificate verification, the redirect check, and
    #   raise_for_status() protect the transfer and reject HTTP error pages.
    #
    # ROBUSTNESS UNDER DOWNLOAD INTERRUPTION:
    # - stream=True downloads small chunks and the connect/read timeout stops
    #   a stalled connection from waiting forever.
    with requests.get(url, stream=True, timeout=(10, 60)) as response:
        response.raise_for_status()
        with raw_path.open("wb") as output:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    output.write(chunk)


def download_with_wget(url: str, raw_path: Path) -> None:
    """Download a dataset with the Python wget package."""

    # DATA SECURITY AND DATA INTEGRITY:
    # - wget is a Python library call, not a shell command, so the URL cannot
    #   be used for command-line injection.
    # - wget gives us less control over TLS, redirects, status codes, and
    #   timeouts than requests, which is why it is not selected in main.py.
    #
    # ROBUSTNESS UNDER DOWNLOAD INTERRUPTION:
    # - wget has fewer download controls, but extract() gives it a temporary
    #   path and removes that file if the call fails.
    wget.download(url, out=str(raw_path), bar=None)


def download_with_curl(url: str, raw_path: Path) -> None:
    """Download a dataset with curl without invoking a shell."""

    curl_path = shutil.which("curl")
    if curl_path is None:
        raise RuntimeError("curl is not available on PATH")

    # DATA SECURITY AND DATA INTEGRITY:
    # - --proto/--proto-redir keep the request and redirects on HTTPS.
    # - The URL is a separate argument and shell=False stays enabled, so URL
    #   characters cannot become an injected shell command.
    # - --fail rejects HTTP errors instead of saving an error page.
    #
    # ROBUSTNESS UNDER DOWNLOAD INTERRUPTION:
    # - --connect-timeout and --max-time handle stalled connections.
    # - The shared temporary-file handling in extract() keeps partial curl
    #   output away from the final CSV.
    subprocess.run(
        [
            curl_path,
            "--fail",
            "--silent",
            "--show-error",
            "--location",
            "--proto",
            "=https",
            "--proto-redir",
            "=https",
            "--connect-timeout",
            "10",
            "--max-time",
            "120",
            "--output",
            str(raw_path),
            url,
        ],
        check=True,
        shell=False,
        timeout=180,
    )


def _prepend_headers(path: Path) -> None:
    """Prepend the known column headers to the downloaded file."""

    headers = f"{IRIS_DATASET.csv_headers}\n".encode()
    with path.open("r+b") as file:
        file.seek(0, 2)
        size = file.tell()
        for end in range(size, 0, -CHUNK_SIZE):
            start = max(0, end - CHUNK_SIZE)
            file.seek(start)
            chunk = file.read(end - start)
            file.seek(start + len(headers))
            file.write(chunk)
        file.seek(0)
        file.write(headers)


def extract(
    method: ExtractionMethod = "requests",
    url: str = IRIS_DATASET.source_url,
    destination_dir: Path = IRIS_DATASET.input_dir,
) -> Path:
    """Download the dataset, add headers, and replace the input file."""

    methods: dict[ExtractionMethod, DownloadFunction] = {
        "requests": download_with_requests,
        "wget": download_with_wget,
        "curl": download_with_curl,
    }
    if method not in methods:
        raise ValueError("Unknown extraction method")

    parsed_url = urlsplit(url)
    if parsed_url.scheme.lower() != "https" or not parsed_url.netloc:
        raise ValueError("The dataset URL must be a valid HTTPS URL")

    destination_dir.mkdir(parents=True, exist_ok=True)
    # FILE NAME AND CSV INTEGRITY:
    # - The filename is taken from the URL using filename_for().
    # - The downloaded source has no header row, so the column names from
    #   IRIS_DATASET are added before the file is made available to the rest of
    #   the pipeline.
    destination = destination_dir / IRIS_DATASET.filename_for(url)
    temporary_path: Path | None = None

    try:
        with NamedTemporaryFile(
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=destination.parent,
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)

        # Shared robustness for all three methods: they download to this
        # temporary file first. The rename below is the commit point, so an
        # interruption cannot replace valid input with partial data. The
        # finally block removes the temporary file when a method fails.
        methods[method](url, temporary_path)
        if temporary_path.stat().st_size == 0:
            raise RuntimeError("The downloaded dataset is empty")

        _prepend_headers(temporary_path)
        temporary_path.replace(destination)
        temporary_path = None
        return destination
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
