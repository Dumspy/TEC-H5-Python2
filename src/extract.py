"""Download the Iris CSV with one of three extraction methods."""

from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Callable, Literal, TypeAlias
from urllib.parse import SplitResult, urlsplit

import requests
import wget

from .dataset import IRIS_DATASET

CHUNK_SIZE = 64 * 1024
MAX_ATTEMPTS = 3
ExtractionMethod: TypeAlias = Literal["requests", "wget", "curl"]
DownloadFunction: TypeAlias = Callable[[Path], None]


def _validate_url(url: str) -> SplitResult:
    parsed = urlsplit(url)
    if parsed.scheme.lower() != "https" or not parsed.netloc:
        raise ValueError("The dataset URL must be a valid HTTPS URL")
    return parsed


def _destination(url: str, directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    return directory / IRIS_DATASET.filename_for(url)


def _finalize(raw_path: Path, destination: Path) -> Path:
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
        output.write(f"{IRIS_DATASET.csv_headers}\n".encode())
        with raw_path.open("rb") as source:
            shutil.copyfileobj(source, output, length=CHUNK_SIZE)

    raw_path.unlink(missing_ok=True)
    output_path.replace(destination)
    return destination


def _download(
    method: ExtractionMethod,
    url: str,
    directory: Path,
    download_once: DownloadFunction,
) -> Path:
    _validate_url(url)
    destination = _destination(url, directory)
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
            return _finalize(raw_path, destination)
        except Exception as error:
            last_error = error
            if raw_path is not None:
                raw_path.unlink(missing_ok=True)
            if attempt < MAX_ATTEMPTS:
                time.sleep(attempt)

    raise RuntimeError(f"{method} failed after {MAX_ATTEMPTS} attempts") from last_error


def download_with_requests(
    url: str = IRIS_DATASET.source_url,
    destination_dir: Path = IRIS_DATASET.input_dir,
) -> Path:
    """Download with streaming requests."""

    def download_once(raw_path: Path) -> None:
        # HTTPS, streaming, timeout, and default certificate verification.
        with requests.get(url, stream=True, timeout=(10, 60)) as response:
            response.raise_for_status()
            if urlsplit(response.url).scheme.lower() != "https":
                raise RuntimeError("The final redirected URL is not HTTPS")
            with raw_path.open("wb") as output:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        output.write(chunk)

    return _download("requests", url, destination_dir, download_once)


def download_with_wget(
    url: str = IRIS_DATASET.source_url,
    destination_dir: Path = IRIS_DATASET.input_dir,
) -> Path:
    """Download with the Python wget package."""

    def download_once(raw_path: Path) -> None:
        wget.download(url, out=str(raw_path), bar=None)

    return _download("wget", url, destination_dir, download_once)


def download_with_curl(
    url: str = IRIS_DATASET.source_url,
    destination_dir: Path = IRIS_DATASET.input_dir,
) -> Path:
    """Download with curl without invoking a shell."""

    curl_path = shutil.which("curl")
    if curl_path is None:
        raise RuntimeError("curl is not available on PATH")

    def download_once(raw_path: Path) -> None:
        # Separate arguments and shell=False prevent command-line injection.
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
            "--connect-timeout",
            "10",
            "--max-time",
            "120",
            "--output",
            str(raw_path),
            url,
        ]
        subprocess.run(command, check=True, shell=False, timeout=180)

    return _download("curl", url, destination_dir, download_once)


def extract(
    method: ExtractionMethod = "requests",
    url: str = IRIS_DATASET.source_url,
    destination_dir: Path = IRIS_DATASET.input_dir,
) -> Path:
    """Run the selected extraction method."""

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
