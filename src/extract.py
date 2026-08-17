"""Download the Iris CSV with one of three extraction methods."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Callable, Literal, TypeAlias
from urllib.parse import urlsplit

import requests
import wget

from .dataset import IRIS_DATASET

CHUNK_SIZE = 64 * 1024
ExtractionMethod: TypeAlias = Literal["requests", "wget", "curl"]
DownloadFunction: TypeAlias = Callable[[str, Path], None]


def download_with_requests(url: str, raw_path: Path) -> None:
    """Stream a dataset with requests."""

    with requests.get(url, stream=True, timeout=(10, 60)) as response:
        response.raise_for_status()
        if urlsplit(response.url).scheme.lower() != "https":
            raise RuntimeError("The final redirected URL is not HTTPS")
        with raw_path.open("wb") as output:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    output.write(chunk)


def download_with_wget(url: str, raw_path: Path) -> None:
    """Download a dataset with the Python wget package."""

    wget.download(url, out=str(raw_path), bar=None)


def download_with_curl(url: str, raw_path: Path) -> None:
    """Download a dataset with curl without invoking a shell."""

    curl_path = shutil.which("curl")
    if curl_path is None:
        raise RuntimeError("curl is not available on PATH")

    # Separate arguments and shell=False prevent command-line injection.
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
        available = ", ".join(methods)
        raise ValueError(f"Unknown extraction method; choose one of {available}")

    parsed_url = urlsplit(url)
    if parsed_url.scheme.lower() != "https" or not parsed_url.netloc:
        raise ValueError("The dataset URL must be a valid HTTPS URL")

    destination_dir.mkdir(parents=True, exist_ok=True)
    destination = destination_dir / IRIS_DATASET.filename_for(url)
    raw_path: Path | None = None
    output_path: Path | None = None

    try:
        with NamedTemporaryFile(
            prefix=f".{destination.name}.",
            suffix=".download",
            dir=destination.parent,
            delete=False,
        ) as temporary:
            raw_path = Path(temporary.name)

        methods[method](url, raw_path)
        if raw_path.stat().st_size == 0:
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

        output_path.replace(destination)
        output_path = None
        return destination
    finally:
        if raw_path is not None:
            raw_path.unlink(missing_ok=True)
        if output_path is not None:
            output_path.unlink(missing_ok=True)
