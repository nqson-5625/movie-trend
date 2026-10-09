from __future__ import annotations

import gzip
import hashlib
import json
import shutil
import tempfile

from datetime import datetime, timezone
from pathlib import Path

import requests


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

DEST_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "imdb"
)

REPORT_DIR = (
    PROJECT_ROOT
    / "data"
    / "reports"
)

MANIFEST_PATH = (
    REPORT_DIR
    / "source_manifest.json"
)


# ============================================================
# ENV
# ============================================================

IMDB_BASE_URL = "https://datasets.imdbws.com"


# ============================================================
# REQUIRED FILES
# ============================================================

REQUIRED_FILES = [
    "title.basics.tsv.gz",
    "title.ratings.tsv.gz",
]


# ============================================================
# UTILITIES
# ============================================================

def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def sha256_file(path: Path) -> str:
    sha256 = hashlib.sha256()

    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)

            if not chunk:
                break

            sha256.update(chunk)

    return sha256.hexdigest()


def download_file(
    url: str,
    destination: Path,
) -> None:

    print(f"[DOWNLOAD] {destination.name}")

    with requests.get(
        url,
        stream=True,
        timeout=120,
    ) as response:

        response.raise_for_status()

        with destination.open("wb") as f:

            for chunk in response.iter_content(
                chunk_size=1024 * 1024
            ):

                if chunk:
                    f.write(chunk)


def validate_gzip(path: Path) -> None:
    """
    Verify that the .gz stream is readable.

    It does NOT extract a .tsv file to disk.
    """

    try:
        with gzip.open(path, "rb") as f:

            while f.read(1024 * 1024):
                pass

    except (gzip.BadGzipFile, EOFError) as exc:

        raise RuntimeError(
            f"Invalid/corrupted gzip file: {path}"
        ) from exc


def make_read_only(path: Path) -> None:
    path.chmod(0o444)


# ============================================================
# MANIFEST
# ============================================================

def update_manifest(
    retrieved_at: str,
    files_metadata: list[dict],
) -> None:

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    if MANIFEST_PATH.exists():

        with MANIFEST_PATH.open(
            "r",
            encoding="utf-8",
        ) as f:

            manifest = json.load(f)

    else:

        manifest = {
            "build_id": retrieved_at,
            "sources": [],
        }

    if "sources" not in manifest:
        manifest["sources"] = []

    # Remove old IMDb metadata entry from manifest only.
    # This never deletes raw files.

    manifest["sources"] = [
        source
        for source in manifest["sources"]
        if source.get("source_name") != "IMDb"
    ]

    manifest["sources"].append(
        {
            "source_name": "IMDb",
            "retrieved_at": retrieved_at,
            "files": files_metadata,
        }
    )

    temp_manifest = MANIFEST_PATH.with_suffix(
        ".json.tmp"
    )

    with temp_manifest.open(
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            manifest,
            f,
            ensure_ascii=False,
            indent=2,
        )

    temp_manifest.replace(
        MANIFEST_PATH
    )


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    print("=" * 60)
    print("IMDb Raw Dataset Downloader")
    print("=" * 60)

    DEST_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # --------------------------------------------------------
    # IMMUTABILITY CHECK
    # --------------------------------------------------------

    existing = [
        filename
        for filename in REQUIRED_FILES
        if (DEST_DIR / filename).exists()
    ]

    if existing:

        raise RuntimeError(
            "\nIMDb raw data already exists:\n"
            + "\n".join(
                str(DEST_DIR / f)
                for f in existing
            )
            + "\n\nRefusing to overwrite raw data."
        )

    retrieved_at = utc_now()

    file_metadata = []

    # --------------------------------------------------------
    # DOWNLOAD EVERYTHING INTO TEMP FIRST
    # --------------------------------------------------------

    with tempfile.TemporaryDirectory() as temp_dir_str:

        temp_dir = Path(temp_dir_str)

        for filename in REQUIRED_FILES:

            source_url = (
                f"{IMDB_BASE_URL}/{filename}"
            )

            temp_path = (
                temp_dir
                / filename
            )

            download_file(
                source_url,
                temp_path,
            )

            print(
                f"[VERIFY] gzip integrity: {filename}"
            )

            validate_gzip(
                temp_path
            )

            checksum = sha256_file(
                temp_path
            )

            file_metadata.append(
                {
                    "name": filename,
                    "source_url": source_url,
                    "size_bytes": temp_path.stat().st_size,
                    "sha256": checksum,
                }
            )

        # ----------------------------------------------------
        # ONLY PUBLISH AFTER ALL FILES PASS
        # ----------------------------------------------------

        for filename in REQUIRED_FILES:

            source = (
                temp_dir
                / filename
            )

            destination = (
                DEST_DIR
                / filename
            )

            shutil.move(
                str(source),
                str(destination),
            )

    # --------------------------------------------------------
    # MAKE RAW IMMUTABLE
    # --------------------------------------------------------

    for filename in REQUIRED_FILES:

        path = (
            DEST_DIR
            / filename
        )

        make_read_only(
            path
        )

    # --------------------------------------------------------
    # UPDATE MANIFEST
    # --------------------------------------------------------

    update_manifest(
        retrieved_at=retrieved_at,
        files_metadata=file_metadata,
    )

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    print()
    print("[OK] IMDb raw download completed.")
    print()

    print("Raw directory:")
    print(DEST_DIR)

    print()
    print("Downloaded files:")

    for item in file_metadata:

        print(
            f"  - {item['name']}"
        )

        print(
            f"    size   : "
            f"{item['size_bytes']} bytes"
        )

        print(
            f"    sha256 : "
            f"{item['sha256']}"
        )

    print()
    print(
        "Files remain gzip-compressed "
        "and were marked read-only."
    )


if __name__ == "__main__":
    main()
