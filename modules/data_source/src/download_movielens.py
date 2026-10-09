from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import zipfile

from datetime import datetime, timezone
from pathlib import Path

import requests


# ============================================================
# CONFIG
# ============================================================

VARIANT = "ml-latest-small"

SOURCE_URL = (
    f"https://files.grouplens.org/"
    f"datasets/movielens/{VARIANT}.zip"
)

REQUIRED_FILES = [
    "ratings.csv",
    "movies.csv",
    "links.csv",
]

OPTIONAL_FILES = [
    "tags.csv",
]


# ============================================================
# PROJECT PATHS
# ============================================================

# File:
# project/modules/data_source/src/download_movielens.py
#
# parents[0] = src
# parents[1] = data_source
# parents[2] = modules
# parents[3] = project root

PROJECT_ROOT = Path(__file__).resolve().parents[3]

MOVIELENS_RAW_ROOT = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "movielens"
)

DEST_DIR = MOVIELENS_RAW_ROOT / VARIANT

REPORT_DIR = (
    PROJECT_ROOT
    / "data"
    / "reports"
)

MANIFEST_PATH = REPORT_DIR / "source_manifest.json"


# ============================================================
# UTILS
# ============================================================

def utc_now() -> str:
    """
    Return current time in UTC ISO-8601.
    """
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def sha256_file(path: Path) -> str:
    """
    Calculate SHA-256 without loading entire file into RAM.
    """
    sha256 = hashlib.sha256()

    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)

            if not chunk:
                break

            sha256.update(chunk)

    return sha256.hexdigest()


def download_file(url: str, destination: Path) -> None:
    """
    Stream-download file to disk.
    """

    print(f"[DOWNLOAD] {url}")

    with requests.get(
        url,
        stream=True,
        timeout=60,
    ) as response:

        response.raise_for_status()

        with destination.open("wb") as f:
            for chunk in response.iter_content(
                chunk_size=1024 * 1024
            ):
                if chunk:
                    f.write(chunk)


def validate_zip_members(zip_file: zipfile.ZipFile) -> None:
    """
    Basic protection against ZIP path traversal.
    """

    for member in zip_file.infolist():

        member_path = Path(member.filename)

        if member_path.is_absolute():
            raise RuntimeError(
                f"Unsafe ZIP member: {member.filename}"
            )

        if ".." in member_path.parts:
            raise RuntimeError(
                f"Unsafe ZIP member: {member.filename}"
            )


def validate_extracted_dataset(dataset_dir: Path) -> None:
    """
    Step 3 only validates file existence.
    Content/schema profiling belongs to Step 5.
    """

    missing = []

    for filename in REQUIRED_FILES:
        path = dataset_dir / filename

        if not path.is_file():
            missing.append(filename)

    if missing:
        raise RuntimeError(
            f"Missing required MovieLens files: {missing}"
        )


def make_raw_read_only(dataset_dir: Path) -> None:
    """
    Guardrail: raw source files should not be edited accidentally.
    """

    for path in dataset_dir.iterdir():

        if path.is_file():
            path.chmod(0o444)


# ============================================================
# MANIFEST
# ============================================================

def build_file_metadata(dataset_dir: Path) -> list[dict]:

    files = []

    for path in sorted(dataset_dir.iterdir()):

        if not path.is_file():
            continue

        files.append(
            {
                "name": path.name,
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )

    return files


def write_manifest(
    retrieved_at: str,
    archive_sha256: str,
    files: list[dict],
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
            "movielens_variant": VARIANT,
            "sources": [],
        }

    # Remove an old MovieLens entry for the same variant
    # from the manifest representation only.
    #
    # IMPORTANT:
    # this does NOT modify raw files.

    manifest["sources"] = [
        source
        for source in manifest.get("sources", [])
        if not (
            source.get("source_name") == "MovieLens"
            and source.get("variant") == VARIANT
        )
    ]

    manifest["sources"].append(
        {
            "source_name": "MovieLens",
            "variant": VARIANT,
            "source_url": SOURCE_URL,
            "retrieved_at": retrieved_at,
            "archive_sha256": archive_sha256,
            "files": files,
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
            indent=2,
            ensure_ascii=False,
        )

    temp_manifest.replace(MANIFEST_PATH)


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    print("=" * 60)
    print("MovieLens Raw Data Downloader")
    print("=" * 60)

    MOVIELENS_RAW_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # --------------------------------------------------------
    # RAW IMMUTABILITY CHECK
    # --------------------------------------------------------

    if DEST_DIR.exists():

        raise RuntimeError(
            "\nRaw dataset already exists:\n"
            f"{DEST_DIR}\n\n"
            "The downloader refuses to overwrite raw data.\n"
            "If a new source version is required, create a new "
            "dataset/version intentionally instead of modifying "
            "the existing raw directory."
        )

    retrieved_at = utc_now()

    # --------------------------------------------------------
    # DOWNLOAD INTO TEMPORARY DIRECTORY
    # --------------------------------------------------------

    with tempfile.TemporaryDirectory() as temp_dir_str:

        temp_dir = Path(temp_dir_str)

        archive_path = (
            temp_dir
            / f"{VARIANT}.zip"
        )

        download_file(
            SOURCE_URL,
            archive_path,
        )

        archive_sha256 = sha256_file(
            archive_path
        )

        print(
            "[CHECKSUM] ZIP SHA-256:",
            archive_sha256,
        )

        # ----------------------------------------------------
        # EXTRACT INTO TEMPORARY STAGING AREA
        # ----------------------------------------------------

        extract_root = (
            temp_dir
            / "extracted"
        )

        extract_root.mkdir()

        with zipfile.ZipFile(
            archive_path,
            "r",
        ) as zip_file:

            validate_zip_members(zip_file)

            zip_file.extractall(
                extract_root
            )

        extracted_dataset = (
            extract_root
            / VARIANT
        )

        if not extracted_dataset.is_dir():

            raise RuntimeError(
                "Expected extracted directory not found: "
                f"{extracted_dataset}"
            )

        # ----------------------------------------------------
        # VALIDATE REQUIRED FILES
        # ----------------------------------------------------

        validate_extracted_dataset(
            extracted_dataset
        )

        # ----------------------------------------------------
        # MOVE TO FINAL RAW LOCATION
        # ----------------------------------------------------

        shutil.move(
            str(extracted_dataset),
            str(DEST_DIR),
        )

    # Temporary ZIP disappears here.
    # Raw CSV files are now in final location.

    # --------------------------------------------------------
    # CALCULATE CHECKSUMS OF EXTRACTED RAW FILES
    # --------------------------------------------------------

    file_metadata = build_file_metadata(
        DEST_DIR
    )

    # --------------------------------------------------------
    # PROTECT RAW FILES
    # --------------------------------------------------------

    make_raw_read_only(
        DEST_DIR
    )

    # --------------------------------------------------------
    # WRITE PROVENANCE
    # --------------------------------------------------------

    write_manifest(
        retrieved_at=retrieved_at,
        archive_sha256=archive_sha256,
        files=file_metadata,
    )

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    print()
    print("[OK] MovieLens download completed.")
    print()
    print("Raw location:")
    print(DEST_DIR)

    print()
    print("Manifest:")
    print(MANIFEST_PATH)

    print()
    print("Files:")

    for item in file_metadata:

        print(
            f"  - {item['name']} "
            f"({item['size_bytes']} bytes)"
        )

    print()
    print(
        "Raw files were marked read-only."
    )


if __name__ == "__main__":
    main()
