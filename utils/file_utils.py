"""
utils/file_utils.py — File handling utilities.
Adapted from data_files_manipulation.py in the original project.
"""

from zipfile import ZipFile
from pathlib import Path
import shutil
import os


def unzip_all_files(directory: str | Path = None) -> None:
    """
    Extracts all .zip files found in the given directory (defaults to cwd).

    Args:
        directory: Path to search for zip files. Defaults to current working directory.
    """
    target = Path(directory) if directory else Path(os.getcwd())
    for file in target.iterdir():
        if file.suffix == ".zip":
            print(f"Extracting: {file.name}")
            with ZipFile(file, "r") as z:
                z.extractall(target)
    print("Done extracting.")


def move_all_files(source_dir: str | Path, dest_dir: str | Path) -> None:
    """
    Moves all files from source_dir into dest_dir.

    Args:
        source_dir: Directory to move files from.
        dest_dir:   Directory to move files into.
    """
    source = Path(source_dir)
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)

    for file in source.iterdir():
        if file.is_file():
            shutil.move(str(file), dest / file.name)
            print(f"Moved: {file.name} → {dest}")
    print("Done moving files.")


def ensure_dirs(*dirs: str | Path) -> None:
    """
    Creates one or more directories if they don't already exist.
    Useful for ensuring the project folder structure is in place.

    Example:
        ensure_dirs("data/raw", "models/saved")
    """
    for d in dirs:
        Path(d).mkdir(parents=True, exist_ok=True)