# supercourier_etl/file_manager.py
"""
File Management Utility for the SuperCourier ETL.

This module centralizes all file system operations, such as archiving, deleting,
and versioning of data files.
This separation of concerns cleans up the main pipeline orchestrator, making it
easier to read and maintain.
"""

# Imports of the necessary libraries
import os
import logging

# Get the logger from the main config
from . import config

def archive_existing_file(file_path: str):
    """Archives an existing file by renaming it with an incremental suffix.

    If 'file.txt' exists, it is renamed to 'file_old.txt'.
    If 'file_old.txt' also exists, it is renamed to 'file_old_1.txt', and so on.
    This prevents accidental data loss during pipeline regeneration.

    Args:
        file_path (str): The absolute or relative path to the file to be archived.
    """
    if not os.path.exists(file_path): return

    base, ext = os.path.splitext(file_path)
    old_path = f"{base}_old{ext}"
    if not os.path.exists(old_path):
        os.rename(file_path, old_path)
        config.logger.info(f"Archived '{os.path.basename(file_path)}' to '{os.path.basename(old_path)}'")
        return

    i = 1
    while True:
        numbered_old_path = f"{base}_old_{i}{ext}"
        if not os.path.exists(numbered_old_path):
            os.rename(file_path, numbered_old_path)
            config.logger.info(f"Archived '{os.path.basename(file_path)}' to '{os.path.basename(numbered_old_path)}'")
            break
        i += 1

def replace_main_file(file_path: str):
    """Deletes a specific file if it exists, without touching its archived versions.

    This is used when the user chooses to replace existing data but keep old archives.
    Logs an error if the file deletion fails.

    Args:
        file_path (str): The path to the file to be deleted.
    """
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            config.logger.info(f"Replaced: '{os.path.basename(file_path)}' deleted.")
    except OSError as e:
        config.logger.error(f"Error deleting file {file_path}: {e}")

def delete_all_file_versions(file_path: str):
    """Performs a complete cleanup by deleting a file and all of its archived versions.

    It operates on a base file path (e.g., 'output/data.db').
    It will then delete all files in the same directory that match the pattern:
    - data.db
    - data_old.db
    - data_old_1.db
    This is useful for a full reset of the output directory.

    Args:
        file_path (str): The base path of the file for which all versions should be deleted.
    """
    try:
        dir_name = os.path.dirname(file_path)
        if not os.path.exists(dir_name): return

        base_name, ext = os.path.splitext(os.path.basename(file_path))

        for filename in os.listdir(dir_name):
            if filename.startswith(base_name) and filename.endswith(ext):
                path_to_delete = os.path.join(dir_name, filename)
                os.remove(path_to_delete)
                config.logger.info(f"Deleted file version: '{filename}'")
    except OSError as e:
        config.logger.error(f"Error deleting versions of {file_path}: {e}")
