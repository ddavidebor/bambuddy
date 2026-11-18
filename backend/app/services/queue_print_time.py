"""The slicer's print time for the plate a queue item prints.

Cached on the row at creation: Shortest-Job-First sorts on it, a scheduled job's
reservation and the projection measure a job by it, and a row without one is
taken to run for a day (queue_rules.UNKNOWN_PRINT_TIME_FOR_RESERVATION). Every
path that queues a job sets it from here.
"""

from pathlib import Path

from backend.app.core.config import settings
from backend.app.utils.threemf_tools import extract_print_time_from_3mf


def queue_print_time(archive, library_file, plate_id: int | None) -> int | None:
    """The plate's own time from the 3MF, else the whole file's."""
    cached_print_time = None
    if archive:
        cached_print_time = archive.print_time_seconds
        if plate_id:
            archive_path = settings.base_dir / archive.file_path
            if archive_path.exists():
                plate_time = extract_print_time_from_3mf(archive_path, plate_id)
                if plate_time is not None:
                    cached_print_time = plate_time
    elif library_file:
        if library_file.file_metadata:
            cached_print_time = library_file.file_metadata.get("print_time_seconds")
        if plate_id:
            lib_path = Path(library_file.file_path)
            library_file_path = lib_path if lib_path.is_absolute() else settings.base_dir / library_file.file_path
            if library_file_path.exists():
                plate_time = extract_print_time_from_3mf(library_file_path, plate_id)
                if plate_time is not None:
                    cached_print_time = plate_time
    return cached_print_time
