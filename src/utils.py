import functools
from datetime import datetime
from pathlib import Path
from typing import Union

import pytz


def timeit(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        start = datetime.now()
        print(f"Process started: {start:%Y-%m-%d %H:%M:%S}")
        result = f(*args, **kwargs)
        end = datetime.now()
        print(f"Process ended: {end:%Y-%m-%d %H:%M:%S}")
        duration = end - start
        total_seconds = duration.total_seconds()
        minutes = total_seconds // 60
        seconds = int(total_seconds % 60 // 1)
        print(f"Duration: {minutes}m {seconds}s")
        return result

    return wrapper


def localize_dttm(datetime: datetime, zone: str):
    """Localizes a datetime to the indicated timezone."""
    tz = pytz.timezone(zone)
    return tz.localize(datetime)


def ensure_path(value: Union[Path, str]) -> Path:
    """Ensures that the provided value is a path object"""
    if isinstance(value, str):
        return Path(value)
    return value
