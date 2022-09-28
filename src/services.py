import gzip
from pathlib import Path
from typing import Iterable, Union

from apachelogs import InvalidEntryError, LogParser

from config import Config
from src.counters import AbstractCounter
from src.models import LogRecord


def read_logfile(logfile: Path) -> Iterable[str]:
    """Opens the logfile (whether gzipped or not) and returns an iterator of its lines."""
    if logfile.suffix == ".gz":
        with open(logfile, "rt") as file:
            for line in file:
                yield line
    else:
        with logfile.open() as file:
            for line in file:
                yield line


def count_log_entries(logfile: Union[str, Path], counters: list[AbstractCounter]):
    logfile = Path(logfile)
    parser = LogParser(Config.LOG_FORMAT)
    for i, line in enumerate(read_logfile(logfile)):
        try:
            entry = parser.parse(line)
        except (InvalidEntryError, ValueError):
            continue
        else:
            record = LogRecord(logfile.name, i + 1, entry)
            for counter in counters:
                counter.handle(record)


def threaded_count_log_entries(
    logfile: Union[str, Path], log_format: str, counter_class: AbstractCounter
) -> AbstractCounter:
    """A thread safe implementation of count_log_entries. This will initialize a new counter object, read the logfile, and return the counter for further processing."""
    logfile = Path(logfile)
    parser = LogParser(log_format)
    counter = counter_class()
    for i, line in enumerate(read_logfile(logfile)):
        try:
            entry = parser.parse(line)
        except (InvalidEntryError, ValueError):
            continue
        else:
            record = LogRecord(logfile.name, i + 1, entry)
            counter.handle(record)
    return counter
