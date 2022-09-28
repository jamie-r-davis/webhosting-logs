"""This module is a collection of utilities to streamline and handle the preprocessing of data"""
import csv
import gzip
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Iterator, Union

from apachelogs import LogParser

from config import Config

from .exceptions import DomainContinuityError, FileTypeError
from .models import LogRecord


def parse_log_file(
    filepath: Union[str, Path], log_format: str = Config.LOG_FORMAT
) -> Iterator[LogRecord]:
    """Parses the given gzipped file into a generator of LogRecords"""
    if isinstance(filepath, str):
        filepath = Path(filepath)

    if filepath.suffix != ".gz":
        raise FileTypeError(f"{filepath.name} is not a .gz file")

    parser = LogParser(log_format)
    with gzip.open(filepath, mode="rt") as f:
        for i, line in enumerate(f):
            try:
                entry = parser.parse(line)
            except Exception as e:
                print(f"{filepath}|{i+1}: {line}\nException raised:\n{e}")
            else:
                record = LogRecord(filepath.name, i + 1, entry)
                if record.valid:
                    yield record


def domain_has_coverage(
    domain: Union[Path, str], start: datetime, end: datetime
) -> bool:
    """Validates that the given domain has data for the start and end dates"""
    start_glob = f"*.{start:%m%d%y}.gz"
    end_glob = f"*.{end:%m%d%y}.gz"

    if isinstance(domain, str):
        domain = Path(domain)
    if len(list(domain.rglob(start_glob))) < 1:
        return False
    if len(list(domain.rglob(end_glob))) < 1:
        return False

    return True


def parse_domain_month(path: Union[Path, str], out_dir: Union[Path, str]):
    domain = path.parent.parent.name
    yyyy = path.parent.name
    mm = path.name
    out_file = Path(out_dir) / f"{domain}_{yyyy}_{mm}.csv"
    fieldnames = ["domain", "client", "user_agent", "time"]
    with out_file.open("w", newline="") as out_fo:
        writer = csv.DictWriter(out_fo, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for in_file in path.rglob("*.gz"):
            for entry in parse_log_file(in_file):
                writer.writerow(entry.to_dict())
    print(f"{domain}: {yyyy}-{mm} - Processed")


def parse_domain_by_month(
    domain: Union[Path, str],
    out_dir: Union[Path, str],
    start: datetime = None,
    end: datetime = None,
    validate: bool = True,
) -> None:
    start = start or datetime(2021, 8, 1)
    end = end or datetime(2022, 7, 31)
    domain = Path(domain)
    out_dir = Path(out_dir)
    if validate:
        if not domain_has_coverage(domain, start, end):
            raise DomainContinuityError(f"{domain} does not have full coverage")
    # delete existing domain files in out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    for existing_file in out_dir.glob(f"{domain.name}_*.csv"):
        existing_file.unlink(missing_ok=True)
    with ThreadPoolExecutor(max_workers=6) as executor:
        for month_dir in domain.glob("????/??"):
            executor.submit(parse_domain_month, month_dir, out_dir)
