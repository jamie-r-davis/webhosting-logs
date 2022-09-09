"""This module is a collection of utilities to streamline and handle the preprocessing of data"""
import csv
import gzip
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator, Union

import pandas as pd
from apachelogs import LogEntry, LogParser
from sqlalchemy.engine import Engine

from .exceptions import DomainContinuityError, FileTypeError
from .utils import ensure_path, localize_dttm
from .validators import MethodValidator, RequestTimeValidator, StatusValidator

LOG_FORMAT = '%h %l %u %t "%r" %s %b "%{Referer}i" "%{User-agent}i" %D'
TIMEZONE = "US/Eastern"
START_DT = localize_dttm(datetime(2021, 8, 1), TIMEZONE)
END_DT = localize_dttm(datetime(2022, 8, 1), TIMEZONE)


@dataclass
class LogRecord:
    filename: str
    row: int
    domain: str
    entry: LogEntry

    @property
    def valid(self) -> bool:
        validators = [
            MethodValidator(methods=["GET"]),
            StatusValidator(),
        ]
        for validator in validators:
            if validator.validate(self.entry) is False:
                return False
        return True

    def to_dict(self) -> dict:
        entry = self.entry
        return {
            "domain": self.domain,
            "client": entry.remote_host,
            "user_agent": entry.headers_in.get("User-Agent"),
            "time": entry.request_time.date().isoformat(),
        }

    def to_verbose_dict(self) -> dict:
        e = self.entry
        return {
            "filename": self.filename,
            "row": self.row,
            "domain": self.domain,
            "remote_host": e.remote_host,
            "request_time": e.request_time,
            "method": e.request_line.split(" ")[0],
            "uri": e.request_line.split(" ")[1],
            "status": e.status,
            "user_agent": e.headers_in.get("User-agent"),
            "referer": e.headers_in.get("Referer"),
            "request_duration": e.request_duration_microseconds,
            "bytes_sent": e.bytes_sent,
        }


def extract_domain(filepath: Union[str, Path]) -> str:
    """Extracts the domain value from the given filepath
    >>> filepath = Path('detroit.080121.gz')
    >>> extract_domain(filepath)
    detroit
    """
    if isinstance(filepath, str):
        filepath = Path(filepath)
    domain, *_ = filepath.stem.split(".")
    return domain


def parse_log_file(
    filepath: Union[str, Path], log_format: str = LOG_FORMAT
) -> Iterator[LogRecord]:
    """Parses the given gzipped file into a generator of LogRecords"""
    if isinstance(filepath, str):
        filepath = Path(filepath)

    if filepath.suffix != ".gz":
        raise FileTypeError(f"{filepath.name} is not a .gz file")

    parser = LogParser(log_format)
    domain = extract_domain(filepath)
    with gzip.open(filepath, mode="rt") as f:
        for i, line in enumerate(f):
            try:
                entry = parser.parse(line)
            except Exception as e:
                print(f"{filepath}|{i+1}: {line}\nException raised:\n{e}")
            else:
                record = LogRecord(filepath.name, i + 1, domain, entry)
                if record.valid:
                    yield record


def parse_log_file_to_csv(filepath: Union[Path, str], output_dir: Union[Path, str]):
    """Parses a log file to a csv"""
    if isinstance(filepath, str):
        filepath = Path(filepath)
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)
    df = pd.DataFrame((x.to_dict() for x in parse_log_file(filepath)))
    output_filename = output_dir / f"{filepath.stem.replace(':', '-')}.csv"
    if len(df) > 0:
        df.to_csv(output_filename, index=False, header=True, sep="\t")
        print(f"{filepath.name} -> {output_filename.name}")
    else:
        print(f"{filepath.name}: No interesting content")


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


def parse_domain(
    domain: Union[Path, str], out_dir: Union[Path, str], start: datetime, end: datetime
) -> Path:
    """Preprocesses an entire domain into a single csv if the domain has continuous data."""
    if isinstance(domain, str):
        domain = Path(domain)
    if isinstance(out_dir, str):
        out_dir = Path(out_dir)
    if not domain_has_coverage(domain, start, end):
        raise DomainContinuityError(f"{domain} does not have full coverage")
    fieldnames = ["domain", "client", "user_agent", "time"]
    out_file = out_dir / f"{domain.stem}.csv"

    print(f"Parsing domain: {domain.name}...")
    with out_file.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for infile in domain.rglob("*.gz"):
            print(f"  Processing {infile.name}...")
            for record in parse_log_file(infile):
                writer.writerow(record.to_dict())
            print(f"  Processing complete: {infile.name}")
    print(f"Domain processed: {domain.name}")
    return out_file


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
) -> None:
    start = start or datetime(2021, 8, 1)
    end = end or datetime(2022, 7, 31)
    domain = ensure_path(domain)
    out_dir = ensure_path(out_dir)
    if not domain_has_coverage(domain, start, end):
        raise DomainContinuityError(f"{domain} does not have full coverage")
    # delete existing domain files in out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    for existing_file in out_dir.glob(f"{domain.name}_*.csv"):
        existing_file.unlink(missing_ok=True)
    with ThreadPoolExecutor(max_workers=6) as executor:
        for month_dir in domain.glob("????/??"):
            executor.submit(parse_domain_month, month_dir, out_dir)


def parse_to_db(data_dir: Union[Path, str], engine: Engine):
    """Recursively parses gzipped log files and inserts the records into the provided database"""
    if isinstance(data_dir, str):
        data_dir = Path(data_dir)
    for file in data_dir.rglob("*.gz"):
        df = pd.DataFrame(x.to_verbose_dict() for x in parse_log_file(file))
        if len(df) > 0:
            df["id"] = df.filename + "-" + df.row.astype(str)
            df.set_index("id", inplace=True)
            df.drop(columns=["filename", "row"], inplace=True)
            df.to_sql("log", engine, if_exists="append", chunksize=1000, method="multi")
            print(f"{file.name}: done!")
        else:
            print(f"{file.name}: no content")
