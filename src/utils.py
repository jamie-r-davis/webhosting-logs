import functools
import re
from datetime import datetime
from pathlib import Path
from typing import Union

DOMAIN_RE = re.compile(r"^(?P<domain>.+?)(?:[:\-/]443)?\.\d{6}(?:\.gz)?$")
FILE_RE = re.compile("^.*?\.\d{6}(\.gz)?$")


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


def split_request_line(value: str) -> tuple[str, str, str]:
    if not isinstance(value, str):
        raise ValueError(f"Unable to split request line: {value}")
    method, uri, protocol = value.split(" ")
    return method, uri, protocol


def domain_from_filename(file: Union[str, Path]) -> str:
    file = Path(file)
    matched = DOMAIN_RE.match(file.name)
    if matched is None:
        raise ValueError(f"Could not parse domain: {file.name}")
    return matched.group("domain")


def gather_domains(domains: tuple[str], source_dir: Union[Path, str]) -> list[Path]:
    source_dir = Path(source_dir)
    if domains[0] == "all":
        domains = [x for x in source_dir.iterdir() if x.is_dir()]
    else:
        domains = [source_dir / domain for domain in domains]
    return domains


def gather_files(domain: Path):
    for file in sorted(domain.rglob("*"), key=lambda x: x.name):
        if file.is_file() and FILE_RE.match(file.name):
            yield file
