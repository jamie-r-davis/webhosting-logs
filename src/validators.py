from datetime import datetime
from typing import Protocol
from urllib.parse import urlparse

from apachelogs import LogEntry

DEFAULT_FILTERED_EXTENSIONS = [
    ".png",
    ".jpg",
    ".jpeg",
    ".pdf",
    ".svg",
    ".ico",
    ".css",
    ".js",
]


def split_request_line(value: str) -> tuple[str, str, str]:
    if not isinstance(value, str):
        raise ValueError(f"Unable to split request line: {value}")
    method, uri, protocol = value.split(" ")
    return method, uri, protocol


class Validator(Protocol):
    def validate(self, entry: LogEntry) -> bool:
        ...


class StatusValidator:
    def validate(self, entry: LogEntry) -> bool:
        if entry.status in (303, 304, 305):
            return True
        if 200 <= entry.status < 300:
            return True
        return False


class RequestTimeValidator:
    def __init__(self, start: datetime, end: datetime):
        """Validates whether the entry's request_time falls within the configured date range"""
        self.start = start
        self.end = end

    def validate(self, entry: LogEntry):
        if self.start <= entry.request_time < self.end:
            return True
        return False


class MethodValidator:
    def __init__(self, methods: list[str]):
        self.methods = [x.upper() for x in methods]

    def validate(self, entry: LogEntry) -> bool:
        """Validates whether the entry's request method matches the allowed methods"""
        try:
            method, _, _ = split_request_line(entry.request_line)
        except ValueError:
            return False
        else:
            if method.upper() in self.methods:
                return True
        return False


class UriValidator:
    def __init__(self, exclusions: list[str]):
        self.exclusions = exclusions

    def validate(self, entry: LogEntry):
        """Validates whether the entry's URI is well-formed and does not match any exclusions."""
        try:
            _, uri, _ = split_request_line(entry.request_line)
        except ValueError:
            return False
        parsed = urlparse(uri)
        for exclusion in self.exclusions:
            if exclusion in parsed.path:
                return False
        return True


class UriExtensionValidator:
    def __init__(self, filtered_extensions: list[str] = None):
        """Validator that filters entries by checking for specific extensions within the uri of the request line."""
        self.filtered_extensions = filtered_extensions or DEFAULT_FILTERED_EXTENSIONS

    def validate(self, entry: LogEntry) -> bool:
        try:
            _, uri, _ = split_request_line(entry.request_line)
        except ValueError:
            return False
        parsed = urlparse(uri)
        for extension in self.filtered_extensions:
            if parsed.path.endswith(extension):
                return False
        return True
