from datetime import datetime
from typing import Protocol
from urllib.parse import urlparse

from apachelogs import LogEntry

from .utils import split_request_line

DEFAULT_FILTERED_EXTENSIONS = [
    ".css",
    ".ico",
    ".jpeg",
    ".jpg",
    ".js",
    ".json",
    ".pdf",
    ".png",
    ".rss",
    ".svg",
    ".tif",
    ".tiff",
    ".txt",
    ".woff",
    ".xml",
]


class Filter(Protocol):
    def filter(self, entry: LogEntry) -> bool:
        ...


class StatusFilter:
    def __init__(self, redirects_ok: bool = True):
        self.redirects_ok = redirects_ok

    def filter(self, entry: LogEntry) -> bool:
        if self.redirects_ok and entry.status in (303, 304, 305):
            return True
        if 200 <= entry.status < 300:
            return True
        return False


class RequestTimeFilter:
    def __init__(self, start: datetime, end: datetime):
        """Validates whether the entry's request_time falls within the configured date range"""
        self.start = start
        self.end = end

    def filter(self, entry: LogEntry):
        if self.start <= entry.request_time < self.end:
            return True
        return False


class MethodFilter:
    def __init__(self, methods: list[str]):
        self.methods = [x.upper() for x in methods]

    def filter(self, entry: LogEntry) -> bool:
        """Validates whether the entry's request method matches the allowed methods"""
        try:
            method, _, _ = split_request_line(entry.request_line)
        except ValueError:
            return False
        else:
            if method.upper() in self.methods:
                return True
        return False


class UriFilter:
    def __init__(self, exclusions: list[str]):
        self.exclusions = exclusions

    def filter(self, entry: LogEntry):
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


class UriExtensionFilter:
    def __init__(self, filtered_extensions: list[str] = None):
        """Validator that filters entries by checking for specific extensions within the uri of the request line."""
        self.filtered_extensions = filtered_extensions or DEFAULT_FILTERED_EXTENSIONS

    def filter(self, entry: LogEntry) -> bool:
        try:
            _, uri, _ = split_request_line(entry.request_line)
        except ValueError:
            return False
        parsed = urlparse(uri)
        for extension in self.filtered_extensions:
            if parsed.path.endswith(extension):
                return False
        return True
