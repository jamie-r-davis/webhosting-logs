from datetime import datetime
from typing import Optional

from apachelogs import LogEntry

from src.filters import (
    Filter,
    MethodFilter,
    StatusFilter,
    UriExtensionFilter,
    UriFilter,
)

from .utils import domain_from_filename, split_request_line


class LogRecord:
    __slots__ = ("filename", "row", "entry", "_domain", "_method", "_uri")

    def __init__(self, filename: str, row: int, entry: LogEntry):
        self.filename = filename
        self.row = row
        self.entry = entry
        self._domain: Optional[str] = None
        self._method: Optional[str] = None
        self._uri: Optional[str] = None

    def _read_request_line(self):
        self._method, self._uri, _ = split_request_line(self.entry.request_line)

    @property
    def domain(self) -> str:
        if self._domain is None:
            self._domain = domain_from_filename(self.filename)
        return self._domain

    @property
    def uri(self) -> str:
        if not self._uri:
            self._read_request_line()
        return self._uri

    @property
    def method(self) -> str:
        if not self._method:
            self._read_request_line()
        return self._method

    @property
    def user_agent(self) -> str:
        return self.entry.headers_in.get("User-Agent")

    @property
    def request_time(self) -> datetime:
        return self.entry.request_time

    @property
    def status(self) -> int:
        return self.entry.status

    @property
    def remote_host(self) -> str:
        return self.entry.remote_host

    @property
    def valid(self) -> bool:
        validators = [
            MethodFilter(methods=["GET"]),
            StatusFilter(),
            UriFilter(["favicon.ico", "robots.txt", ".well-known"]),
            UriExtensionFilter(),
        ]
        for validator in validators:
            if validator.filter(self.entry) is False:
                return False
        return True

    def to_dict(self) -> dict:
        entry = self.entry
        return {
            "domain": self.domain,
            "client": entry.remote_host,
            "user_agent": self.user_agent,
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
            "method": self.method,
            "uri": self.uri,
            "status": e.status,
            "user_agent": self.user_agent,
            "referer": e.headers_in.get("Referer"),
            "request_duration": e.request_duration_microseconds,
            "bytes_sent": e.bytes_sent,
        }
