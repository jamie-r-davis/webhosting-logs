from datetime import datetime
from typing import Protocol

from apachelogs import LogEntry


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

    def validate(self, entry: LogEntry):
        """Validates whether the entry's request method matches the allowed methods"""
        method: str = entry.request_line.split(" ")[0]
        if method.upper() in self.methods:
            return True
        return False
