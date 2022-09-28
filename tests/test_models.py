from src.models import LogRecord

from .fakes import FakeLogEntry


def test_log_record_domain():
    entry = FakeLogEntry()
    record = LogRecord("example-dev.010122.gz", 1, entry)
    assert record.domain == "example-dev"


def test_log_record_method():
    entry = FakeLogEntry(request_line="GET / HTTP/1.2")
    record = LogRecord("example.gz", 1, entry)
    assert record.method == "GET"


def test_log_record_uri():
    entry = FakeLogEntry(request_line="GET / HTTP/1.2")
    record = LogRecord("example.gz", 1, entry)
    assert record.uri == "/"
