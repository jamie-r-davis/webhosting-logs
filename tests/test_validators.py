import pytest as pytest

from src.validators import UriValidator

URI_TESTS = [
    ("favicon.ico", False),
    ("robots.txt", False),
    (".well-known", False),
    ("/", True),
    ("/home", True),
    ("favicon.ico?v=test", False),
    ("/home?f=favicon.ico", True),
]


class MockLogEntry:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


@pytest.fixture
def uri_validator():
    exclusions = ["favicon.ico", "robots.txt", ".well-known"]
    return UriValidator(exclusions)


@pytest.mark.parametrize(["uri", "expected"], URI_TESTS)
def test_uri_validator(uri_validator: UriValidator, uri: str, expected: bool):
    request_line = f"GET {uri} HTTP/1.1"
    entry = MockLogEntry(request_line=request_line)
    assert uri_validator.validate(entry) == expected
