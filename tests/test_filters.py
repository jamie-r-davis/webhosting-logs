import pytest as pytest

from src.filters import MethodFilter, UriExtensionFilter, UriFilter

from .fakes import FakeLogEntry

URI_TESTS = [
    ("favicon.ico", False),
    ("robots.txt", False),
    (".well-known", False),
    ("/", True),
    ("/home", True),
    ("favicon.ico?v=test", False),
    ("/home?f=favicon.ico", True),
    ("/.well-known/alfacgiapi", False),
]

URI_EXTENSION_TESTS = [
    ("/", True),
    ("/logo.png", False),
    ("/img/header.jpg", False),
    ("/robots.txt", False),
    ("/profile?id=123&type=jpg", True),
]


@pytest.fixture
def uri_validator():
    exclusions = ["favicon.ico", "robots.txt", ".well-known"]
    return UriFilter(exclusions)


@pytest.mark.parametrize(["uri", "expected"], URI_TESTS)
def test_uri_filter(uri: str, expected: bool):
    exclusions = ["favicon.ico", "robots.txt", ".well-known"]
    uri_filter = UriFilter(exclusions)
    request_line = f"GET {uri} HTTP/1.1"
    entry = FakeLogEntry(request_line=request_line)
    assert uri_filter.filter(entry) == expected


@pytest.mark.parametrize(["uri", "expected"], URI_EXTENSION_TESTS)
def test_uri_extension_filter(uri: str, expected: bool):
    ext_filter = UriExtensionFilter()
    request_line = f"GET {uri} HTTP/1.2"
    entry = FakeLogEntry(request_line=request_line)
    assert ext_filter.filter(entry) == expected


def test_uri_extension_filter_with_css_in_path():
    request_line = "GET /wp-admin/css/ HTTP/1.1"
    entry = FakeLogEntry(request_line=request_line)
    ext_filter = UriExtensionFilter([".css"])
    assert ext_filter.filter(entry) is True


@pytest.mark.parametrize(
    ["method", "expected"], [("GET", True), ("POST", True), ("DELETE", False)]
)
def test_method_filter(method: str, expected: bool):
    method_filter = MethodFilter(["GET", "POST"])
    entry = FakeLogEntry(request_line=f"{method} /home HTTP/1.2")
    assert method_filter.filter(entry) == expected
