import pytest

from src.utils import domain_from_filename

DOMAIN_TESTS = ["example", "example.dev", "dev-example"]


@pytest.mark.parametrize("domain", DOMAIN_TESTS)
def test_domain_from_gz_filename(domain: str):
    http_filename = f"{domain}.010122.gz"
    assert domain_from_filename(http_filename) == domain
    https_filename = f"{domain}:443.010122.gz"
    assert domain_from_filename(https_filename) == domain


@pytest.mark.parametrize("domain", DOMAIN_TESTS)
def test_domain_from_filename(domain: str):
    http_filename = f"{domain}.010122"
    https_filename = f"{domain}:443.010122"
    assert domain_from_filename(http_filename) == domain
    assert domain_from_filename(https_filename) == domain
