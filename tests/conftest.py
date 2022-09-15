"""
Utils for tests for Watchful SDK.
"""
################################################################################

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--test_env",
        action="store",
        default="dev"
    )

@pytest.fixture()
def test_env(request):
    return request.config.getoption("--test_env")
