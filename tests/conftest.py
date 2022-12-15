"""
This script is a utility for Watchful SDK tests.
"""
################################################################################

import pytest


def pytest_addoption(parser):
    parser.addoption("--test_env", action="store", default="dev")


@pytest.fixture()
def test_env(request):
    return request.config.getoption("--test_env")
