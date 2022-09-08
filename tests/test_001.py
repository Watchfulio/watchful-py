"""
Basic tests for Watchful SDK.
"""
################################################################################

import os
import sys
import importlib.metadata
import watchful
from watchful import client

THIS_DIR_PATH = os.path.dirname(os.path.abspath(__file__))


def test_version() -> None:  # pylint: disable=unused-variable
    with open(
        os.path.join(THIS_DIR_PATH, "..", "src", "watchful", "VERSION"),
        "r",
        encoding=sys.getdefaultencoding()) as f:
        assert watchful.__version__ \
            == importlib.metadata.version(_get_package_name()) \
            == f.readline()

def test_connection() -> None:  # pylint: disable=unused-variable
    conn = client._get_conn()
    assert str(client.PORT).isnumeric() \
        and conn.host == str(client.HOST) \
        and conn.port == int(client.PORT)

def _get_package_name() -> str:  # pylint: disable=unused-variable
    with open(
        os.path.join(THIS_DIR_PATH, "..", "setup.cfg"),
        "r",
        encoding=sys.getdefaultencoding()) as f:
        while line := f.readline():
            if "name" in line:
                return line.split("=")[1].strip()
        return ""


if __name__ == "__main__":

    test_version()
    test_connection()

    sys.exit(0)
