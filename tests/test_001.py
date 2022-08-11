"""
    Test_001 for Watchful Python Package
"""

import watchful
from watchful import client
import os
import importlib.metadata

THIS_FILE_PATH = os.path.dirname(os.path.abspath(__file__))

def test_version() -> None:  # pylint: disable=unused-variable  
    assert watchful.__version__ \
            == importlib.metadata.version(_get_package_name()) \
            == open(os.path.join(THIS_FILE_PATH, "..", "src", "watchful", "VERSION"), "r").read()

def test_connection() -> None:  # pylint: disable=unused-variable 
    conn = client._get_conn()
    assert str(client.PORT).isnumeric() \
            and conn.host == str(client.HOST) \
            and conn.port == int(client.PORT)

def _get_package_name() -> str:  # pylint: disable=unused-variable  
    setup_data = open(os.path.join(THIS_FILE_PATH, "..", "setup.cfg"), "r")
    while line := setup_data.readline():
        if "name" in line:
            return line.split("=")[1].strip()
    return ""
