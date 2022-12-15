"""
This script tests Watchful SDK version.
"""
################################################################################

import argparse
import importlib.metadata
import os
import sys

import watchful
from watchful import client

THIS_DIR_PATH = os.path.dirname(os.path.abspath(__file__))


def test_version(test_env: str) -> None:
    if test_env in ["dev", "prod", "deploy"]:
        filename = (
            os.path.join(THIS_DIR_PATH, "..", "VERSION_PROD_TEST")
            if test_env == "prod"
            else os.path.join(THIS_DIR_PATH, "..", "src", "watchful", "VERSION")
        )
        with open(filename, encoding="utf-8") as f:
            version = f.readline()
            import_version = importlib.metadata.version(_get_package_name())
            assert watchful.__version__ == version == import_version, (
                f"At least a pair of versions in SDK ({watchful.__version__}), "
                f"source code ({version}) and import ({import_version}) is "
                "different!"
            )
    else:
        raise ValueError(f"No such environment: {test_env}")


def test_connection() -> None:
    conn = client._get_conn()
    assert str(client.PORT).isnumeric(), f"{client.PORT} is not numeric!"
    assert conn.host == str(
        client.HOST
    ), f"The default host ({conn.host} != {client.HOST}) is incorrect!"
    assert conn.port == int(
        client.PORT
    ), f"The default port ({conn.port} != {client.PORT}) is incorrect!"


def _get_package_name() -> str:
    with open(
        os.path.join(THIS_DIR_PATH, "..", "setup.cfg"),
        encoding="utf-8",
    ) as f:
        while line := f.readline():
            if "name" in line:
                return line.split("=")[1].strip()
    raise ValueError('Package "name" is not found in setup file!')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--test_env",
        type=str,
        default="dev",
        help="The test environment to use, dev, prod or deploy; dev if \
            unspecified.",
    )
    args = parser.parse_args()

    test_version(args.test_env)
    test_connection()

    sys.exit(0)
