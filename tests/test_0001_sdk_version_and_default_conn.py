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


def test_connection(test_env: str) -> None:
    if test_env in ["dev", "prod", "deploy"]:
        conn_url = client._get_conn_url()
        scheme, host_port = conn_url.split("://")
        host, port = (
            host_port.split(":") if client.PORT else (host_port, client.PORT)
        )
        assert (
            scheme == client.SCHEME
        ), f"The default scheme ({scheme} != {client.SCHEME}) is incorrect!"
        assert client.PORT.isnumeric(), f"{client.PORT} is not numeric!"
        assert (
            host == client.HOST
        ), f"The default host ({host} != {client.HOST}) is incorrect!"
        assert (
            port == client.PORT
        ), f"The default port ({port} != {client.PORT}) is incorrect!"
    else:
        raise ValueError(f"No such environment: {test_env}")


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
    test_connection(args.test_env)

    sys.exit(0)
