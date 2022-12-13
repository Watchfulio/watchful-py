"""
This script tests Watchful SDK version.
"""
################################################################################

import argparse
import os
import sys

# import importlib.metadata
import watchful
from watchful import client

THIS_DIR_PATH = os.path.dirname(os.path.abspath(__file__))


def test_version(test_env: str) -> None:  # pylint: disable=unused-variable
    if test_env in ["dev", "prod", "deploy"]:
        filename = (
            os.path.join(THIS_DIR_PATH, "..", "VERSION_PROD_TEST")
            if test_env == "prod"
            else os.path.join(THIS_DIR_PATH, "..", "src", "watchful", "VERSION")
        )
        with open(filename, "r", encoding=sys.getdefaultencoding()) as f:
            version = f.readline()
            # assert (watchful.__version__
            #     == importlib.metadata.version(_get_package_name())
            #     == version
            # ), (
            #     f"versions are {watchful.__version__}, "
            #     f"{importlib.metadata.version(_get_package_name())} and "
            #     f"{version}."
            # )
            assert watchful.__version__ == version, (
                f"Versions in SDK ({watchful.__version__}) and source code "
                f"({version}) are different!"
            )
    else:
        assert False, f"No such environment: {test_env}"


def test_connection() -> None:  # pylint: disable=unused-variable
    conn = client._get_conn()
    assert (
        str(client.PORT).isnumeric()
        and conn.host == str(client.HOST)
        and conn.port == int(client.PORT)
    )


# def _get_package_name() -> str:  # pylint: disable=unused-variable
#     with open(
#         os.path.join(THIS_DIR_PATH, "..", "setup.cfg"),
#         "r",
#         encoding=sys.getdefaultencoding()) as f:
#         line = f.readline()
#         while line:
#             if "name" in line:
#                 return line.split("=")[1].strip()
#             line = f.readline()
#         return ""


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
