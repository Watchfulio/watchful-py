"""
This script tests the csv encoding detection.
"""
################################################################################

import argparse
import sys

from watchful import client


def test_ascii(test_env: str) -> None:
    ascii_text = "A\nabc\ndef".encode("utf-8")
    if test_env in ["dev", "deploy"]:
        assert client.is_utf8(ascii_text)
    elif test_env in ["prod"]:
        assert not client.is_utf8(ascii_text)
    else:
        raise ValueError(f"No such environment: {test_env}")


def test_utf8() -> None:
    utf8_text = "A\nabc\ndéf".encode("utf-8")
    assert client.is_utf8(utf8_text)


def test_utf16() -> None:
    utf16_text = "A\nabc\nde𐐷".encode("utf-16")
    assert not client.is_utf8(utf16_text)


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

    test_ascii(args.test_env)
    test_utf8()
    test_utf16()

    sys.exit(0)
