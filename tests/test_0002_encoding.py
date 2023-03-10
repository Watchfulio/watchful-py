"""
This script tests the csv encoding detection.
"""
################################################################################

import argparse
import sys

from watchful import client


def test_ascii() -> None:
    ascii_text = "A\nabc\ndef".encode("utf-8")
    assert client.is_utf8(ascii_text)


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
    args = parser.parse_args()

    test_ascii()
    test_utf8()
    test_utf16()

    sys.exit(0)
