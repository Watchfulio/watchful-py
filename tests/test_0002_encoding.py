"""
This script tests the csv encoding detection.
"""
################################################################################

from watchful import client
import cchardet as chardet


def test_ascii():
    ascii_text = "A\nabc\ndef".encode("utf-8")
    assert client.is_utf8(ascii_text)


def test_utf8():
    utf8_text = "A\nabc\ndéf".encode("utf-8")
    assert client.is_utf8(utf8_text)


def test_utf16():
    utf16_text = "A\nabc\nde𐐷".encode("utf-16")
    assert not client.is_utf8(utf16_text)
