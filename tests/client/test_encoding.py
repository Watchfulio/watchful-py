import unittest

from watchful import client


class TestEncoding(unittest.TestCase):
    """Test client encodings."""

    def test_ascii(self) -> None:
        ascii_text = "A\nabc\ndef".encode("utf-8")
        assert client.is_utf8(ascii_text)

    def test_utf8(self) -> None:
        utf8_text = "A\nabc\ndéf".encode("utf-8")
        assert client.is_utf8(utf8_text)

    def test_utf16(self) -> None:
        utf16_text = "A\nabc\nde𐐷".encode("utf-16")
        assert not client.is_utf8(utf16_text)
