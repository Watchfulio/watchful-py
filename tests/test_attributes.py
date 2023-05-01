import unittest
from unittest import mock

from watchful import attributes


class TestSetMultiprocessing(unittest.TestCase):
    def setUp(self):
        self.old_is_multiproc = attributes.IS_MULTIPROC
        self.old_multiproc_chunksize = attributes.MULTIPROC_CHUNKSIZE

    def tearDown(self):
        attributes.IS_MULTIPROC = self.old_is_multiproc
        attributes.MULTIPROC_CHUNKSIZE = self.old_multiproc_chunksize

    def test_set_multiprocessing_true(self):
        """Setting multiprocessing to true also sets a default chunk size"""
        attributes.set_multiprocessing(True)

        self.assertEqual(True, attributes.IS_MULTIPROC)
        self.assertEqual(500, attributes.MULTIPROC_CHUNKSIZE)

    def test_set_multiprocessing_false(self):
        """Turning multiprocessing off sets chunk size to None as well"""
        attributes.set_multiprocessing(False)

        self.assertEqual(False, attributes.IS_MULTIPROC)
        self.assertEqual(None, attributes.MULTIPROC_CHUNKSIZE)


class TestSetMultiprocChunksize(unittest.TestCase):
    def setUp(self):
        self.old_is_multiproc = attributes.IS_MULTIPROC
        self.old_multiproc_chunksize = attributes.MULTIPROC_CHUNKSIZE

    def tearDown(self):
        attributes.IS_MULTIPROC = self.old_is_multiproc
        attributes.MULTIPROC_CHUNKSIZE = self.old_multiproc_chunksize

    def test_set_multiproc_chunksize(self):
        """The chunk size is updated to a new value."""
        attributes.set_multiprocessing(True)

        attributes.set_multiproc_chunksize(10)

        self.assertEqual(10, attributes.MULTIPROC_CHUNKSIZE)

    def test_set_multiproc_chunksize_less_than_zero(self):
        """Chunk size cannot be set to zero."""
        self.assertRaises(AssertionError, attributes.set_multiproc_chunksize, 0)

    def test_set_multiproc_chunksize_not_multiproc(self):
        """Setting chunk size when multiprocessing is off is a no-op"""
        attributes.set_multiproc_chunksize(10)

        self.assertEqual(None, attributes.MULTIPROC_CHUNKSIZE)


class TestBase64(unittest.TestCase):
    """Tests for watchful.attributes.base64

    See the XXX comment in the source file. This function is named incorrectly,
    and contains a mystery meat encoding.
    """

    def test_base64(self):
        value = attributes.base64(55)

        self.assertEqual("g", value)

    def test_base64_zero(self):
        value = attributes.base64(0)

        self.assertEqual("0", value)


class TestBase64Str(unittest.TestCase):
    def test_base64str(self):
        """An array of bytes is encoding as a comma-separated string"""
        value = attributes.base64str([83, 55, 83, 2291947])

        self.assertEqual("1C,g,1C,8_S[", value)

    def test_base64str_with_compression(self):
        """Repeated values use compression"""
        value = attributes.base64str([83, 83, 83])

        self.assertEqual("$1C1C1C", value)

    def test_base64str_empty_list(self):
        """An empty list makes an empty string"""
        value = attributes.base64str([])

        self.assertEqual("", value)


class TestContigSpans(unittest.TestCase):
    def test_contig_spans(self):
        """An array of spans is encoded into a list of relative offsets"""
        value = attributes.contig_spans([(0, 1), (2, 3), (4, 5)])

        self.assertEqual([0, 1, 1, 1, 1, 1], value)


class TestWriter(unittest.TestCase):
    def test_writer(self):
        """A writer factory produces a writer object to write attribute files"""
        write_mock = mock.Mock()
        expected_calls = [
            mock.call("{"),
            mock.call('"version"'),
            mock.call(":"),
            mock.call('"0.3"'),
            mock.call(","),
            mock.call('"rows"'),
            mock.call(":"),
            mock.call("2"),
            mock.call(","),
            mock.call('"cols"'),
            mock.call(":"),
            mock.call("2"),
            mock.call("}"),
            mock.call("\n"),
            mock.call('["@"'),
            mock.call(',"key"'),
            mock.call("]"),
            mock.call("\n"),
            mock.call('["$"'),
            mock.call(',"key"'),
            mock.call(',"1"'),
            mock.call("]"),
            mock.call("\n"),
            mock.call('["#110101"'),
            mock.call(","),
            mock.call('["an_name"'),
            mock.call(",1"),
            mock.call(',"#110"'),
            mock.call("]"),
            mock.call("]"),
            mock.call("\n"),
        ]

        writer = attributes.writer(write_mock, 2, 2)
        writer(
            [
                (
                    [(1, 2), (2, 3), (3, 4)],
                    {
                        "key": [
                            "1",
                            1,
                            None,
                        ]
                    },
                    "an_name",
                )
            ]
        )

        self.assertEquals(
            expected_calls,
            write_mock.write.mock_calls,
        )
