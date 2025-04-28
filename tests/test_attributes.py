import unittest
from unittest import mock
import unittest.mock
from typing import Dict, Optional

from watchful import attributes


class TestSetMultiprocessing(unittest.TestCase):
    def setUp(self) -> None:
        self.old_is_multiproc: bool = attributes.IS_MULTIPROC
        self.old_multiproc_chunksize: Optional[int] = (
            attributes.MULTIPROC_CHUNKSIZE
        )

    def tearDown(self) -> None:
        attributes.IS_MULTIPROC = self.old_is_multiproc
        attributes.MULTIPROC_CHUNKSIZE = self.old_multiproc_chunksize

    def test_set_multiprocessing_true(self) -> None:
        """Setting multiprocessing to true also sets a default chunk size"""
        attributes.set_multiprocessing(True)

        self.assertEqual(True, attributes.IS_MULTIPROC)
        self.assertEqual(500, attributes.MULTIPROC_CHUNKSIZE)

    def test_set_multiprocessing_false(self) -> None:
        """Turning multiprocessing off sets chunk size to None as well"""
        attributes.set_multiprocessing(False)

        self.assertEqual(False, attributes.IS_MULTIPROC)
        self.assertEqual(None, attributes.MULTIPROC_CHUNKSIZE)


class TestSetMultiprocChunksize(unittest.TestCase):
    def setUp(self) -> None:
        self.old_is_multiproc: bool = attributes.IS_MULTIPROC
        self.old_multiproc_chunksize: Optional[int] = (
            attributes.MULTIPROC_CHUNKSIZE
        )

    def tearDown(self) -> None:
        attributes.IS_MULTIPROC = self.old_is_multiproc
        attributes.MULTIPROC_CHUNKSIZE = self.old_multiproc_chunksize

    def test_set_multiproc_chunksize(self) -> None:
        """The chunk size is updated to a new value."""
        attributes.set_multiprocessing(True)

        attributes.set_multiproc_chunksize(10)

        self.assertEqual(10, attributes.MULTIPROC_CHUNKSIZE)

    def test_set_multiproc_chunksize_less_than_zero(self) -> None:
        """Chunk size cannot be set to zero."""
        self.assertRaises(AssertionError, attributes.set_multiproc_chunksize, 0)

    def test_set_multiproc_chunksize_not_multiproc(self) -> None:
        """Setting chunk size when multiprocessing is off is a no-op"""
        attributes.set_multiproc_chunksize(10)

        self.assertEqual(None, attributes.MULTIPROC_CHUNKSIZE)


class TestBase64(unittest.TestCase):
    """Tests for watchful.attributes.base64

    See the XXX comment in the source file. This function is named incorrectly,
    and contains a mystery meat encoding.
    """

    def test_base64(self) -> None:
        value = attributes.base64(55)

        self.assertEqual("g", value)

    def test_base64_zero(self) -> None:
        value = attributes.base64(0)

        self.assertEqual("0", value)


class TestBase64Str(unittest.TestCase):
    def test_base64str(self) -> None:
        """An array of bytes is encoding as a comma-separated string"""
        value = attributes.base64str([83, 55, 83, 2291947])

        self.assertEqual("1C,g,1C,8_S[", value)

    def test_base64str_with_compression(self) -> None:
        """Repeated values use compression"""
        value = attributes.base64str([83, 83, 83])

        self.assertEqual("$1C1C1C", value)

    def test_base64str_empty_list(self) -> None:
        """An empty list makes an empty string"""
        value = attributes.base64str([])

        self.assertEqual("", value)


class TestContigSpans(unittest.TestCase):
    def test_contig_spans(self) -> None:
        """An array of spans is encoded into a list of relative offsets"""
        value = attributes.contig_spans([(0, 1), (2, 3), (4, 5)])

        self.assertEqual([0, 1, 1, 1, 1, 1], value)


class TestWriter(unittest.TestCase):
    def test_writer(self) -> None:
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

        self.assertEqual(
            expected_calls,
            write_mock.write.mock_calls,
        )


class TestDatasets(unittest.TestCase):
    @unittest.mock.patch(
        "watchful.client.open", unittest.mock.mock_open(read_data="abc123")
    )
    @unittest.mock.patch("watchful.client.os.path.isfile")
    def test_get_dataset_dir_filepath(self, isfile: mock.Mock) -> None:
        isfile.return_value = True

        summary: Dict[str, str] = {"watchful_home": "/path/to/watchful"}

        (path, file) = attributes.get_dataset_dir_filepath("abc123", summary)

        self.assertEqual("/path/to/watchful/datasets", path)
        self.assertEqual("/path/to/watchful/datasets/raw/abc123", file)
