import json
import unittest

import responses

from watchful import client


class TestExternal(unittest.TestCase):
    def setUp(self):
        super(TestExternal, self).setUp()
        self.old_scheme = client.SCHEME
        self.old_host = client.HOST
        self.old_port = client.PORT

    def tearDown(self):
        super(TestExternal, self).setUp()
        client.SCHEME = self.old_scheme
        client.HOST = self.old_host
        client.PORT = self.old_port

    def test_external(self):
        """The address to the watchful instance is changed."""
        client.external("lolcathost", "8083", "https")

        self.assertEqual("https", client.SCHEME)
        self.assertEqual("8083", client.PORT)
        self.assertEqual("lolcathost", client.HOST)


class TestClient(unittest.TestCase):
    URL_ROOT = "http://localhost:9002"

    def setUp(self):
        super(TestClient, self).setUp()

    @responses.activate
    def test_list_projects(self):
        responses.add(
            "GET",
            f"{self.URL_ROOT}/projects",
            body=json.dumps(
                [
                    {
                        "title": "An Project",
                        "path": "/path/to/project",
                        "shared": False,
                    },
                    {
                        "title": "An Other Project",
                        "path": "/path/to/other/project",
                        "shared": True,
                    },
                ]
            ),
        )
        expected = [
            {
                "title": "An Project",
                "path": "/path/to/project",
                "shared": False,
            },
            {
                "title": "An Other Project",
                "path": "/path/to/other/project",
                "shared": True,
            },
        ]

        projects = client.list_projects()

        self.assertEqual(expected, projects)

    @responses.activate
    def test_open_project(self):
        responses.add("POST", f"{self.URL_ROOT}/projects", body='"OK"')
        responses.add(
            "POST",
            f"{self.URL_ROOT}/api",
            body=json.dumps(
                {
                    "status": "not current",
                }
            ),
        )
        responses.add(
            "POST",
            f"{self.URL_ROOT}/api",
            body=json.dumps(
                {
                    "status": "current",
                }
            ),
        )

        value = client.open_project("abc123")

        self.assertEqual('"OK"', value)

    @responses.activate
    def test_create_project(self):
        responses.add("POST", f"{self.URL_ROOT}/projects", body='"OK"\n')
        responses.add(
            "POST",
            f"{self.URL_ROOT}/api",
            body=json.dumps(
                {
                    "status": "current",
                }
            ),
        )

        summary = client.create_project("An Project")

        self.assertEqual({"status": "current"}, summary)

    @responses.activate
    def test_create_project_no_title(self):
        responses.add("POST", f"{self.URL_ROOT}/projects", body='"OK"\n')
        responses.add(
            "POST",
            f"{self.URL_ROOT}/api",
            body=json.dumps(
                {
                    "status": "current",
                }
            ),
        )

        value = client.create_project()

        self.assertEqual('"OK"\n', value)

    def test_get_project_id(self):
        summary = {"project_id": "abc123"}

        project_id = client.get_project_id(summary)

        self.assertEqual("abc123", project_id)

    def test_get_project_id_not_open(self):
        summary = {}

        self.assertRaises(
            client.WatchfulAppInstanceError, client.get_project_id, summary
        )

    def test_get_watchful_home(self):
        summary = {"watchful_home": "/path/to/watchful/home"}

        home = client.get_watchful_home(summary)

        self.assertEqual("/path/to/watchful/home", home)

    @unittest.mock.patch("watchful.client.os.path.expanduser")
    def test_get_watchful_home_no_project_open(self, expanduser):
        expanduser.return_value = "/home/user"
        home = client.get_watchful_home({})

        self.assertEqual("/home/user/watchful", home)

    def test_get_watchful_home_no_project_open_not_local(self):
        self.assertRaises(
            client.WatchfulAppInstanceError,
            client.get_watchful_home,
            {},
            is_local=False,
        )

    def test_get_datasets_dir(self):
        summary = {"watchful_home": "/path/to/watchful"}

        path = client.get_datasets_dir(summary)

        self.assertEqual("/path/to/watchful/datasets", path)

    @unittest.mock.patch(
        "watchful.client.open", unittest.mock.mock_open(read_data="abc123")
    )
    @unittest.mock.patch("watchful.client.os.path.isfile")
    def test_get_datasets_filepath(self, isfile):
        isfile.return_value = True

        summary = {"datasets": ["abc123"], "watchful_home": "/path/to/watchful"}

        path = client.get_dataset_filepath(summary["datasets"][0], summary)

        self.assertEqual("/path/to/watchful/datasets/raw/abc123", path)

    def test_get_datasets_filepath_nonexistent(self):
        summary = {"datasets": ["abc123"], "watchful_home": "/path/to/watchful"}

        self.assertRaises(
            FileNotFoundError,
            client.get_dataset_filepath,
            summary["datasets"][0],
            summary,
        )

    def test_get_datasets_filepath_not_local(self):
        summary = {"datasets": ["abc123"], "watchful_home": "/path/to/watchful"}

        path = client.get_dataset_filepath(
            summary["datasets"][0], summary, is_local=False
        )

        self.assertEqual("", path)
