import unittest
from unittest import mock
from urllib.parse import urljoin

import responses

from watchful.client2 import Client, Summary
from watchful import types

URL_ROOT = "http://example.com:9001"
DATASET_CONTENTS = """columnA,columnB
a,b
c,d
e,f
g,h"""


class TestSummary(unittest.TestCase):
    """Tests for watchful.client2.Summary"""

    def test_dataset_filepath(self):
        summary = Summary(
            project_id="abc123",
            title="test title",
            datasets=["abc"],
            auto_complete="",
            cand_seq_full="",
            cand_seq_prefix="",
            candidates="",
            classes="",
            column_flags="",
            disagreements="",
            enrichment_tasks="",
            error_msg=None,
            error_verb=None,
            export_preview=None,
            exports="",
            field_names="",
            hand_labels="",
            hinters="",
            is_shared=False,
            messages="",
            n_candidates="",
            n_handlabels="",
            ner_hl_text="",
            notifications="",
            precision_candidate="",
            project_config="",
            published_title="",
            pull_actions="",
            push_actions="",
            query="",
            query_breakdown="",
            query_completed="",
            query_end="",
            query_examined="",
            query_full_rows="",
            query_history="",
            query_hit_count="",
            query_page="",
            selected_class="",
            selections="",
            show_notification_badge="",
            state_seq="",
            status="",
            suggestion="",
            suggestions="",
            unlabeled_candidate="",
            watchful_home="/path/to/watchful",
        )

        self.assertEqual(
            "/path/to/watchful/datasets/refs/abc", summary.dataset_filepath
        )


class TestClient(unittest.TestCase):
    """Tests for watchful.Client"""

    @responses.activate
    def test_list_projects(self):
        """All projects are all listed."""
        responses.add(
            "GET",
            urljoin(URL_ROOT, "projects"),
            json=[
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
            ],
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

        client = Client(URL_ROOT)
        projects = client.list_projects()

        self.assertEqual(expected, projects)

    @responses.activate
    @mock.patch("watchful.client2.uuid.uuid4")
    def test_create_project(self, uuid4):
        uuid4.return_value = "7"
        responses.add(
            "POST",
            urljoin(URL_ROOT, "projects"),
            body="OK",
        )
        responses.add(
            "POST",
            urljoin(URL_ROOT, "api/_stream/7/0/true"),
        )
        responses.add(
            "POST",
            urljoin(URL_ROOT, "api/_stream/7"),
            json={"id": "12"},
        )
        responses.add(
            "POST",
            urljoin(URL_ROOT, "api"),
            json={
                "project_id": "abc123",
                "title": "my new project",
                "datasets": ["12"],
                "auto_complete": "",
                "cand_seq_full": "",
                "cand_seq_prefix": "",
                "candidates": [],
                "classes": "",
                "column_flags": "",
                "disagreements": "",
                "enrichment_tasks": "",
                "error_msg": None,
                "error_verb": None,
                "export_preview": None,
                "exports": [],
                "field_names": [],
                "hand_labels": [],
                "hinters": [],
                "is_shared": False,
                "messages": [],
                "n_candidates": "",
                "n_handlabels": "",
                "ner_hl_text": "",
                "notifications": "",
                "precision_candidate": "",
                "project_config": "",
                "published_title": "",
                "pull_actions": "",
                "push_actions": "",
                "query": "",
                "query_breakdown": "",
                "query_completed": "",
                "query_end": "",
                "query_examined": "",
                "query_full_rows": "",
                "query_history": "",
                "query_hit_count": "",
                "query_page": "",
                "selected_class": "",
                "selections": "",
                "show_notification_badge": "",
                "state_seq": "",
                "status": "",
                "suggestion": "",
                "suggestions": "",
                "unlabeled_candidate": "",
            },
        )

        client = Client(URL_ROOT)
        summary = client.create_project(
            "my new project",
            data=DATASET_CONTENTS,
            columns=["columnA", "columnB"],
        )

        self.assertEqual("my new project", summary.title)

    @responses.activate
    def test_flag_inference_columns(self):
        """Column flags are set."""
        responses.add(
            "POST",
            urljoin(URL_ROOT, "api"),
            json={
                "project_id": "abc123",
                "title": "my new project",
                "datasets": ["12"],
                "auto_complete": "",
                "cand_seq_full": "",
                "cand_seq_prefix": "",
                "candidates": [],
                "classes": "",
                "column_flags": {"enrichable": [True, False, False]},
                "disagreements": "",
                "enrichment_tasks": "",
                "error_msg": None,
                "error_verb": None,
                "export_preview": None,
                "exports": [],
                "field_names": [],
                "hand_labels": [],
                "hinters": [],
                "is_shared": False,
                "messages": [],
                "n_candidates": "",
                "n_handlabels": "",
                "ner_hl_text": "",
                "notifications": "",
                "precision_candidate": "",
                "project_config": "",
                "published_title": "",
                "pull_actions": "",
                "push_actions": "",
                "query": "",
                "query_breakdown": "",
                "query_completed": "",
                "query_end": "",
                "query_examined": "",
                "query_full_rows": "",
                "query_history": "",
                "query_hit_count": "",
                "query_page": "",
                "selected_class": "",
                "selections": "",
                "show_notification_badge": "",
                "state_seq": "",
                "status": "",
                "suggestion": "",
                "suggestions": "",
                "unlabeled_candidate": "",
            },
        )

        flags = [True, False, False]
        client = Client(URL_ROOT)
        summary = client.flag_columns(flags, "enrichable")

        self.assertEqual({"enrichable": flags}, summary.column_flags)

    @responses.activate
    def test_flag_enrich_columns(self):
        """Column flags are set."""
        responses.add(
            "POST",
            urljoin(URL_ROOT, "api"),
            json={
                "project_id": "abc123",
                "title": "my new project",
                "datasets": ["12"],
                "auto_complete": "",
                "cand_seq_full": "",
                "cand_seq_prefix": "",
                "candidates": [],
                "classes": "",
                "column_flags": {"enrichable": [True, False, True]},
                "disagreements": "",
                "enrichment_tasks": "",
                "error_msg": None,
                "error_verb": None,
                "export_preview": None,
                "exports": [],
                "field_names": [],
                "hand_labels": [],
                "hinters": [],
                "is_shared": False,
                "messages": [],
                "n_candidates": "",
                "n_handlabels": "",
                "ner_hl_text": "",
                "notifications": "",
                "precision_candidate": "",
                "project_config": "",
                "published_title": "",
                "pull_actions": "",
                "push_actions": "",
                "query": "",
                "query_breakdown": "",
                "query_completed": "",
                "query_end": "",
                "query_examined": "",
                "query_full_rows": "",
                "query_history": "",
                "query_hit_count": "",
                "query_page": "",
                "selected_class": "",
                "selections": "",
                "show_notification_badge": "",
                "state_seq": "",
                "status": "",
                "suggestion": "",
                "suggestions": "",
                "unlabeled_candidate": "",
            },
        )

        flags = [True, False, True]
        client = Client(URL_ROOT)
        summary = client.flag_columns(flags, "enrichable")

        self.assertEqual({"enrichable": flags}, summary.column_flags)

    def test_flag_columns_not_valid(self):
        """Only "inferencable" and "enrichable" are valid flags."""
        client = Client(URL_ROOT)

        self.assertRaises(
            ValueError, client.flag_columns, [False, False], "my-flag"
        )

    @responses.activate
    def test_create_class(self):
        """A text class is created."""
        responses.add(
            responses.POST,
            urljoin(URL_ROOT, "api"),
            json={
                "project_id": "abc123",
                "title": "my new project",
                "datasets": ["12"],
                "auto_complete": "",
                "cand_seq_full": "",
                "cand_seq_prefix": "",
                "candidates": [],
                "classes": "",
                "column_flags": {"enrichable": [True, False, False]},
                "disagreements": "",
                "enrichment_tasks": "",
                "error_msg": None,
                "error_verb": None,
                "export_preview": None,
                "exports": [],
                "field_names": [],
                "hand_labels": [],
                "hinters": [],
                "is_shared": False,
                "messages": [],
                "n_candidates": "",
                "n_handlabels": "",
                "ner_hl_text": "",
                "notifications": "",
                "precision_candidate": "",
                "project_config": "",
                "published_title": "",
                "pull_actions": "",
                "push_actions": "",
                "query": "",
                "query_breakdown": "",
                "query_completed": "",
                "query_end": "",
                "query_examined": "",
                "query_full_rows": "",
                "query_history": "",
                "query_hit_count": "",
                "query_page": "",
                "selected_class": "",
                "selections": "",
                "show_notification_badge": "",
                "state_seq": "",
                "status": "",
                "suggestion": "",
                "suggestions": "",
                "unlabeled_candidate": "",
            },
        )
        client = Client(URL_ROOT)
        client.create_class("myClass")

    @responses.activate
    def test_delete_class(self):
        """A text class is deleted."""
        responses.add(
            responses.POST,
            urljoin(URL_ROOT, "api"),
            json={
                "project_id": "abc123",
                "title": "my new project",
                "datasets": ["12"],
                "auto_complete": "",
                "cand_seq_full": "",
                "cand_seq_prefix": "",
                "candidates": [],
                "classes": "",
                "column_flags": {"enrichable": [True, False, False]},
                "disagreements": "",
                "enrichment_tasks": "",
                "error_msg": None,
                "error_verb": None,
                "export_preview": None,
                "exports": [],
                "field_names": [],
                "hand_labels": [],
                "hinters": [],
                "is_shared": False,
                "messages": [],
                "n_candidates": "",
                "n_handlabels": "",
                "ner_hl_text": "",
                "notifications": "",
                "precision_candidate": "",
                "project_config": "",
                "published_title": "",
                "pull_actions": "",
                "push_actions": "",
                "query": "",
                "query_breakdown": "",
                "query_completed": "",
                "query_end": "",
                "query_examined": "",
                "query_full_rows": "",
                "query_history": "",
                "query_hit_count": "",
                "query_page": "",
                "selected_class": "",
                "selections": "",
                "show_notification_badge": "",
                "state_seq": "",
                "status": "",
                "suggestion": "",
                "suggestions": "",
                "unlabeled_candidate": "",
            },
        )

        client = Client(URL_ROOT)
        client.delete_class("myClass")

    @responses.activate
    def test_set_base_rate(self):
        """The base rate for a class is set."""
        responses.add(
            "POST",
            urljoin(URL_ROOT, "api"),
            json={
                "project_id": "abc123",
                "title": "my new project",
                "datasets": ["12"],
                "auto_complete": "",
                "cand_seq_full": "",
                "cand_seq_prefix": "",
                "candidates": [],
                "classes": {
                    "my-class": {
                        "base_rate_given": 10,
                        "base_rate_pdf": [1, 0, 0, 0],
                        "class_type": "ftc",
                        "confidences": [[0, "BaseRate"]],
                        "description": {
                            "error_rate": "Error rate computed over all plabels",
                            "precision": (
                                "Precision computed over hand_labels.\n"
                                "Sum of plables for positively hand labeled examples."
                            ),
                            "recall": (
                                "Recall computed over hand labels.\n"
                                "Average plabel of the positively hand labeled "
                                "examples."
                            ),
                        },
                        "error_rate": [[0, "BaseRate"]],
                        "hand_label_distribution_counts_negative": [0, 0, 0],
                        "hand_label_distribution_counts_positive": [0, 0, 0],
                        "label_distribution": [0, 0, 0],
                        "label_distribution_counts": [0, 0, 0],
                        "precision": [[0, "BaseRate"]],
                        "recall": [[0, "BaseRate"]],
                        "thresholds": [50, 50],
                    }
                },
                "column_flags": {"enrichable": [True, False, False]},
                "disagreements": "",
                "enrichment_tasks": "",
                "error_msg": None,
                "error_verb": None,
                "export_preview": None,
                "exports": [],
                "field_names": [],
                "hand_labels": [],
                "hinters": [],
                "is_shared": False,
                "messages": [],
                "n_candidates": "",
                "n_handlabels": "",
                "ner_hl_text": "",
                "notifications": "",
                "precision_candidate": "",
                "project_config": "",
                "published_title": "",
                "pull_actions": "",
                "push_actions": "",
                "query": "",
                "query_breakdown": "",
                "query_completed": "",
                "query_end": "",
                "query_examined": "",
                "query_full_rows": "",
                "query_history": "",
                "query_hit_count": "",
                "query_page": "",
                "selected_class": "",
                "selections": "",
                "show_notification_badge": "",
                "state_seq": "",
                "status": "",
                "suggestion": "",
                "suggestions": "",
                "unlabeled_candidate": "",
            },
        )

        client = Client(URL_ROOT)
        summary = client.set_base_rate("my-class", 10)

        self.assertIn("my-class", summary.classes)

    @responses.activate
    def test_set_config(self):
        """A configuration option is set."""
        responses.add(
            "POST",
            urljoin(URL_ROOT, "config"),
        )
        responses.add(
            "GET", urljoin(URL_ROOT, "config"), json={"username": "bobbyhill"}
        )

        client = Client(URL_ROOT)
        config = client.set_config("username", "bobbyhill")

        self.assertEqual({"username": "bobbyhill"}, config)

    @responses.activate
    def test_set_hub_url(self):
        """A hub url is set."""
        responses.add(
            "POST",
            urljoin(URL_ROOT, "config"),
        )
        responses.add(
            "GET",
            urljoin(URL_ROOT, "config"),
            json={"remote": "http://watchful.example.com"},
        )

        client = Client(URL_ROOT)
        config = client.set_hub_url("http://watchful.example.com")

        self.assertEqual({"remote": "http://watchful.example.com"}, config)

    @responses.activate
    def test_login(self):
        """A user can log in to a remote hub."""
        responses.add(
            responses.POST, urljoin(URL_ROOT, "remote"), body="myToken"
        )

        client = Client(URL_ROOT)
        data = client.login("myUserName", "NotAVerySecurePassword")

        self.assertTrue(
            responses.assert_call_count(urljoin(URL_ROOT, "remote"), 1)
        )
        self.assertEqual(data, "myToken")

    @responses.activate
    def test_publish(self):
        """Project data is published to a hub."""
        responses.add(responses.POST, urljoin(URL_ROOT, "remote"), body="OK")

        client = Client(URL_ROOT)
        data = client.publish("myToken")

        self.assertTrue(
            responses.assert_call_count(urljoin(URL_ROOT, "remote"), 1)
        )
        self.assertEqual(data, "OK")

    @responses.activate
    def test_fetch(self):
        """Project state is fetched from a hub."""
        responses.add(responses.POST, urljoin(URL_ROOT, "remote"), body="OK")

        client = Client(URL_ROOT)
        data = client.fetch("myToken")

        self.assertTrue(
            responses.assert_call_count(urljoin(URL_ROOT, "remote"), 1)
        )
        self.assertEqual(data, "OK")

    @responses.activate
    def test_pull(self):
        """Project data is pulled from a hub."""
        responses.add(responses.POST, urljoin(URL_ROOT, "remote"), body="OK")

        client = Client(URL_ROOT)
        data = client.pull("myToken")

        self.assertTrue(
            responses.assert_call_count(urljoin(URL_ROOT, "remote"), 1)
        )
        self.assertEqual(data, "OK")

    @responses.activate
    def test_push(self):
        """Project data is pushed to a hub."""
        responses.add(responses.POST, urljoin(URL_ROOT, "remote"), body="OK")

        client = Client(URL_ROOT)
        data = client.push("myToken")

        self.assertTrue(
            responses.assert_call_count(urljoin(URL_ROOT, "remote"), 1)
        )
        self.assertEqual(data, "OK")

    @responses.activate
    def test_peek(self):
        """Hub data can be viewed without a pull."""
        responses.add(
            responses.POST,
            urljoin(URL_ROOT, "remote"),
            body="OK",
        )

        client = Client(URL_ROOT)
        data = client.peek("myToken")

        self.assertTrue(
            responses.assert_call_count(urljoin(URL_ROOT, "remote"), 1)
        )
        self.assertEqual(data, "OK")

    @responses.activate
    def test_whoami(self):
        """A user can find out who they are logged in as."""
        responses.add(responses.POST, urljoin(URL_ROOT, "remote"), body="OK")

        client = Client(URL_ROOT)
        data = client.whoami("myToken")

        self.assertTrue(
            responses.assert_call_count(urljoin(URL_ROOT, "remote"), 1)
        )
        self.assertEqual(data, "OK")

    @responses.activate
    def test_create_hinter(self):
        """A hinter is created."""
        responses.add(
            "POST",
            urljoin(URL_ROOT, "api"),
            json={
                "project_id": "abc123",
                "title": "my new project",
                "datasets": ["12"],
                "auto_complete": "",
                "cand_seq_full": "",
                "cand_seq_prefix": "",
                "candidates": [],
                "classes": {
                    "my-class": {
                        "base_rate_given": 10,
                        "base_rate_pdf": [1, 0, 0, 0],
                        "class_type": "ftc",
                        "confidences": [[0, "BaseRate"]],
                        "description": {
                            "error_rate": "Error rate computed over all plabels",
                            "precision": (
                                "Precision computed over hand_labels.\n"
                                "Sum of plables for positively hand labeled examples."
                            ),
                            "recall": (
                                "Recall computed over hand labels.\n"
                                "Average plabel of the positively hand labeled "
                                "examples."
                            ),
                        },
                        "error_rate": [[0, "BaseRate"]],
                        "hand_label_distribution_counts_negative": [0, 0, 0],
                        "hand_label_distribution_counts_positive": [0, 0, 0],
                        "label_distribution": [0, 0, 0],
                        "label_distribution_counts": [0, 0, 0],
                        "precision": [[0, "BaseRate"]],
                        "recall": [[0, "BaseRate"]],
                        "thresholds": [50, 50],
                    }
                },
                "column_flags": {"enrichable": [True, False, False]},
                "disagreements": "",
                "enrichment_tasks": "",
                "error_msg": None,
                "error_verb": None,
                "export_preview": None,
                "exports": [],
                "field_names": [],
                "hand_labels": [],
                "hinters": [],
                "is_shared": False,
                "messages": [],
                "n_candidates": "",
                "n_handlabels": "",
                "ner_hl_text": "",
                "notifications": "",
                "precision_candidate": "",
                "project_config": "",
                "published_title": "",
                "pull_actions": "",
                "push_actions": "",
                "query": "",
                "query_breakdown": "",
                "query_completed": "",
                "query_end": "",
                "query_examined": "",
                "query_full_rows": "",
                "query_history": "",
                "query_hit_count": "",
                "query_page": "",
                "selected_class": "",
                "selections": "",
                "show_notification_badge": "",
                "state_seq": "",
                "status": "current",
                "suggestion": "",
                "suggestions": "",
                "unlabeled_candidate": "",
            },
        )

        client = Client(URL_ROOT)
        client.create_hinter("myHinter", "bareword", 65)

        self.assertTrue(
            responses.assert_call_count(urljoin(URL_ROOT, "api"), 2)
        )

    @responses.activate
    def test_create_external_hinter(self):
        """An external hinter is created."""
        responses.add(
            "POST",
            urljoin(URL_ROOT, "api"),
            json={
                "project_id": "abc123",
                "title": "my new project",
                "datasets": ["12"],
                "auto_complete": "",
                "cand_seq_full": "",
                "cand_seq_prefix": "",
                "candidates": [],
                "classes": {
                    "my-class": {
                        "base_rate_given": 10,
                        "base_rate_pdf": [1, 0, 0, 0],
                        "class_type": "ftc",
                        "confidences": [[0, "BaseRate"]],
                        "description": {
                            "error_rate": "Error rate computed over all plabels",
                            "precision": (
                                "Precision computed over hand_labels.\n"
                                "Sum of plables for positively hand labeled examples."
                            ),
                            "recall": (
                                "Recall computed over hand labels.\n"
                                "Average plabel of the positively hand labeled "
                                "examples."
                            ),
                        },
                        "error_rate": [[0, "BaseRate"]],
                        "hand_label_distribution_counts_negative": [0, 0, 0],
                        "hand_label_distribution_counts_positive": [0, 0, 0],
                        "label_distribution": [0, 0, 0],
                        "label_distribution_counts": [0, 0, 0],
                        "precision": [[0, "BaseRate"]],
                        "recall": [[0, "BaseRate"]],
                        "thresholds": [50, 50],
                    }
                },
                "column_flags": {"enrichable": [True, False, False]},
                "disagreements": "",
                "enrichment_tasks": "",
                "error_msg": None,
                "error_verb": None,
                "export_preview": None,
                "exports": [],
                "field_names": [],
                "hand_labels": [],
                "hinters": [],
                "is_shared": False,
                "messages": [],
                "n_candidates": "",
                "n_handlabels": "",
                "ner_hl_text": "",
                "notifications": "",
                "precision_candidate": "",
                "project_config": "",
                "published_title": "",
                "pull_actions": "",
                "push_actions": "",
                "query": "",
                "query_breakdown": "",
                "query_completed": "",
                "query_end": "",
                "query_examined": "",
                "query_full_rows": "",
                "query_history": "",
                "query_hit_count": "",
                "query_page": "",
                "selected_class": "",
                "selections": "",
                "show_notification_badge": "",
                "state_seq": "",
                "status": "current",
                "suggestion": "",
                "suggestions": "",
                "unlabeled_candidate": "",
            },
        )

        client = Client(URL_ROOT)
        client.create_external_hinter(
            "myHinter", types.ClassificationType.FTC, 65
        )

        self.assertTrue(
            responses.assert_call_count(urljoin(URL_ROOT, "api"), 2)
        )

    @responses.activate
    def test_delete_hinter(self):
        """A hinter is deleted."""
        responses.add(
            "POST",
            urljoin(URL_ROOT, "api"),
            json={
                "project_id": "abc123",
                "title": "my new project",
                "datasets": ["12"],
                "auto_complete": "",
                "cand_seq_full": "",
                "cand_seq_prefix": "",
                "candidates": [],
                "classes": {
                    "my-class": {
                        "base_rate_given": 10,
                        "base_rate_pdf": [1, 0, 0, 0],
                        "class_type": "ftc",
                        "confidences": [[0, "BaseRate"]],
                        "description": {
                            "error_rate": "Error rate computed over all plabels",
                            "precision": (
                                "Precision computed over hand_labels.\n"
                                "Sum of plables for positively hand labeled examples."
                            ),
                            "recall": (
                                "Recall computed over hand labels.\n"
                                "Average plabel of the positively hand labeled "
                                "examples."
                            ),
                        },
                        "error_rate": [[0, "BaseRate"]],
                        "hand_label_distribution_counts_negative": [0, 0, 0],
                        "hand_label_distribution_counts_positive": [0, 0, 0],
                        "label_distribution": [0, 0, 0],
                        "label_distribution_counts": [0, 0, 0],
                        "precision": [[0, "BaseRate"]],
                        "recall": [[0, "BaseRate"]],
                        "thresholds": [50, 50],
                    }
                },
                "column_flags": {"enrichable": [True, False, False]},
                "disagreements": "",
                "enrichment_tasks": "",
                "error_msg": None,
                "error_verb": None,
                "export_preview": None,
                "exports": [],
                "field_names": [],
                "hand_labels": [],
                "hinters": [],
                "is_shared": False,
                "messages": [],
                "n_candidates": "",
                "n_handlabels": "",
                "ner_hl_text": "",
                "notifications": "",
                "precision_candidate": "",
                "project_config": "",
                "published_title": "",
                "pull_actions": "",
                "push_actions": "",
                "query": "",
                "query_breakdown": "",
                "query_completed": "",
                "query_end": "",
                "query_examined": "",
                "query_full_rows": "",
                "query_history": "",
                "query_hit_count": "",
                "query_page": "",
                "selected_class": "",
                "selections": "",
                "show_notification_badge": "",
                "state_seq": "",
                "status": "current",
                "suggestion": "",
                "suggestions": "",
                "unlabeled_candidate": "",
            },
        )

        client = Client(URL_ROOT)
        client.delete_hinter(83)

        self.assertTrue(
            responses.assert_call_count(urljoin(URL_ROOT, "api"), 2)
        )

    @responses.activate
    def test_query(self):
        """Test query execution results."""
        responses.add(
            "POST",
            urljoin(URL_ROOT, "api"),
            json={
                "project_id": "abc123",
                "title": "my new project",
                "datasets": ["12"],
                "auto_complete": "",
                "cand_seq_full": "",
                "cand_seq_prefix": "",
                "candidates": [],
                "classes": {
                    "my-class": {
                        "base_rate_given": 10,
                        "base_rate_pdf": [1, 0, 0, 0],
                        "class_type": "ftc",
                        "confidences": [[0, "BaseRate"]],
                        "description": {
                            "error_rate": "Error rate computed over all plabels",
                            "precision": (
                                "Precision computed over hand_labels.\n"
                                "Sum of plables for positively hand labeled examples."
                            ),
                            "recall": (
                                "Recall computed over hand labels.\n"
                                "Average plabel of the positively hand labeled "
                                "examples."
                            ),
                        },
                        "error_rate": [[0, "BaseRate"]],
                        "hand_label_distribution_counts_negative": [0, 0, 0],
                        "hand_label_distribution_counts_positive": [0, 0, 0],
                        "label_distribution": [0, 0, 0],
                        "label_distribution_counts": [0, 0, 0],
                        "precision": [[0, "BaseRate"]],
                        "recall": [[0, "BaseRate"]],
                        "thresholds": [50, 50],
                    }
                },
                "column_flags": {"enrichable": [True, False, False]},
                "disagreements": "",
                "enrichment_tasks": "",
                "error_msg": None,
                "error_verb": None,
                "export_preview": None,
                "exports": [],
                "field_names": [],
                "hand_labels": [],
                "hinters": [],
                "is_shared": False,
                "messages": [],
                "n_candidates": "",
                "n_handlabels": "",
                "ner_hl_text": "",
                "notifications": "",
                "precision_candidate": "",
                "project_config": "",
                "published_title": "",
                "pull_actions": "",
                "push_actions": "",
                "query": "",
                "query_breakdown": "",
                "query_completed": True,
                "query_end": "",
                "query_examined": "",
                "query_full_rows": "",
                "query_history": "",
                "query_hit_count": "",
                "query_page": "",
                "selected_class": "",
                "selections": "",
                "show_notification_badge": "",
                "state_seq": "",
                "status": "current",
                "suggestion": "",
                "suggestions": "",
                "unlabeled_candidate": "",
            },
        )

        client = Client(URL_ROOT)
        client.query("/myQuery/", 1)

        self.assertTrue(
            responses.assert_call_count(urljoin(URL_ROOT, "api"), 2)
        )
