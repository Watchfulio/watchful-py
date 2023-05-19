from __future__ import annotations

from dataclasses import dataclass, field
import os
import typing
import time
from urllib.parse import urljoin
import uuid

import requests

from watchful.__about__ import __version__


@dataclass(frozen=True, kw_only=True)
class Summary:
    """A summary of the current state of Watchful.

    Watchful is a stateful service, and as such, has an open project,
    with its values and settings, as well as application related
    values as well. The Summary object represents that state.
    """

    project_id: str
    title: str
    datasets: typing.List[str]
    watchful_home: str = field(default=os.path.expanduser("~/watchful"))

    auto_complete: typing.Any
    cand_seq_full: int
    cand_seq_prefix: int
    candidates: typing.List[typing.Any]
    classes: typing.Dict[str, str]
    column_flags: typing.Dict[str, str]
    disagreements: typing.List[typing.Any]
    enrichment_tasks: typing.List[typing.Any]
    error_msg: typing.Optional[str]
    error_verb: typing.Optional[str]
    export_preview: typing.Optional[typing.Any]
    exports: typing.List[typing.Any]
    field_names: typing.List[typing.Any]
    hand_labels: typing.Optional[typing.Any]
    hinters: typing.Optional[typing.Any]
    is_shared: bool
    messages: typing.List[typing.Any]
    n_candidates: int
    n_handlabels: int
    ner_hl_text: typing.Optional[typing.Any]
    notifications: typing.List[typing.Any]
    precision_candidate: typing.Dict[str, typing.Any]
    project_config: typing.Dict[str, typing.Any]
    published_title: typing.Optional[typing.Any]
    pull_actions: typing.List[typing.Any]
    push_actions: typing.List[typing.Any]
    query: str
    query_breakdown: typing.Dict[str, typing.Any]
    query_completed: bool
    query_end: bool
    query_examined: int
    query_full_rows: bool
    query_history: typing.Dict[str, typing.Any]
    query_hit_count: int
    query_page: int
    selected_class: str
    selections: typing.List[typing.Any]
    show_notification_badge: bool
    state_seq: int
    status: str
    suggestion: typing.Optional[typing.Any]
    suggestions: typing.Dict[str, typing.Any]
    unlabeled_candidate: typing.List[typing.Any]

    @property
    def datasets_dir(self) -> str:
        return os.path.join(self.watchful_home, "datasets")

    @property
    def dataset_filepath(self) -> str:
        return os.path.join(self.datasets_dir, "refs", self.datasets[0])


class Client:
    """A Watchful API client.

    Many of these methods are equivalent to RPC calls and, as such,
    do not "return" a result. In those cases, a follow up call to
    `get_summary` will should the resulting change.
    """

    _root_url: str
    _session: requests.Session

    timeout = 10

    def __init__(self, url: str) -> None:
        self._root_url = url

        self._session = requests.Session()
        self._session.headers["x-watchful-sdk"] = __version__
        self._session.headers["content-type"] = "application/json"

    def wait_for_ready(self, timeout: int = 10) -> None:  # pragma: no cover
        """Wait for the Watchful service to be ready."""
        end = time.time_ns() + (timeout * 1_000_000_000)
        while time.time_ns() < end:
            try:
                self._session.get(self._root_url)
                return
            except requests.exceptions.ConnectionError:
                time.sleep(0.1)

        raise TimeoutError("Timed out waiting for Watchful to start")

    def list_projects(self) -> typing.List[str]:
        """List all projects."""
        response = self._session.get(
            urljoin(self._root_url, "projects"), timeout=self.timeout
        )
        return response.json()

    def open_project(self, title: str) -> Summary:
        """Open a project."""
        _response = self._session.post(
            urljoin(self._root_url, "projects"),
            json=title,
            timeout=self.timeout,
        )

        return self.get_summary()

    def create_project(
        self,
        title: str,
        data: bytes,
        columns: typing.List[str],
        has_header: bool = True,
    ) -> Summary:
        """Create a project.

        The client differs slightly from the API, as the API
        allows a project to be created without setting a title. However,
        the client does not provide a method for doing the same. It requires
        a title to be specified on creation.

        Additionally, the API will allow users to create projects that
        don't have datasets attached to them. This interface requires a
        dataset when creating the project.
        """
        self.open_project("new")

        self._session.post(
            urljoin(self._root_url, "api"),
            json={"verb": "title", "title": title},
            timeout=self.timeout,
        )

        dataset_id = self.create_dataset(data, columns)

        # It's _possible_ this loop isn't needed here (and probably shouldn't
        # be, regardless). It's probably a standard practice to get the summary
        # and check for validity with the recent changes, for now.
        end = time.time_ns() + (self.timeout * 1_000_000_000)
        while time.time_ns() < end:
            summary = self.get_summary()
            if summary.title == title and summary.datasets == [dataset_id]:
                return summary
            time.sleep(0.1)
        raise TimeoutError(
            "Timeout waiting for project to be completed. "
            "The state of the project is unknown."
        )

    def create_dataset(
        self, data: bytes, columns: typing.List[str], has_header: bool = True
    ) -> str:
        """Create a dataset from a CSV file."""
        # Yes, that's right. We create our own id...
        dataset_uuid = uuid.uuid4()
        self._session.post(
            urljoin(self._root_url, f"api/_stream/{dataset_uuid}/0/true"),
            data=data,
            headers={"content-type": "text/csv"},
        )
        response = self._session.post(
            urljoin(self._root_url, f"/api/_stream/{dataset_uuid}"),
            json={"filename": f"{dataset_uuid}.csv", "has_header": has_header},
        )
        dataset_id = response.json()["id"]

        self._session.post(
            urljoin(self._root_url, "/api"),
            json={"verb": "dataset_add", "id": dataset_id, "columns": columns},
        )
        return dataset_id

    # TODO: should there be a way to see the columns easily?
    def set_columns(self, columns: typing.List[bool]):
        """Set the columns to infer hints."""

    def create_class(
        self, classification: str, class_type: str = "ftc"
    ) -> None:
        """Add a classification."""

    def delete_class(
        self,
        classification: str,
    ) -> None:
        """Delete a classifier."""

    def query(self, query: str, page: int = 0) -> None:
        """Execute a query."""

    # TODO: do we need to query_all?

    def set_base_rate(self, classification: str, rate: int) -> None:
        """Set the base rate for a classification."""

    def create_hinter(
        self, classification: str, query: str, weight: int
    ) -> None:
        """Create a hinter."""

    def create_external_hinter(self, classification: str, weight: int) -> None:
        """Create an external hinter."""

    def delete_hinter(self, hinter_id: int) -> None:
        """Delete a hinter."""

    def upload_attributes(
        self, dataset_id: str, atributes_filepath: str
    ) -> None:
        """Upload attributes from an external source."""
        # TODO: warn that this is deprecated.

    def dump(self) -> None:
        """Return all candidates in "hint API order"."""

    # TODO: this should be a file-like object
    def export_dataset(self, classification: str) -> None:
        """Export the dataset as a stream."""

    def export_project(self) -> None:
        """Export the project."""

    def label_row(self, row: typing.List[str]) -> typing.List[str]:
        """Label a candidate row."""
        return []

    def set_config(self, key: str, value: str) -> None:
        """Set a config value."""

    # TODO: config should be a typed dataclass
    def get_config(self) -> typing.Dict:
        """Get the config."""
        return {}

    def set_hub_url(self, url) -> None:
        """Set the hub url."""

    def login(self, email: str, password: str) -> None:
        """Log in to hub."""

    def publish(self, token: str) -> None:
        """Publish to hub."""

    def fetch(self, token: str) -> None:
        """Fetch data from hub."""

    def pull(self, token: str) -> None:
        """Pull data from hub."""

    def push(self, token: str) -> None:
        """Push data to hub."""

    def peek(self, token: str) -> None:
        """Peek at data in hub."""

    def whoami(self, token: str) -> None:
        """Get login info from hub."""

    def get_summary(self) -> Summary:
        """Get the Watchful summary."""
        response = self._session.post(
            urljoin(self._root_url, "api"),
            json={"verb": "nop"},
            timeout=self.timeout,
        )
        return Summary(**response.json())
