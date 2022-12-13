"""
This script provides the functions required for interacting directly with
Watchful client application.
"""
################################################################################


import base64
import csv
import http.client
import io
import json
import os
import socket
import subprocess
import sys
import time
import urllib
from typing import Callable, Dict, Generator, List, Optional, Union
from uuid import uuid4


HOST = "localhost"
PORT = "9002"


def _refresh() -> None:
    """
    This function explicitly refreshes the imported ``watchful`` package.
    """

    del sys.modules["watchful"]
    import watchful

    print(f"watchful version: {watchful.__version__}")


def _get_conn() -> http.client.HTTPConnection:
    """
    This function creates the HTTP connection from the global
    ``HOST`` and ``PORT``.

    :return: A ``http.client.HTTPConnection`` to ``HOST`` and ``PORT``.
    :rtype: http.client.HTTPConnection
    """

    return http.client.HTTPConnection(HOST + ":" + PORT)


def await_port_opening(port: int, timeout_sec: int = 10) -> None:
    """
    This function waits for the port to be open; it returns None if ``port`` was
    opened within the timeout, otherwise it raises an exception. It is used for
    awaiting Watchful process startup.

    :param port: The port.
    :type port: int
    :param timeout_sec: The timeout in seconds, defaults to 10.
    :type timeout_sec: int, optional
    """

    end = time.time_ns() + (timeout_sec * 1_000_000_000)
    while time.time_ns() < end:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout_sec)
        res = sock.connect_ex((HOST, port))
        if res == 0:
            return None
        time.sleep(0.001)

    raise Exception("Timed out waiting for Watchful to start")


def spawn_cmd(cmd: str, env: str = None) -> int:
    """
    This function spawns a command and returns the PID of the spawned process.

    :param cmd: The command.
    :type cmd: str
    :param env: The environment, defaults to None.
    :type env: str, optional
    :return: The PID of the spawned process.
    :rtype: int
    """

    with subprocess.Popen(
        cmd + " &\n echo $!", shell=True, stdout=subprocess.PIPE, env=env
    ) as proc:
        out, _ = proc.communicate()
    pid = int(out.decode("utf-8")[:-1])

    return pid


def await_summary(
    pred: Callable,
    halt_fn: Callable = lambda x: False,
    unchanged_timeout: int = 60,
) -> Optional[Dict]:
    """
    This function returns the summary once ``pred(summary)`` returns true, or
    stops waiting once ``halt_fn`` returns true and then returns None, or raises
    an exception if the summary is unchanged for ``unchanged_timeout`` seconds.

    :param pred: The predicate function.
    :type pred: Callable
    :param halt_fn: The halt function, defaults to lambda x: False.
    :type halt_fn: Callable, optional
    :param unchanged_timeout: The timeout in seconds, defaults to 60.
    :type unchanged_timeout: int, optional
    :return: The dictionary of the HTTP response from :func:`get()`.
    :rtype: Dict, optional
    """

    prev_summary = None
    end = float("inf")
    while time.time_ns() < end:
        summary = get()
        if halt_fn(summary):
            return None
        if pred(summary):
            return summary
        if prev_summary == summary:
            end = time.time_ns() + (unchanged_timeout * 1_000_000_000)
        prev_summary = summary
        time.sleep(0.03)  # 30ms sleep for ~30 requests per second

    raise Exception(
        "Timed out awaiting summary. Summary went stale for "
        + str(unchanged_timeout)
        + " seconds"
    )


def _assert_success(summary: Dict) -> Optional[Dict]:
    """
    This function raises an exception if summary contains "error_msg", otherwise
    it returns the summary.

    :param summary: The dictionary of the HTTP response from a connection
        request.
    :type summary: Dict
    :return: The summary.
    :rtype: Dict, optional
    """

    if "error_msg" in summary and summary["error_msg"]:
        verb_str = ""
        if "error_verb" in summary and summary["error_verb"]:
            verb_str = f' ({summary["error_verb"]})'
        raise Exception(f'Summary error{verb_str}: {summary["error_msg"]}')

    return summary


# as used by register_summary_string_hook
API_SUMMARY_HOOK_CALLBACK = None


def register_summary_hook(function: Callable) -> None:
    """
    This function allows you to provide a function that will be called with
    every summary object that is returned from any API call to the /api
    endpoint, that being the raw response body before JSON parsing. This can be
    used, for example, to instrument a test suite with a function that writes
    every summary object to disk and then creating a dataset of Watchful summary
    objects for further analysis. Most SDK users probably won't be reaching for
    this function every day, but if you find a clever use for it, let us know!

    :param function: Your function to be called with every summary string
    :type function: Callable
    """
    global API_SUMMARY_HOOK_CALLBACK
    API_SUMMARY_HOOK_CALLBACK = function


def _read_response(
    resp: http.client.HTTPResponse, resp_is_summary: bool = False
) -> Optional[Dict]:
    """
    This function raises an exception if ``resp.status`` is not 200, otherwise
    it returns ``ret``.

    If ``resp_is_summary`` then we will also run the
    ``API_SUMMARY_HOOK_CALLBACK`` (any hook that was provided), so this is only
    appropriate for endpoints that return a summary, that is, the /api endpoint
    rather than some other JSON object like /config, /remote, etc.

    :param resp: The HTTP response from a connection request.
    :type resp: http.client.HTTPResponse
    :param resp_is_summary: Boolean indicating if the response is known to be a
        summary object.
    :type resp_is_summary: bool, optional
    :return: The dictionary of ``resp``.
    :rtype: Dict, optional
    """
    # the assertion is here because that's what our API endpoints always return
    assert 200 == int(resp.status)
    json_str = resp.read()

    if resp_is_summary and API_SUMMARY_HOOK_CALLBACK:
        API_SUMMARY_HOOK_CALLBACK(json_str)

    ret = json.loads(json_str)

    # One idea:
    # if ret["error_msg"]:
    #     raise Exception(ret["error_msg"])
    if "error_msg" in ret and ret["error_msg"]:
        print(ret["error_msg"])

    return ret


def api(verb: str, **args: Dict) -> Optional[Dict]:
    """
    This is a convenience function for API calls; made up of a verb and optional
    keyword arguments.

    :param verb: The verb for the API.
    :type verb: str
    :param args: Optional parameters to support the API for ``verb``.
    :type args: Dict
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    action = args  # already a dictionary
    action["verb"] = verb

    return api_send_action(action)


def api_send_action(action: Dict) -> Optional[Dict]:
    """
    This is a convenience function for API calls with an action.

    :param action: The ``verb`` for the API with optional parameters.
    :type action: Dict
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    conn = _get_conn()
    conn.request(
        "POST", "/api", json.dumps(action), {"Content-Type": "application/json"}
    )
    return _read_response(conn.getresponse(), resp_is_summary=True)


def ephemeral(port: str = "9002") -> None:
    """
    This function starts the backend using the specified ``port`` for an
    interactive session without persistence.

    :param port: The port, defaults to "9002".
    :type port: str, optional
    """

    _ = spawn_cmd(
        f"watchful -p {port} --no-persistence "
        f">watchful_ephemeral_output.txt 2>&1"
    )
    await_port_opening(int(port))
    external(port=port)


def external(host: str = "localhost", port: str = "9001") -> None:
    """
    This function changes the global ``HOST`` abd ``PORT`` values.

    :param host: The host, defaults to "localhost".
    :type host: str, optional
    :param port: The port, defaults to "9001".
    :type port: str, optional
    """

    global HOST, PORT
    HOST = host
    PORT = port


def list_projects() -> Dict:
    """
    This function lists the available projects.

    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict
    """

    c = _get_conn()
    c.request("GET", "/projects")
    r = c.getresponse()

    return json.loads(r.read())


def open_project(id_: str) -> str:
    """
    This function opens a project via its project id, which is the path to its
    hints file.

    :param id_: The project id.
    :type id_: str
    :return: The read HTTP response.
    :rtype: str
    """

    c = _get_conn()
    c.request(
        "POST",
        "/projects",
        json.dumps(id_),
        {"Content-Type": "application/json"},
    )
    r = c.getresponse()

    ret = r.read().decode("utf-8")
    await_plabels()

    return ret


def create_project(title_: Optional[str] = None) -> Union[str, Optional[Dict]]:
    """
    This function creates a new project. Additionally, if title is supplied, a
    title is given to the newly created project.

    :param title: The title for the project.
    :type title: str, optional
    :return: If a title is supplied and ``open_project("new")`` is successful,
        the dictionary of the HTTP response from the connection request from
        ``title(title)``; otherwise the read HTTP response from
        ``open_project("new")``.
    :rtype: Union[str, Optional[Dict]]
    """

    ret = open_project("new")

    if title_ is not None and ret == '"OK"\n':
        return title(title_)
    return ret


def title(title_: str) -> Optional[Dict]:
    """
    This function gives a title to a newly created project.

    :param title_: The title for the project.
    :type title_: str
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return api("title", title=title_)


def get_project_id(summary: Dict) -> str:
    """
    This function gets the active project id from ``summary``. For correctness,
    we use the ``summary`` that has been success asserted via
    ``_assert_success``.

    :param summary: The dictionary of the HTTP response from a connection
        request.
    :type summary: Dict
    :return: The project id.
    :rtype: str
    """

    if "project_id" in summary:
        return summary["project_id"]

    raise Exception("No project is currently active.")


def get_dataset_id(summary: Dict) -> str:
    """
    This function gets the active dataset id from ``summary``. For correctness,
    we use the ``summary`` that has been success asserted via
    ``_assert_success``.

    :param summary: The dictionary of the HTTP response from a connection
        request.
    :type summary: Dict
    :return: The dataset id.
    :rtype: str
    """

    if "datasets" in summary:
        # ``dataset_ids`` should either be empty or contain one dataset id.
        dataset_ids = summary["datasets"]
        if len(dataset_ids) == 0:
            raise Exception("No dataset is currently opened.")
        dataset_id = dataset_ids[0]
        return dataset_id

    raise Exception("`datasets` is currently not available.")


def get_watchful_home(summary: Dict, is_local: bool = True) -> str:
    """
    This function gets Watchful home from ``summary``. For correctness, we use
    the summary that has been success asserted via ``_assert_success``. If
    Watchful home is not available and the Watchful application is local, we
    derive Watchful home from the user home.

    :param summary: The dictionary of the HTTP response from a connection
        request.
    :type summary: Dict
    :param is_local: The boolean indicating if the Watchful application is
        local, defaults to True
    :type is_local: bool, optional
    :return: Watchful home.
    :rtype: str
    """

    if "watchful_home" in summary:
        return summary["watchful_home"]
    if is_local:
        user_home = os.path.expanduser("~")
        watchful_home = os.path.join(user_home, "watchful")
        return watchful_home

    raise Exception("`watchful_home` is currently not available.")


def get_datasets_dir(summary: Dict, is_local: bool = True) -> str:
    """
    This function infers the datasets directory from ``summary``. For
    correctness, we use the summary that has been success asserted via
    ``_assert_success``.

    :param summary: The dictionary of the HTTP response from a connection
        request.
    :type summary: Dict
    :param is_local: The boolean indicating if the Watchful application is
        local, defaults to True
    :type is_local: bool, optional
    :return: The datasets directory.
    :rtype: str
    """

    watchful_home = get_watchful_home(summary, is_local)
    datasets_dir = os.path.join(watchful_home, "datasets")
    return datasets_dir


def get_dataset_filepath(summary: Dict, is_local: bool = True) -> str:
    """
    This function infers the datasets filepath from ``summary``. For
    correctness, we use the summary that has been success asserted via
    ``_assert_success``. As this function uses file operations, it does not work
    when the Watchful application is remote, and in such case returns "".

    :param summary: The dictionary of the HTTP response from a connection
        request.
    :type summary: Dict
    :param is_local: The boolean indicating if the Watchful application is
        local, defaults to True
    :type is_local: bool, optional
    :return: The dataset filepath.
    :rtype: str
    """

    if not is_local:
        return ""

    datasets_dir = get_datasets_dir(summary, is_local)
    dataset_id = get_dataset_id(summary)
    dataset_ref_path = os.path.join(datasets_dir, "refs", dataset_id)

    # Check that ``dataset_ref_path`` exists.
    if not os.path.isfile(dataset_ref_path):
        raise Exception(f"File {dataset_ref_path} does not exist.")
    with open(dataset_ref_path, encoding="utf-8") as f:
        dataset_ref = f.readline()
    dataset_filepath = os.path.join(datasets_dir, "raw", dataset_ref)

    # Check that ``dataset_filepath`` exists.
    if not os.path.isfile(dataset_filepath):
        raise Exception(f"File {dataset_filepath} does not exist.")

    return dataset_filepath


# def poll():
#     pass


def records(csv_: str) -> Optional[Dict]:
    """
    This function loads the csv dataset.

    :param csv_: The csv dataset.
    :type csv_: str
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return api("records", data=csv_, content_type="text/csv")


def col_flag(flag: str, columns: List[bool]) -> Optional[Dict]:
    """
    This function sets a flag for all the columns in the list.

    :param flag: "inferenceable" is currently the only supported flag.
    :type flag: str
    :param columns: A List of true/false values, specifying whether the flag
        should be set for the column.
    :type columns: List
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """
    return api("col_flag", flag=flag, columns=columns)


def class_(class__: str) -> Optional[Dict]:
    """
    This function creates a class.

    :param class__: The class.
    :type class__: str
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return api("class", name=class__)


def create_class(class__: str, class_type: str = "ftc") -> Optional[Dict]:
    """
    This function creates a class.

    :param class__: The class.
    :type class__: str
    :param class_type: The class type, it can be either "ftc" or "ner", defaults
        to "ftc".
    :type class_type: str, optional
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return api("class", name=class__, class_type=class_type)


def query_async(q: str, page: int = 0) -> Optional[Dict]:
    """
    This function queries for a page. As it is asynchronous, the immediate HTTP
    response is likely not updated yet.

    :param q: The query.
    :type q: str
    :param page: The page, defaults to 0.
    :type page: int, optional
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return api("query", query=q, page=page)


def query(q: str, page: int = 0) -> Optional[Dict]:
    """
    This function queries for a page and returns an updated HTTP response.

    :param q: The query.
    :type q: str
    :param page: The page, defaults to 0.
    :type page: int, optional
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    query_async(q, page)

    return await_summary(
        lambda s: s["query_completed"], lambda s: s["query"] != q
    )


def query_all(q: str, max_pages: int = 0) -> Generator[List[str], None, None]:
    """
    This function evaluates the query returning the results as opposed to the
    summary. By default, it returns all results for the query (all pages). This
    can be limited by setting max_pages to the positive number of pages you
    want. Each query result is a vector with a string for each field that is
    returned. Note that TOKS, SENTS, CELLS queries only return one field and
    each result will be wrapped in a vector of one string.

    :param q: The query.
    :type q: str
    :param max_pages: The maximum page, defaults to 0.
    :type max_pages: int, optional
    :return: The fields.
    :rtype: Generator[List[str], None, None]
    """

    page = 0
    while True:
        summary = query(q, page)
        for fields in [cand["fields"] for cand in summary["candidates"]]:
            yield fields
        page += 1
        if summary["query_end"] or max_pages and page == max_pages:
            break


def base_rate(class__: str, rate: int) -> Optional[Dict]:
    """
    This function sets the base rate for a class.

    :param class__: The class to set a base rate for.
    :type class__: str
    :param rate: The base rate for the class.
    :type rate: int
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return api("base_rate", label=class__, rate=rate)


def await_plabels() -> Optional[Dict]:
    """
    This function gets the updated HTTP response.

    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return await_summary(lambda s: s["status"] == "current")


def hinter_async(class__: str, query_: str, weight: int) -> Optional[Dict]:
    """
    This function creates a hinter. As it is asynchronous, the immediate HTTP
    response is likely not updated yet.

    :param class__: The class for the hinter.
    :type class__: str
    :param query_: The query for the hinter.
    :type query_: str
    :param weight: The weight for the hinter.
    :type weight: int
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return api("hinter", label=class__, weight=weight, query=query_)


def hinter(class__: str, query_: str, weight: int) -> Optional[Dict]:
    """
    This function creates a hinter and returns an updated HTTP response.

    :param class__: The class for the hinter.
    :type class__: str
    :param query_: The query for the hinter.
    :type query_: str
    :param weight: The weight for the hinter.
    :type weight: int
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    _assert_success(hinter_async(class__, query_, weight))
    return await_plabels()


def delete(id_: int) -> Optional[Dict]:
    """
    This function deletes a hinter.

    :param id_: The hinter id to delete.
    :type id_: int
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return api("delete", id=id_)


def delete_class(class__: str) -> Optional[Dict]:
    """
    This function deletes a class.

    :param class__: The class to delete.
    :type class__: str
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return api("delete", class_name=class__)


def get() -> Optional[Dict]:
    """
    This function gets the current status of the Watchful application,
    containing information such as your currently active project, dataset
    examples (candidates) and classes, hinters created, hand labels and label
    distribution, confidences and error rate, recall and precision and many
    more.

    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return api("nop")


def external_hinter(class__: str, name: str, weight: int) -> Optional[Dict]:
    """
    This function creates an external hinter.

    :param class__: The class for the hinter.
    :type class__: str
    :param name: The name for the hinter.
    :type name: str
    :param weight: The weight for the hinter.
    :type weight: int
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    _assert_success(
        api(
            "hinter",
            query="[external]",
            name=name,
            label=class__,
            weight=weight,
        )
    )

    return await_plabels()


def upload_attributes(
    dataset_id: str,
    attributes_filepath: str,
) -> Optional[Dict]:
    """
    This function uploads the attributes for the ``dataset_id`` to the remote
    Watchful application, where the Watchful application then saves it to a
    filepath according to its stable application logic.

    :param dataset_id: The dataset id.
    :type dataset_id: str
    :param attributes_filepath: The attributes filepath.
    :type attributes_filepath: str
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """
    conn = _get_conn()
    with open(attributes_filepath, encoding="utf-8") as attributes_file:
        conn.request(
            "PUT",
            f"/datasets/{dataset_id}/attributes",
            attributes_file,
            {"Content-Type": "text/plain"},
        )
    resp = conn.getresponse()
    assert resp.status == 200, f"not OK HTTP status. Was: {resp.status}"
    return _assert_success(_read_response(resp))


def load_attributes(
    dataset_id: str,
    attributes_filename: str,
) -> Optional[Dict]:
    """
    This function is used in the case of Watchful application being on the same
    machine as the data enrichment.

    :param dataset_id: The dataset id.
    :type dataset_id: str
    :param attributes_filename: The attributes filename.
    :type attributes_filename: str
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return api("attributes", id=dataset_id, file=attributes_filename)


def _dump(offset: int) -> Optional[Dict]:
    """
    This function returns a chunk of candidates in "hint API order".

    :param offset: The offset chunk.
    :type offset: int
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return api("dump", offset=offset)


def dump() -> Generator[List[str], None, None]:
    """
    This function returns all the candidates in "hint API order".

    :return: The generator of all the candidates.
    :rtype: Generator[List[str], None, None]
    """

    n_cands = get()["n_candidates"]
    offset = 0
    chunk = []
    while offset < n_cands:
        summary = _dump(offset)
        # TODO: Add error handling.
        chunk = summary["candidates"]
        offset += len(chunk)
        i = 0
        while i < len(chunk):
            yield chunk[i]
            i += 1


def dump_dicts() -> Generator[Dict[str, str], None, None]:
    """
    This function returns all the candidates in "hint API order", together with
    the column names for all values.

    :return: The generator of all the candidates, each as a dictionary of named
        values.
    :rtype: Generator[Dict[str, str], None, None]
    """

    field_names = get()["field_names"]
    for c in dump():
        yield dict(zip(field_names, c))


def hint(name: str, offset: int, values: List[bool]) -> Optional[Dict]:
    """
    This function adds the hints for an external hinter.

    :param name: The hinter name.
    :type name: str
    :param offset: The offset.
    :type offset: int
    :param values: The hints.
    :type values: List[bool]
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional

    TODO: Come up with a better streaming Python API here.
    """

    values = list(map(lambda x: x and 1 or 0, values))

    return _assert_success(api("hint", name=name, offset=offset, values=values))


def apply_hints(name: str) -> Optional[Dict]:
    """
    This function applies the hints for an external hinter.

    :param name: The hinter name.
    :type name: str
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    _assert_success(api("apply_hints", name=name))

    return await_plabels()


def hint_all(name: str, values: List[bool]) -> Optional[Dict]:
    """
    This function applies the hints for an external hinter.

    :param name: The hinter name.
    :type name: str
    :param values: The hints.
    :type values: List[bool]
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    hint(name, 0, values)

    return apply_hints(name)


def export_stream(
    content_type: str = "text/csv",
    mode: str = "ftc",
) -> http.client.HTTPResponse:
    """
    This function begins the export using the export_stream call. The result is
    not JSON, but is data to be processed directly.

    For FTC mode, content_type must be text/csv and mode must be ftc.
    For NER mode, content_type must be application/jsonlines and mode
    must be ner.

    On success, it returns the http.client.HTTPResponse object from which you
    can read the data.

    :param content_type: The content type of the export, defaults to "text/csv".
    :type content_type: str, optional
    :param mode: The mode of the export, it can be either "ftc" or "ner",
        defaults to "ftc".
    :type mode: str, optional
    :return: The HTTP response from the connection request.
    :rtype: http.client.HTTPResponse
    """

    conn = _get_conn()
    conn.request(
        "GET",
        "/export_stream?content-type="
        + urllib.parse.quote_plus(content_type)
        + "&mode="
        + urllib.parse.quote_plus(mode),
    )
    resp = conn.getresponse()
    assert 200 == int(resp.status), f"Request failed with status {resp.status}."
    return resp


def export_dataset_to_path(out_file: str, fields: List[str] = None) -> None:
    """
    This function exports the original dataset via a buffered stream to the
    specified output file path. It takes ``fields`` as an optional argument for
    the header (column names), for the case where the callee expects to use
    specific columns; otherwise it uses the column names returned by the
    Watchful application. An exception is raised when the dataset's column names
    do not match the user's expected column names.

    :param out_file: The file path to export the original dataset to.
    :type out_file: str
    :param fields: The list of column names to use for the dataset export.
    :type fields: List, optional
    """

    if not fields:
        fields = get()["field_names"]
    n_cols = len(fields)

    dataset_export_stream = export_stream()
    with open(out_file, "w", encoding="utf-8") as f:
        writer = csv.writer(f)
        reader = csv.reader(
            io.TextIOWrapper(
                dataset_export_stream, encoding="utf-8", newline=""
            )
        )
        header = next(reader)
        if header[:n_cols] != fields:
            raise Exception(
                f"Dataset's column names {header} did not match the expected "
                f"column names {fields}."
            )
        writer.writerow(fields)
        for row in reader:
            writer.writerow(row[:n_cols])


def export_async() -> Optional[Dict]:
    """
    This function exports the dataset. As it is asynchronous, the immediate HTTP
    response is likely not updated yet.

    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return api("export", query="", content_type="text/csv")


def export() -> Optional[Dict]:
    """
    This function exports the dataset and returns an updated HTTP response.

    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    n_exports = len(get()["exports"])
    export_async()

    return await_summary(lambda s: len(s["exports"]) == n_exports + 1)


def export_preview(mode: str = "ftc") -> Optional[Dict]:
    """
    Returns a preview of the export.

    :param mode: The mode of the export preview, it can be either "ftc" or
        "ner", defaults to "ftc".
    :type mode: str, optional
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return api("export_preview", mode=mode)


def create_dataset(
    csv_bytes: bytes,
    columns: List[str],
    filename: str = "none",
    has_header: bool = True,
) -> str:
    """
    This function loads the specified columns of a csv dataset and returns the
    dataset id.

    :param csv_bytes: The csv dataset bytes.
    :type csv_bytes: bytes
    :param columns: The list of column names to use.
    :type columns: List[str]
    :param filename: The csv dataset filename, defaults to "none".
    :type filename: str, optional
    :param has_header: The boolean indicating if the csv dataset has a header,
        defaults to True.
    :type has_header: bool, optional
    :return: The dataset id.
    :rtype: str

    TODO: Add error handling.
    """

    id_ = str(uuid4())
    conn = _get_conn()
    conn.request(
        "POST",
        "/api/_stream/" + id_ + "/0/true",
        csv_bytes,
        {"Content-Type": "text/csv"},
    )
    _ = _read_response(conn.getresponse())

    params = json.dumps({"filename": filename, "has_header": has_header})
    conn.request(
        "POST",
        "/api/_stream/" + id_,
        params,
        {"Content-Type": "application/json"},
    )
    dataset_id = _read_response(conn.getresponse())["id"]

    api("dataset_add", id=dataset_id, columns=columns)

    return dataset_id


def label_single(row: List[str]) -> List[str]:
    """
    This function labels a candidate row.

    :param row: The candidate row.
    :type row: List[str]
    :return: The plabels for the candidate row.
    :rtype: List[str]
    """

    c = _get_conn()
    sio = io.StringIO()
    w = csv.writer(sio)
    w.writerow(row)
    csv_row = sio.getvalue().encode("utf-8")
    sio.close()
    c.request("POST", "/label", csv_row, {"Content-Type": "text/csv"})
    r = c.getresponse()
    resp_body = r.read().decode("utf-8")
    csv_str = io.StringIO(resp_body)
    rdr = csv.reader(csv_str)
    rdr_list = list(rdr)
    if len(rdr_list) == 2:
        fields = get()["field_names"]
        assert (
            fields == rdr_list[0][: len(fields)]
        ), "server prepended the header to the labeled row"
    else:
        assert len(rdr_list) == 1, "server returned a single row"
    return rdr_list[-1]


def config_set(key: str, value: str) -> Optional[Dict]:
    """
    This function sets one app instance configuration parameter using a key and
    value pair.

    :param key: The parameter name.
    :type key: str
    :param value: The parameter value.
    :type value: str
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    conn = _get_conn()
    params = json.dumps({"verb": "set", "key": key, "value": value})
    conn.request(
        "POST", "/config", params, {"Content-Type": "application/json"}
    )

    return _read_response(conn.getresponse())


def config() -> Optional[Dict]:
    """
    This function retrieves the app instance configuration parameters
    ``remote``, ``username``, ``role`` and ``authorization`` and their values.

    :return: A dictionary of key value pairs
    :rtype: Dict, optional
    """

    conn = _get_conn()
    conn.request("GET", "/config", None, {"Content-Type": "application/json"})

    return _read_response(conn.getresponse())


def set_hub_url(url: str) -> Optional[Dict]:
    """
    This function sets the Watchful hub URL of the Watchful client. The Watchful
    hub URL should not change after data has been fetched or published to a hub.

    :param url: The Watchful hub URL.
    :type url: str
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """
    conn = _get_conn()
    conn.request("POST", "/set_hub_url", url, {"Content-Type": "text/plain"})
    return _read_response(conn.getresponse())


def print_candidates(summary: Optional[Dict] = None) -> None:
    """
    This function retrieves and prints the column names and all the candidates.

    :param summary: The dictionary of the HTTP response from a connection
        request, defaults to None.
    :type summary: Dict, optional
    """

    if summary is None:
        summary = get()
    print(
        "\n".join(
            [",".join(summary["field_names"])]
            + list(map(lambda c: ",".join(c["fields"]), summary["candidates"]))
        )
    )


def candidate_dicts(summary: Optional[Dict] = None) -> List[Dict[str, str]]:
    """
    This function retrieves and returns all the candidates, together with the
    column names for all values.

    :param summary: The dictionary of the HTTP response from a connection
        request, defaults to None.
    :type summary: Dict, optional
    :return: The list of all the candidates, each as a dictionary of named
        values.
    :rtype: List[Dict[str, str]]
    """

    if summary is None:
        summary = get()
    return list(
        map(
            lambda c: dict(zip(summary["field_names"], c["fields"])),
            summary["candidates"],
        )
    )


def exit_backend() -> None:
    """
    This function exits the backend. Note that the API call will usually fail
    because the backend exits before returning a HTTP response so we suppress
    the error. This is an old API function wrapper and will not be applicable to
    the dockerized Watchful application instances.
    """

    try:
        api("exit")
    # We allowing passing this error as it is known.
    except http.client.RemoteDisconnected:
        pass


## Hub API


def hub_api(verb: str, token: str, **args: Dict) -> Optional[Dict]:
    """
    This is a convenience function for collaboration API calls with Watchful;
    made up of a verb, a token and optional keyword arguments.

    :param verb: The verb for the hub API.
    :type verb: str
    :param verb: The user's auth token.
    :type token: str
    :param args: Optional parameters to support the hub API for ``verb``.
    :type args: Dict
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    headers = {"Content-Type": "application/json"}
    headers.update({"Authorization": "Bearer " + token})
    action = args  # already a dictionary
    action["verb"] = verb
    conn = _get_conn()
    conn.request("POST", "/remote", json.dumps(action), headers)
    return _assert_success(_read_response(conn.getresponse()))


def login(email: str, password: str) -> Optional[Dict]:
    """
    This function performs login with the email and password with Watchful hub.

    :param email: The user's email.
    :type email: str
    :param password: The user's password.
    :type password: str
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    headers = {"Content-Type": "application/json"}
    credentials = base64.b64encode(str.encode(f"{email}:{password}")).decode(
        "utf-8"
    )
    headers.update({"Authorization": f"Basic {credentials}"})
    conn = _get_conn()
    conn.request("POST", "/remote", json.dumps({"verb": "login"}), headers)
    return _assert_success(_read_response(conn.getresponse()))


def publish(token: Optional[str] = None) -> Optional[Dict]:
    """
    This function performs publish with Watchful hub.

    :param token: The user's auth token, defaults to None.
    :type token: str, optional
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return hub_api("publish", token)


def fetch(token: Optional[str] = None) -> Optional[Dict]:
    """
    This function performs fetch with Watchful hub.

    :param token: The user's auth token, defaults to None.
    :type token: str, optional
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return hub_api("fetch", token)


def pull(token: Optional[str] = None) -> Optional[Dict]:
    """
    This function performs pull with Watchful hub.

    :param token: The user's auth token, defaults to None.
    :type token: str, optional
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return hub_api("pull", token)


def push(token: Optional[str] = None) -> Optional[Dict]:
    """
    This function performs push with Watchful hub.

    :param token: The user's auth token, defaults to None.
    :type token: str, optional
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return hub_api("push", token)


def peek(token: Optional[str] = None) -> Optional[Dict]:
    """
    This function performs peek with Watchful hub.

    :param token: The user's auth token, defaults to None.
    :type token: str, optional
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return hub_api("peek", token)


def whoami(token: Optional[str] = None) -> Optional[Dict]:
    """
    This function performs whoami with Watchful hub.

    :param token: The user's auth token, defaults to None.
    :type token: str, optional
    :return: The dictionary of the HTTP response from the connection request.
    :rtype: Dict, optional
    """

    return hub_api("whoami", token)
