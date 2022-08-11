"""
This script provides the functions required for interacting directly with
Watchful client.
"""
################################################################################


import os, subprocess, sys, time, io, socket, urllib
import http.client, json, csv

"""
TODO:
- we need a better way of signalling error besides making the caller check for
  it in the summary. Maybe throw from api() if error_msg is set?
"""

def _refresh():
    del sys.modules['watchful']
    import watchful

HOST="localhost"
PORT="9002"

def _get_conn():
    return http.client.HTTPConnection(HOST + ":" + PORT)

def await_port_opening(port, timeout_sec=10):
    """
    Waits for the port to be open.
    Returns true if the port was open within the timeout.
    Used for awaiting Watchful process startup.
    """
    end = time.time_ns() + (timeout_sec * 1_000_000_000)
    while time.time_ns() < end:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout_sec)
        res = sock.connect_ex((HOST, port))
        if res == 0:
            return
        time.sleep(0.001)
    raise Exception("Timed out waiting for Watchful to start")

def spawn_cmd(cmd, env=None):
    """Spawns a command and returns the PID of the spawned process"""
    proc = subprocess.Popen(
        cmd + " &\n echo $!", shell=True, stdout=subprocess.PIPE, env=env
    )
    out, _ = proc.communicate()
    pid = int(out.decode("utf-8")[:-1])
    return pid

def await_summary(pred, halt_fn=lambda x: False, unchanged_timeout=60):
    """
    Returns the summary once predicate(summary) returns true.
    Stops waiting once the halt function returns true, and returns None.
    Raises an exception if the summary is unchanged for `unchanged_timeout`
    seconds.
    """
    prev_summary = None
    end = float('inf')
    while time.time_ns() < end:
        summary = get()
        if halt_fn(summary):
            return None
        if pred(summary):
            return summary
        if prev_summary == summary:
            end = time.time_ns() + (unchanged_timeout * 1_000_000_000)
        prev_summary = summary
        time.sleep(0.03) # 30ms sleep for ~30 requests per second

    raise Exception("Timed out awaiting summary. Summary went stale for "
                    + str(unchanged_timeout)
                    + " seconds")

def _assert_success(summary):
    if "error_msg" in summary and summary["error_msg"]:
        verb_str = ""
        if "error_verb" in summary and summary["error_verb"]:
            verb_str = " ({})".format(summary["error_verb"])
        raise Exception(
            "Summary error{}: {}".format(verb_str, summary["error_msg"])
        )
    return summary

def _read_response_summary(resp):
    assert(200 == int(resp.status))
    ret = json.loads(resp.read())
    """ one idea:
    if ret["error_msg"]:
        raise Exception(ret["error_msg"])
    """
    if "error_msg" in ret and ret["error_msg"]:
        print(ret["error_msg"])
    return ret


def api(verb, **args):
    """
    Convenience method for API calls. See web/api.md for verbs and their
    arguments.
    """
    action = args # already a dictionary
    action["verb"] = verb
    return api_send_action(action)

def api_send_action(action):
    """
    Convenience method used by `se_auto_evaluator.py`. Consequently, api()
    should be refactored to reuse this function abstraction. Why? Where `action`
    is directly available, we should not strip it for api() and reform it in
    api().
    """
    conn = _get_conn()
    conn.request(
        "POST","/api", json.dumps(action), {"Content-Type":"application/json"}
    )
    return _read_response_summary(conn.getresponse())

def ephemeral(port="9002"):
    """Start the backend for an interactive session without persistence."""
    spawn_cmd(
        f"watchful -p {port} --no-persistence >watchful_ephemeral_output.txt 2>&1"
    )
    await_port_opening(int(port))
    external(port=port)

def external(host="localhost", port="9001"):
    global HOST, PORT
    HOST=host
    PORT=port

def list_projects():
    c = _get_conn()
    c.request("GET", "/projects")
    r = c.getresponse()
    return json.loads(r.read())

def open_project(id):
    c = _get_conn()
    c.request(
        "POST", "/projects", json.dumps(id), {"Content-Type":"application/json"}
    )
    r = c.getresponse()
    ret = r.read()
    await_plabels()
    return ret

def create_project():
    return open_project("new")

# def poll():

def records(csv):
    return api("records", data=csv, content_type="text/csv")

def class_(class_):
    return api("class", name=class_)

def create_class(class_, class_type="ftc"):
    return api("class", name=class_, class_type=class_type)

def query_async(q, page=0):
    return api("query", query=q, page=page)

def query(q, page=0):
    query_async(q, page)
    return await_summary(
        lambda s: s["query_completed"],
        lambda s: s["query"] != q
    )

def query_all(q, max_pages=0):
    """
    Evaluates the query returning the results as opposed to the summary. Will by
    default return all results for the query (all pages). This can be limited by
    setting max_pages to the positive number of pages you want.
    Each query result is a vector with a string for each field that is returned.
    Note: TOKS, SENTS, CELLS queries only return one field and each result will
          be wrapped in a vector of one string.
    """
    page = 0
    while True:
        summary = query(q, page)
        for fields in [cand["fields"] for cand in summary["candidates"]]:
            yield fields
        page += 1
        if summary["query_end"] or max_pages and page == max_pages:
            break

def base_rate(class_, rate):
    return api("base_rate", label=class_, rate=rate)

def await_plabels():
    return await_summary(lambda s: s['status'] == "current")

def hinter_async(class_, query, weight):
    return api("hinter", label=class_, weight=weight, query=query)

def hinter(class_, query, weight):
    _assert_success(hinter_async(class_, query, weight))
    return await_plabels()

def delete(id):
    return api("delete", id=id)

def delete_class(class_):
    return api("delete", class_name=class_)

def get():
    return api("nop")

def title(title):
    return api("title", title=title)

def external_hinter(class_, name, weight):
    _assert_success(api(
        "hinter", query="[external]", name=name, label=class_, weight=weight
    ))
    return await_plabels()

# load attributes into local watchful using dataset_id:string and
# attributes_filename:string
def load_attributes(dataset_id, attributes_filename):
    return api("attributes", id=dataset_id, file=attributes_filename)

def _dump(offset):
    return api("dump", offset=offset)

def dump():
    """ a generator that returns all the candidates in "hint API order"."""
    n_cands = get()["n_candidates"]
    offset = 0
    chunk = []
    while offset < n_cands:
        summary = _dump(offset)
        # TODO: handle errors
        chunk = summary["candidates"]
        offset += len(chunk)
        i = 0
        while i < len(chunk):
            yield chunk[i]
            i += 1

def dump_dicts():
    field_names = get()['field_names']
    for c in dump():
        yield dict(zip(field_names, c))

# TODO: come up with a better streaming Python API here
def hint(name, offset, values):
    values = list(map(lambda x: x and 1 or 0, values))
    return _assert_success(api("hint", name=name, offset=offset, values=values))

def apply_hints(name):
    _assert_success(api("apply_hints", name=name))
    return await_plabels()

def hint_all(name, values):
    hint(name, 0, values)
    return apply_hints(name)

def export_stream(content_type="text/csv", mode="ftc"):
    """
    Begins the export using the export_stream call.  The result is not
    JSON, but is data to be processed directly.

    For FTC mode, content_type must be text/csv and mode must be ftc.

    For NER mode, content_type must be application/jsonlines and mode
    must be ner.

    On success, returns the HTTPResponse object from which you can
    read the data.
    """
    conn = _get_conn()
    conn.request(
        "GET",
        "/export_stream?content-type=" + urllib.parse.quote_plus(content_type) \
        + "&mode=" + urllib.parse.quote_plus(mode)
    )
    resp = conn.getresponse()
    assert(200 == int(resp.status))
    return resp

def export_async():
    return api("export", query="", content_type="text/csv")

def export():
    n_exports = len(get()['exports'])
    export_async()
    return await_summary(lambda s: len(s['exports']) == n_exports + 1)

def export_preview(mode="ftc"):
    """
    Returns a preview of the export on summary key 'export_preview'
    The mode parameter can be "ftc" or "ner".
    """
    return api("export_preview", mode=mode)

def create_dataset(csv_bytes, columns, filename="none", has_header=True):
    # TODO: Add error handling
    from uuid import uuid4
    id_ = str(uuid4())
    conn = _get_conn()
    conn.request(
        "POST",
        "/api/_stream/" + id_ + "/0/true",
        csv_bytes,
        {"Content-Type":"text/csv"}
    )
    _ = _read_response_summary(conn.getresponse())
    params = json.dumps({"filename":filename,"has_header":has_header})
    conn.request(
        "POST",
        "/api/_stream/" + id_,
        params,
        {"Content-Type":"application/json"}
    )
    dataset_id = _read_response_summary(conn.getresponse())["id"]
    api("dataset_add", id=dataset_id, columns=columns)
    return dataset_id

def label_single(row):
    c = _get_conn()
    sio = io.StringIO()
    w = csv.writer(sio)
    w.writerow(row)
    csv_row = sio.getvalue().encode('utf-8')
    sio.close()
    c.request("POST", "/label", csv_row, {"Content-Type":"text/csv"})
    r = c.getresponse()
    resp_body = r.read().decode('utf-8')
    csv_str = io.StringIO(resp_body)
    rdr = csv.reader(csv_str)
    rdr_list = list(rdr)
    if len(rdr_list) == 2:
        fields = get()['field_names']
        assert fields == rdr_list[0][0:len(fields)], \
            "server prepended the header to the labeled row"
    else:
        assert len(rdr_list) == 1, "server returned a single row"
    return rdr_list[-1]

def config_set(key, value):
    conn = _get_conn()
    params = json.dumps({"verb":"set","key":key,"value":value})
    conn.request("POST","/config", params, {"Content-Type":"application/json"})
    return _read_response_summary(conn.getresponse())

def print_candidates(summary=None):
    if summary is None:
        summary = get()
    print(
        "\n".join([",".join(summary['field_names'])] \
        + list(map(lambda c: ",".join(c['fields']), summary["candidates"])))
    )

def candidate_dicts(summary=None):
    if summary is None:
        summary = get()
    return list(map(
        lambda c: dict(zip(summary['field_names'], c['fields'])),
        summary['candidates']
    ))

def exit_backend():
    # note the API call will usually fail because the backend exits before
    # returning an HTTP response so we suppress the error.
    try:
        api("exit")
    except:
        None

### Hub API

def hub_api(verb, token, **args):
    """
    Convenience method for collaboration API calls.
    """
    headers = {"Content-Type":"application/json"}
    headers.update({"Authorization": "Bearer " + token})
    action = args  # already a dictionary
    action["verb"] = verb
    conn = _get_conn()
    conn.request("POST","/remote",json.dumps(action),headers)
    return _read_response_summary(conn.getresponse())

def login(credentials):
    return hub_api("login", credentials=credentials)

def publish(token=None):
    return hub_api("publish", token)

def fetch(token=None):
    return hub_api("fetch", token)

def pull(token=None):
    return hub_api("pull", token)

def push(token=None):
    return hub_api("push", token)

def peek(token=None):
    return hub_api("peek", token)
