"""Watchful python library"""

# ruff: noqa: F401

# We need to be careful of unintended overriding, and also a deeper
# consideration into what should be made directly available in the ``watchful``
# namespace.
from .client import (
    await_port_opening,
    spawn_cmd,
    await_summary,
    register_summary_hook,
    request,
    api,
    api_send_action,
    ephemeral,
    external,
    list_projects,
    open_project,
    create_project,
    title,
    get_project_id,
    get_dataset_id,
    get_watchful_home,
    get_datasets_dir,
    get_dataset_filepath,
    records,
    set_column_flag,
    ignore_column_flag,
    class_,
    create_class,
    query_async,
    query,
    query_all,
    base_rate,
    await_plabels,
    hinter_async,
    hinter,
    delete,
    delete_class,
    get,
    upload_attributes,
    load_attributes,
    dump,
    dump_dicts,
    export_stream,
    export_dataset_to_path,
    export_async,
    export,
    export_preview,
    export_project,
    is_utf8,
    create_dataset,
    label_single,
    config_set,
    config,
    set_hub_url,
    candidate_dicts,
    hub_api,
    login,
    publish,
    fetch,
    pull,
    push,
    peek,
    whoami,
    WatchfulAppInstanceError,
)

from .client2 import Client
from .attributes import (
    set_multiprocessing,
    set_multiproc_chunksize,
    base64,
    base64str,
    contig_spans,
    writer,
    spacy_atterize,
    spacy_atterize_fn,
    load_spacy,
    flair_atterize,
    flair_atterize_fn,
    load_flair,
    enrich_row,
    adjust_span_offsets_from_char_to_byte,
    init_args,
    enrich,
    enrich_by_row,
    enrich_by_col,
    proc_enriched_row,
    proc_enriched_cell,
    get_vars_for_enrich_row_with_attribute_data,
    enrich_row_with_attribute_data,
    validate_attribute_names,
    atterize_values_in_cell,
    create_attribute_for_values,
    get_context,
    get_dataset_id_dir_filepath,
)
from .enrich import main
from .enricher import (
    set_enrich_fn_order,
    Enricher,
)
