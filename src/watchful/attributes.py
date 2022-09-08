"""
This script provides the functions required for data enrichment.
"""
################################################################################


import csv
import json
import numbers
import os
import pprint
import re
import sys
from typing import Callable, Dict, List, Optional, Tuple, Union
from multiprocessing import Pool
import psutil
from watchful import client

pprint._sorted = lambda x: x
pprint = pprint.PrettyPrinter(indent=4).pprint


# Generally, you should not need to edit these variables directly.
IS_MULTIPROC = False
MULTIPROC_CHUNKSIZE = None
ENRICHMENT_ARGS = None
ATTR_WRITER = None
EnrichedCell = List[
    Tuple[Union[List[Tuple[int]], Dict[str, List[str]], Optional[str]]]
]

# Constants for encoding spans into compact strings
BASE = 64
COMP_LEN = 8

# '0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmno'
numerals = dict(map(
    lambda ic: (ic[0], chr(ic[1])), enumerate(range(48, 48+BASE))
))

# "#$%&'()*"
compressed = dict(map(
    lambda ic: (ic[0], chr(ic[1])), enumerate(range(35, 35+COMP_LEN))
))


def set_multiprocessing(is_multiproc: bool) -> None:
    global IS_MULTIPROC
    global MULTIPROC_CHUNKSIZE
    if is_multiproc and not IS_MULTIPROC:
        IS_MULTIPROC = True
        MULTIPROC_CHUNKSIZE = 500
    elif not is_multiproc:
        IS_MULTIPROC = False
        MULTIPROC_CHUNKSIZE = None


def set_multiproc_chunksize(multiproc_chunksize: int) -> None:
    assert multiproc_chunksize >= 1
    if IS_MULTIPROC:
        global MULTIPROC_CHUNKSIZE
        MULTIPROC_CHUNKSIZE = multiproc_chunksize


def print_multiproc_params() -> None:
    print(
        f"Multiprocessing: {IS_MULTIPROC}, "
        f"Multiprocessing chunksize: {MULTIPROC_CHUNKSIZE}"
    )


def base64(num):
    if num == 0:
        return numerals[0]

    ret = ""
    while num > 0:
        ret += numerals[num % BASE]
        num = num // BASE
    return ret[::-1]


def base64str(coll):
    """
    Takes a collection of numbers and returns a collection with strings
    representing those numbers in base64.

    Additional compression is done by concatenating consecutive base64 strings
    of the same length. This compressed encoding is detected by inspecting the
    first character in the ASCII range 35 inclusive to 42 inclusive. The rest of
    the string should be partitioned into their own strings with length
    ascii_code(s[0]) - 34.

    For example:
    * String: "#1234" represents "1,2,3,4"
    * String: "$1234" represents "12,34"
    * String: "&1234" represents "1234", (which is never compressed since the
    value is smaller than the compressed)
    (compression will not be done for a base64 encoded number if it is preceded
    or suceeded by a base64 encoded number of a different length)

    Here are the ascii codes for the prefixes in the examples above:
    * "#" => 35
    * "$" => 36
    * "&" => 38
    The range 35 inclusive to 42 inclusive was chosen because it contains
    characters that do not need to be escaped in JSON, nor does the range
    contain comma (','), which is used as a delimiter to concat all of the
    strings.
    """

    ret = []
    buf = []

    def push_buf(buf, s):
        if len(buf) == 0 or len(s) == len(buf[0]):
            buf.append(s)
        else:
            flush_buf()
            buf = push_buf([], s)
        return buf

    def flush_buf():
        buf_len = len(buf)
        if buf_len == 1:
            ret.append(buf[0])
        elif buf_len > 1:
            compress_idx = len(buf[0]) - 1
            if compress_idx < len(compressed): # compressed limit
                ret.append(compressed[compress_idx] + "".join(buf))
            else:
                for x in buf:
                    ret.append(x)

    for x in coll:
        buf = push_buf(buf, base64(x))
    flush_buf()
    return ",".join(ret)


def contig_spans(spans):
    """
    Decode spans to contig_spans/2 with
    [gap_len,span_len,...,gap_len_N,span_len_N].
    Takes a list of (start, end).
    """

    contig = []
    offset = 0
    for (a, b) in spans:
        contig.append(a - offset)
        contig.append(b - a)
        offset = b
    return contig


def writer(out, n_rows, n_cols):
    """
    Takes the output file and the number of rows & columns of the original
    dataset. Returns a write function that takes all of the attributes for a
    cell in the original dataset, where a cell is a single row and column pair.

    The cells attributes should be in this shape:
    [(spans:Vec<(int,int)>, attr_vals:Map<String,Vec<Any>>, name:Option<String>)
     ..
    ]
    * `spans` is a sorted vector of span [start, end) in the cell.
    * `attr_vals` is a map from attribute name to values for the `spans`. None
      means that the attribute has no value for that token defined by its span.
    * `name` is an optional parameter which can be used to give a name to the
      spans, where the attribute value of that name is the content of the spans
      themselves. Examples of this is: sentences, noun_chunks, tokens or
      collage_names.
    """

    # Write attribute and value mappings
    attrs = {}
    values = {}

    def write_jsonl(obj):
        json.dump(obj, out, separators=(',',':'))
        out.write('\n')

    def write(cell_data):
        new_attrs = []
        new_values = {}
        cell = []

        # For each span data tuple for the cell with the span and attribute
        # values.
        # for those span, do:
        for span_data in cell_data:
            span = span_data[0]
            attr_vals = span_data[1]
            name = span_data[2] if len(span_data) == 3 else None

            # Gather the new attributes and values
            # Gather and create new mappings at the same time. Duh :)
            for (attr,vals) in attr_vals.items():
                if attr not in attrs:
                    attrs[attr] = len(attrs) + 1
                    values[attr] = {}
                    new_attrs.append(attr)
                for val in vals:
                    if isinstance(val, numbers.Number):
                        val = str(val)
                    elif val and not isinstance(val, str):
                        raise Exception("Attribute value must be a string, "
                            "None or a number. Was: " + val)
                    if val and not val in values[attr]:
                        values[attr][val] = len(values[attr]) + 1
                        if attr not in new_values:
                            new_values[attr] = []
                        new_values[attr].append(val)

            # Create the vector for the current cell
            span_val = []
            if name is not None:
                span_val.append(name)
            for (attr,vals) in attr_vals.items():
                assert len(span) == len(vals), \
                    "Must be the same amount of spans as attribute values."
                # Not base64 the attributes to save space since there aren't
                # that many of them.
                span_val.append(attrs[attr])
                span_val.append(
                    base64str([values[attr][val] if val else 0 for val in vals])
                )

            cell.append(base64str(contig_spans(span)))
            cell.append(span_val)

        # Output the lines (attributes, values and the cell itself)
        if new_attrs:
            write_jsonl(["@"] + new_attrs)
        if new_values:
            for (k, vals) in new_values.items():
                write_jsonl(["$", k] + vals)
        write_jsonl(cell)

    # Write the header and return the write function to be called by users
    write_jsonl({"version":"0.3","rows":n_rows,"cols":n_cols})
    return write


def spacy_atterize(doc):
    """
    Extracts attributes from a SpaCy document. Attributes are associated to
    substrings, being tokens, entities, sentences or noun chunks. Every
    Substring is identified by its character start index and character end
    index.
    """

    # Return value
    cell = []

    def float_to_str(f):
        return str(round(f * 100.0))

    # Polarity and subjectivity via nlp.add_pipe("spacytextblob")
    cell.append(([(0, len(doc.text))],
             {"polarity": [float_to_str(doc._.blob.polarity)],
              "subjectivity": [float_to_str(doc._.blob.subjectivity)]}))

    # Token level assessment of the polarity and subjectivity
    assessments = {}
    for (words, polarity, subjectivity, _) in \
        doc._.blob.sentiment_assessments.assessments:
        for word in words:
            regex = re.compile(f"\\b{re.escape(word)}\\b")
            for m in regex.finditer(doc.text):
                span = m.span()
                if not span in assessments:
                    assessments[span] = (1, [polarity], [subjectivity])
                else:
                    (c, ps, ss) = assessments.get(span)
                    ps.append(polarity)
                    ss.append(subjectivity)
                    assessments[span] = (c+1, ps, ss)
    assess_spans = []
    pol_vals = []
    subj_vals = []
    for span in sorted(assessments.keys()):
        (c, ps, ss) = assessments.get(span)
        assess_spans.append(span)
        pol_vals.append(float_to_str(sum(ps) / c))
        subj_vals.append(float_to_str(sum(ss) / c))
    cell.append((
        assess_spans,
        {"token_polarity": pol_vals, "token_subjectivity": subj_vals}
    ))

    # Token level attributes
    tok_spans = []
    pos_vals = []
    tag_vals = []
    lem_vals = []
    case_vals = []
    for token in doc:
        tok_spans.append((token.idx, token.idx + len(token.text)))
        pos_vals.append(token.pos_)
        tag_vals.append(token.tag_)
        lem = token.lemma_
        lem_vals.append(lem if lem.isalpha() else None)
        case = None
        if token.is_lower:
            case = "lower"
        elif token.is_title:
            case = "title"
        elif token.is_upper:
            case = "upper"
        elif token.is_space:
            case = "space"
        elif token.is_punct:
            case = "punct"
        elif token.is_quote:
            case = "quote"
        elif token.is_digit:
            case = "number"
        elif token.is_bracket:
            case = "bracket"
        elif token.is_currency:
            case = "currency"
        case_vals.append(case)

    cell.append((
        tok_spans,
        {"pos":pos_vals,"tag":tag_vals,"lemma":lem_vals,"case":case_vals},
        "TOKS"
    ))

    # Entities
    ent_spans = []
    ent_vals = []
    for ent in doc.ents:
        ent_spans.append((ent.start_char, ent.end_char))
        ent_vals.append(ent.label_)
    cell.append((ent_spans, {"entity":ent_vals}))

    # Sentences and noun chunks
    cell.append((
        [(sent.start_char, sent.end_char) for sent in doc.sents],
        {},
        "SENTS"
    ))
    cell.append((
        [(chunk.start_char, chunk.end_char) for chunk in doc.noun_chunks],
        {},
        "NOUNCH"
    ))

    return cell


def spacy_atterize_fn(cell, spacy_atterize_, nlp):
    # Adding spacytextblob, cannot do it in load_spacy because of
    # our multiprocessing code. Adding a pipe to SpaCy is idempotent.
    from spacytextblob.spacytextblob import SpacyTextBlob \
        # pylint: disable=unused-import

    if not nlp.has_pipe("spacytextblob"):
        nlp.add_pipe("spacytextblob")
    return spacy_atterize_(nlp(cell))


def load_spacy():
    import spacy
    #nlp = spacy.load("en_core_web_sm", exclude=["parser"])
    nlp = spacy.load("en_core_web_sm")
    nlp.enable_pipe("senter")
    return nlp


def flair_atterize(sent):

    enriched_cell = []

    ent_spans = []
    ent_values = []
    ent_scores = []
    for ent in sent.get_spans("ner"):
        ent_spans.append((ent.start_position, ent.end_position))
        ent_values.append(ent.get_label("ner").value)
        ent_scores.append(
            str(int(round(ent.get_label("ner").score, 2) * 100))
        )
    enriched_cell.append(
        (ent_spans, {"entity": ent_values, "score": ent_scores}, "ENTS")
    )

    return enriched_cell


def flair_atterize_fn(cell, flair_atterize_, tagger_pred, sent_fn):
    sent = sent_fn(cell)
    tagger_pred(sent)
    return flair_atterize_(sent)


def load_flair():
    from flair.data import Sentence
    from flair.models import SequenceTagger
    # import logging
    # import warnings

    # logging.getLogger("flair").setLevel(logging.ERROR)
    # warnings.filterwarnings("ignore", module="huggingface_hub")

    tagger = SequenceTagger.load("ner")
    return (tagger.predict, Sentence)


def enrich_row(row):
    """
    Processes one row. Takes an input row and returns an output row. The
    global `ENRICHMENT_ARGS` would have previously been set.
    """
    atterize_fn = ENRICHMENT_ARGS[0]
    atterize_args = ENRICHMENT_ARGS[1:]

    cells = []

    for raw_cell in row:
        raw_cell = str(raw_cell)

        cell = atterize_fn(raw_cell, *atterize_args)
        adjust_span_offsets_from_char_to_byte(raw_cell, cell)

        cells.append(cell)

    return cells


def adjust_span_offsets_from_char_to_byte(raw_cell, cell):
    """
    Adjust all spans from character offsets to byte offsets, since Watchful's
    attributes API takes byte offsets and spacy provides us with character
    offsets.
    """
    byte_offsets = {}
    byte_offset = 0

    for char_offset, ch in enumerate(raw_cell):
        byte_offsets[char_offset] = byte_offset
        byte_offset += len(ch.encode('utf-8'))
    byte_offsets[len(raw_cell)] = byte_offset

    for context in cell:
        spans = context[0]
        for i, span in enumerate(spans):
            (start, end) = span
            spans[i] = (byte_offsets[start], byte_offsets[end])


def init_args(
    *args,
) -> None:
    """
    In this function, we initialize the user arguments as global variables.
    Generally, you should not need to edit this function.

    Initializes a per-process context with the user function that will be used
    in the imap. This is not necessarily thread-safe but is multiprocess-safe.
    """

    global ENRICHMENT_ARGS
    ENRICHMENT_ARGS = args


def enrich(
    in_file: str,
    out_file: str,
    enrich_row_fn: Callable,
    enrichment_args,
) -> None:
    """
    Usage:
    in_file: filepath of the csv formatted original dataset
    out_file: filepath where the enriched attributes in Watchful custom format
              for Watchful client ingestion are saved to
    enrich_row_fn: user custom function for enriching every row of the dataset
    enrichment_args: additional user variables to perform the data enrichment

    Generally, you should not need to edit this function.
    """
    sys_encoding = sys.getdefaultencoding()

    with open(in_file, "r", encoding=sys_encoding) as infile:
        in_reader = csv.reader(infile)
        n_cols = len(next(in_reader))
        n_rows = None
        for n_rows, _ in enumerate(in_reader, 1):
            pass

    with open(in_file, "r", encoding=sys_encoding) as infile, \
        open(out_file, "w", encoding=sys_encoding) as outfile:

        in_reader = csv.reader(infile)
        next(in_reader)

        global ATTR_WRITER
        ATTR_WRITER = writer(outfile, n_rows, n_cols)

        if IS_MULTIPROC:
            # Parallelize to the number of available cores (not the number of
            # available hyper threads).  `psutil` is the only standard Python
            # package that can provide this measure (with logical=False).
            # Testing revealed wall times to be quite close to using all logical
            # CPUs, with better overall system responsiveness and less thermal
            # throttling in this scenario.
            # Additionally, as Python's threading uses a GIL, it is unsuitable
            # for this task.  Use its multiprocessing intsead.  However,
            # multiprocessing uses pickle and is unable to send functions across
            # process boundaries, hence the global variable set by the
            # initializer.
            with Pool(
                initializer=init_args,
                initargs=enrichment_args,
                processes=psutil.cpu_count(logical=False),
            ) as pool:
                for enriched_row in pool.imap(
                    func=enrich_row_fn,
                    iterable=in_reader,
                    chunksize=MULTIPROC_CHUNKSIZE,
                ):
                    proc_enriched_row(enriched_row)
        else:
            init_args(*enrichment_args)
            for enriched_row in map(enrich_row_fn, in_reader):
                proc_enriched_row(enriched_row)

        del ATTR_WRITER


def proc_enriched_row(
    enriched_row: List[EnrichedCell],
) -> None:
    """
    This function is iterated over every enriched row. Optionally, you may add
    code if you wish to do something else with every row.
    """

    ########################################
    # Do not edit this code.
    for enriched_cell in enriched_row:
        proc_enriched_cell(enriched_cell)
    ########################################

    ########################################
    # EDIT THE CODE BELOW if you wish to do
    # something else with every row. Here
    # we simply print to stdout at the end
    # of every `enriched_row`.
    #print(f'{"*" * 20} end of row {"*" * 20}')
    ########################################


def proc_enriched_cell(
    enriched_cell: EnrichedCell,
) -> None:
    """
    This function is iterated over every enriched cell. Optionally, you may add
    code if you wish to do something else with every cell.
    """

    ########################################
    # Do not edit this code.
    ATTR_WRITER(enriched_cell)
    ########################################

    ########################################
    # EDIT THE CODE BELOW if you wish to do
    # something else with every cell. Here
    # we simply print every `enriched_cell`
    # to stdout.
    #print("Enriched cell: ")
    #pprint(enriched_cell)
    ########################################


def get_vars_for_enrich_row_with_attribute_data(attr_names, attr_file):

    f = open(attr_file, "r", encoding=sys.getdefaultencoding()) \
        # pylint: disable=consider-using-with
    attr_reader = csv.reader(f)
    attr_name_list_all = next(attr_reader)

    if not attr_names:
        attr_name_list = attr_name_list_all
        def get_attr_row(attr_row_all):
            return attr_row_all
    else:
        attr_name_list = attr_names.split(",")
        attr_locs = list(map(attr_name_list_all.index, attr_name_list))
        def get_attr_row(attr_row_all):
            return list(map(attr_row_all.__getitem__, attr_locs))

    return (get_attr_row, attr_name_list, attr_reader)


def enrich_row_with_attribute_data(row):
    """
    Extracts attributes from an attributes file. Attributes are associated to
    the entire text in each input dataset cell. The entire text in each input
    dataset cell is identified by its character start index and character end
    index.
    """

    get_attr_row_fn = ENRICHMENT_ARGS[0]
    attr_names = ENRICHMENT_ARGS[1]
    attr_row_reader = ENRICHMENT_ARGS[2]

    attr_row = get_attr_row_fn(next(attr_row_reader))
    cells_attrs = []

    for in_cell in row:
        # Just one span for example-level attributes.
        span = [(0, len(str(in_cell)))]

        # `cell_attrs` is to be appended over the len of attr_names.
        cell_attrs = []

        for attr_name, attr_val in zip(attr_names, attr_row):
            # This is the attribute representation per input dataset cell as
            # required by Watchful client interface. Watchful client has a
            # restriction of uppercase characters and maximum 6 characters on
            # attr_representn[2].
            # attr_representn = (
            #     span, {attr_name: [attr_val]},
            #     f"EX{attr_name.replace("_", "").upper()}"
            # )
            # attr_representn = (
            #     span, {attr_name: [attr_val]},
            #     "SOME_GENERAL_INFO_ABOUT_ALL_ATTRIBUTES"
            # )
            attr_representn = (span, {attr_name: [attr_val]})
            cell_attrs.append(attr_representn)

        # Another possible representation.
        # attr_dict = {}
        # for attr_name, attr_val in zip(attr_names, attr_row):
        #     attr_dict[attr_name] = [attr_val]
        # attr_representn = (span, attr_dict, "EXATTR")
        # cell_attrs.append(attr_representn)

        cell_attrs.append((span, {}, "EX"))

        cells_attrs.append(cell_attrs)

    return cells_attrs


def validate_attr_names(attr_names, attr_file):
    """
    Check that all attribute names are present in the attribute file. Returns
    `False` as soon as an attribute name is absent, or `True` when all attribute
    names match.
    """

    with open(attr_file, "r", encoding=sys.getdefaultencoding()) as f:
        attr_row_reader = csv.reader(f)
        attr_names_all = next(attr_row_reader)

    for attr_name in attr_names.split(","):
        if attr_name not in attr_names_all:
            return False

    return True


def atterize_values_in_cell(cell, name, values):
    """
    Helper function to `create_attribute_for_values()` for finding the spans for
    each value.
    """

    from heapq import merge
    cell = str(cell)
    matches = [
        [(m.start(), m.end()) for m in re.finditer(pattern, cell)]
        for pattern in values
    ]
    spans = list(merge(*matches))
    return [(spans, {}, name)]


def create_attribute_for_values(attribute_name, values):
    """
    Takes an attribute name and a list of known values to create attributes for.
    The list of values will be looked up in each cell of the currently loaded
    dataset. An attributes file will be prepared to be loaded in watchful.
    Returns the filename of the attribute file created, which can be used by the
    attributes action and function:
    api.load_attributes(dataset_id, attribute_filename)
    """

    in_file, out_file, out_filename = get_context(attribute_name)
    enrich(
        in_file,
        out_file,
        enrich_row,
        (atterize_values_in_cell, attribute_name, values)
    )
    return out_filename


def get_context(attribute_filename):
    """
    Given a `attribute_filename`, find the current dataset file loaded in
    Watchful and produce the context needed to enrich that dataset. Also returns
    the filename of the file used by the attributes action and function:
    `api.load_attributes(dataset_id, attribute_filename)`
    """

    summary = client.get()
    attrs_dir = os.path.join(summary['watchful_home'], "datasets", "attrs")
    os.makedirs(attrs_dir, exist_ok=True)
    _, _, in_file = get_dataset_id_dir_filepath(summary)
    in_filename = os.path.basename(in_file)
    out_file = os.path.join(
        attrs_dir,
        f"{in_filename}_{attribute_filename}.attrs"
    )
    out_filename = os.path.basename(out_file)
    return in_file, out_file, out_filename


def get_dataset_id_dir_filepath(summary, in_file="", is_local=True):
    """
    This function returns the id, directory and filepath of the currently opened
    dataset.
    """

    summary = client._assert_success(summary)
    dataset_id = client.get_dataset_id(summary)
    datasets_dir = client.get_datasets_dir(summary, is_local)

    if in_file != "":
        # Check that `in_file` exists.
        if not os.path.isfile(in_file):
            raise Exception(f"File {in_file} does not exist.")
        dataset_filepath = in_file
    else:
        dataset_filepath = client.get_dataset_filepath(summary, is_local)

    return dataset_id, datasets_dir, dataset_filepath
