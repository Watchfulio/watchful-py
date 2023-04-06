"""
This script provides the functions required for data enrichment.
"""
################################################################################


import csv
import io
import json
import numbers
import os
import pprint
import re
from heapq import merge
from multiprocessing import Pool
from typing import Callable, Dict, List, Optional, Tuple
import psutil
from watchful import client, enricher


# Utility printer.
pprint._sorted = lambda x: x
pprint = pprint.PrettyPrinter(indent=4).pprint


# Constants for use in the data enrichment. Generally, they should not be edited
# directly.
IS_MULTIPROC = False
MULTIPROC_CHUNKSIZE = None
ENRICHMENT_ARGS = None
ATTR_WRITER = None


# Constants for encoding spans into compact strings. Do not edit them.
BASE = 64
COMPRESSED_LEN = 8

# Chars: "0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmno"
NUMERALS = dict(
    map(lambda ic: (ic[0], chr(ic[1])), enumerate(range(48, 48 + BASE)))
)

# Chars: "#$%&'()*"
COMPRESSED = dict(
    map(
        lambda ic: (ic[0], chr(ic[1])),
        enumerate(range(35, 35 + COMPRESSED_LEN)),
    )
)


def set_multiprocessing(is_multiproc: bool) -> None:
    """
    This function sets whether multiprocessing is used for the data enrichment.
    This is still in internal alpha mode and is not expected to be used by user.

    :param is_multiproc: The multiprocessing flag.
    :type is_multiproc: bool
    """

    global IS_MULTIPROC
    global MULTIPROC_CHUNKSIZE
    if is_multiproc and not IS_MULTIPROC:
        IS_MULTIPROC = True
        MULTIPROC_CHUNKSIZE = 500
    elif not is_multiproc:
        IS_MULTIPROC = False
        MULTIPROC_CHUNKSIZE = None


def set_multiproc_chunksize(multiproc_chunksize: int) -> None:
    """
    This function sets the multiprocessing chunk size for the data enrichment,
    if multiprocessing is used. This is still in internal alpha mode and is not
    expected to be used by user.

    :param multiproc_chunksize: The multiprocessing chunk size, at least 1.
    :type multiproc_chunksize: int
    """

    assert multiproc_chunksize >= 1
    if IS_MULTIPROC:
        global MULTIPROC_CHUNKSIZE
        MULTIPROC_CHUNKSIZE = multiproc_chunksize


def print_multiproc_params() -> None:
    """
    This function prints the multiprocessing flag and the multiprocessing chunk
    size for the data enrichment. This is still in internal alpha mode and is
    not expected to be used by user.
    """

    print(
        f"multiprocessing: {IS_MULTIPROC}, "
        f"multiprocessing chunksize: {MULTIPROC_CHUNKSIZE}"
    )


def base64(num: int) -> str:
    """
    This function takes in an integer value and returns its encoded string
    value.

    :param num: The integer value.
    :type num: int
    :return: The encoded string value.
    :rtype: str
    """

    if num == 0:
        return NUMERALS[0]

    ret = ""
    while num > 0:
        ret += NUMERALS[num % BASE]
        num = num // BASE

    return ret[::-1]


def base64str(list_of_integers: List[int]) -> str:
    """
    This function takes in a list of integers and returns its encoded string
    value with substrings representing those integers in base64.

    Additional compression is done by concatenating consecutive base64 strings
    of the same length. This compressed encoding is detected by inspecting the
    first character s[0] in the ASCII range 35 inclusive to 42 inclusive. The
    rest of the string s[1:] should be partitioned into its own strings with
    length ascii_code(s[0]) - 34 each.

    Examples:
    "#1234" represents "1,2,3,4"
    "$1234" represents "12,34"
    "&1234" represents "1234" (which is never compressed since the original
    value is shorter than the compressed value)

    The ASCII codes for the character prefixes in the examples are:
    "#" => 35
    "$" => 36
    "&" => 38

    Compression will not be done for a base64 encoded integer if it is preceded
    or suceeded by a base64 encoded integer of a different length.

    The range 35 inclusive to 42 inclusive is chosen because it contains
    characters that do not need to be escaped in JSON, nor does the range
    contain comma (",") as it is used as a delimiter to concatenate all of the
    strings.

    :param list_of_integers: The list of integers.
    :type list_of_integers: List[int]
    :return: The encoded string value.
    :rtype: str
    """

    ret = []
    buf = []

    def flush_buf():
        buf_len = len(buf)
        if buf_len == 1:
            ret.append(buf[0])
        elif buf_len > 1:
            compress_idx = len(buf[0]) - 1
            if compress_idx < COMPRESSED_LEN:  # compression limit
                ret.append(f'{COMPRESSED[compress_idx]}{"".join(buf)}')
            else:
                for x in buf:
                    ret.append(x)

    def push_buf(buf, s):
        if len(buf) == 0 or len(s) == len(buf[0]):
            buf.append(s)
        else:
            flush_buf()
            buf = push_buf([], s)
        return buf

    for x in list_of_integers:
        buf = push_buf(buf, base64(x))
    flush_buf()

    return ",".join(ret)


def contig_spans(spans: List[Tuple[int, int]]) -> List[int]:
    """
    This function decodes a list of spans, i.e.
    [(start_1, end,_1), ..., (start_N, end_N)] to a list of contiguous spans,
    i.e. [gap_len_1, span_len_1, ..., gap_len_N, span_len_N].

    :param spans: The list of spans.
    :type spans: List[Tuple[int, int]]
    :return: The list of contiguous spans.
    :rtype: List[int]
    """

    contig = []
    offset = 0
    for a, b in spans:
        contig.append(a - offset)
        contig.append(b - a)
        offset = b

    return contig


def writer(output: io.TextIOWrapper, n_rows: int, n_cols: int) -> Callable:
    """
    This function takes in the output file object and the number of rows and
    columns of the dataset. It returns a write function that takes in all of the
    attributes for a cell in the dataset, where a cell is located on a row and a
    column pair.

    The cells' attributes should be in this shape (note that the following is in
    Rust idiom):
    [
      (
        spans: Vec<(int, int)>,
        attr_vals: Map<String, Vec<Any>>,
        name: Option<String>
      ),
      ..
    ]
    ``spans`` is a sorted vector of span (start, end) in the cell.
    ``attr_vals`` is a map from attribute name to values for the ``spans``. None
    means that the attribute has no value for that token defined by its span.
    ``name`` is an optional parameter which can be used to give a name to the
    spans, where the attribute value of that name is the content of the spans
    themselves. Examples of this are sentences, noun_chunks, tokens or
    collage_names.

    :param output: The output file for all the attributes of a dataset.
    :type output: io.TextIOWrapper
    :param n_rows: The number of rows of the original dataset.
    :type n_rows: int
    :param n_cols: The number of columns of the original dataset.
    :type n_cols: int
    :return: The function that takes in all of the attributes for a cell in the
        dataset and writes an encoded representation for them onto output.
    :rtype: Callable
    """

    # Dictionaries for writing the attribute and value mappings.
    attrs = {}
    values = {}

    def write_jsonl(obj):
        json.dump(obj, output, separators=(",", ":"))
        output.write("\n")

    def write(cell_data):
        new_attrs = []
        new_values = {}
        cell = []

        # For each span data tuple for the cell with the span and attribute
        # values.
        # For those spans, do:
        for span_data in cell_data:
            span = span_data[0]
            attr_vals = span_data[1]
            name = span_data[2] if len(span_data) == 3 else None

            # Gather the new attributes and values
            # Gather and create new mappings at the same time. Duh :)
            for attr, vals in attr_vals.items():
                if attr not in attrs:
                    attrs[attr] = len(attrs) + 1
                    values[attr] = {}
                    new_attrs.append(attr)
                for val in vals:
                    if isinstance(val, numbers.Number):
                        val = str(val)
                    elif val and not isinstance(val, str):
                        raise Exception(
                            "Attribute value must be a string, "
                            "None or a number. Was: " + val
                        )
                    if val and not val in values[attr]:
                        values[attr][val] = len(values[attr]) + 1
                        if attr not in new_values:
                            new_values[attr] = []
                        new_values[attr].append(val)

            # Create the vector for the current cell.
            span_val = []
            if name is not None:
                span_val.append(name)
            for attr, vals in attr_vals.items():
                assert len(span) == len(
                    vals
                ), "Must be the same amount of spans as attribute values."
                # Not base64 the attributes to save space since there aren't
                # that many of them.
                span_val.append(attrs[attr])
                span_val.append(
                    base64str([values[attr][val] if val else 0 for val in vals])
                )

            cell.append(base64str(contig_spans(span)))
            cell.append(span_val)

        # Output the lines (attributes, values and the cell value itself).
        if new_attrs:
            write_jsonl(["@"] + new_attrs)
        if new_values:
            for k, vals in new_values.items():
                write_jsonl(["$", k] + vals)
        write_jsonl(cell)

    # Write the header once and return the write function to be called by users.
    write_jsonl({"version": "0.3", "rows": n_rows, "cols": n_cols})
    return write


def spacy_atterize(
    doc,  # spacy.tokens.doc.Doc (remove type hint to reduce load time)
) -> enricher.EnrichedCell:
    """
    This function creates an enriched cell from the cell inference derived by
    SpaCy NLP. It extracts attributes from a SpaCy document. Attributes are
    associated to substrings, being tokens, entities, sentences or noun chunks.
    Every Substring is identified by its character start index and character end
    index.

    :param doc: Cell inference.
    :type doc: spacy.tokens.doc.Doc
    :return: The enriched cell.
    :rtype: enricher.EnrichedCell
    """

    # Return value
    cell = []

    def float_to_str(f):
        return str(round(f * 100.0))

    # Polarity and subjectivity via nlp.add_pipe("spacytextblob")
    cell.append(
        (
            [(0, len(doc.text))],
            {
                "polarity": [float_to_str(doc._.blob.polarity)],
                "subjectivity": [float_to_str(doc._.blob.subjectivity)],
            },
        )
    )

    # Token level assessment of the polarity and subjectivity
    assessments = {}
    for (
        words,
        polarity,
        subjectivity,
        _,
    ) in doc._.blob.sentiment_assessments.assessments:
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
                    assessments[span] = (c + 1, ps, ss)
    assess_spans = []
    pol_vals = []
    subj_vals = []
    for span in sorted(assessments.keys()):
        (c, ps, ss) = assessments.get(span)
        assess_spans.append(span)
        pol_vals.append(float_to_str(sum(ps) / c))
        subj_vals.append(float_to_str(sum(ss) / c))
    cell.append(
        (
            assess_spans,
            {"token_polarity": pol_vals, "token_subjectivity": subj_vals},
        )
    )

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

    cell.append(
        (
            tok_spans,
            {
                "pos": pos_vals,
                "tag": tag_vals,
                "lemma": lem_vals,
                "case": case_vals,
            },
            "TOKS",
        )
    )

    # Entities
    ent_spans = []
    ent_vals = []
    for ent in doc.ents:
        ent_spans.append((ent.start_char, ent.end_char))
        ent_vals.append(ent.label_)
    cell.append((ent_spans, {"entity": ent_vals}))

    # Sentences and noun chunks
    cell.append(
        ([(sent.start_char, sent.end_char) for sent in doc.sents], {}, "SENTS")
    )
    cell.append(
        (
            [(chunk.start_char, chunk.end_char) for chunk in doc.noun_chunks],
            {},
            "NOUNCH",
        )
    )

    return cell


def spacy_atterize_fn(
    cell: str,
    spacy_atterize_: Callable,
    nlp: Callable,
) -> enricher.EnrichedCell:
    """
    This function creates an enriched cell from the original cell using the
    SpaCy NLP enrichment objects.

    :param cell: The original cell.
    :type cell: str
    :param spacy_atterize_: The enrichment function that creates an enriched
        cell from the cell inference derived by SpaCy NLP.
    :type spacy_atterize_: Callable
    :param nlp: A SpaCy NLP enrichment object.
    :type nlp: Callable
    :return: The enriched cell.
    :rtype: enricher.EnrichedCell
    """

    # Adding spacytextblob, cannot do it in load_spacy because of
    # our multiprocessing code. Adding a pipe to SpaCy is idempotent.
    from spacytextblob.spacytextblob import (  # pylint: disable=unused-import
        SpacyTextBlob,
    )

    if not nlp.has_pipe("spacytextblob"):
        nlp.add_pipe("spacytextblob")
    return spacy_atterize_(nlp(cell))


def load_spacy() -> Tuple:
    """
    This function creates and returns the SpaCy NLP objects for data enrichment.

    :return: The tuple of SpaCy NLP objects.
    :rtype: Tuple
    """

    import spacy

    # nlp = spacy.load("en_core_web_sm", exclude=["parser"])
    nlp = spacy.load("en_core_web_sm")
    nlp.enable_pipe("senter")

    return (nlp,)


def flair_atterize(
    sent,  # flair.data.Sentence (remove type hint to reduce load time)
) -> enricher.EnrichedCell:
    """
    This function creates an enriched cell from the cell inference derived by
    Flair NLP. It extracts attributes from a Flair paragraph. Attributes are
    associated to substrings, being tokens, entities, sentences or noun chunks.
    Every Substring is identified by its character start index and character end
    index.

    :param sent: Cell inference.
    :type sent: flair.data.Sentence
    :return: The enriched cell.
    :rtype: enricher.EnrichedCell
    """

    enriched_cell = []

    ent_spans = []
    ent_values = []
    ent_scores = []
    for ent in sent.get_spans("ner"):
        ent_spans.append((ent.start_position, ent.end_position))
        ent_values.append(ent.get_label("ner").value)
        ent_scores.append(str(int(round(ent.get_label("ner").score, 2) * 100)))
    enriched_cell.append(
        (ent_spans, {"entity": ent_values, "score": ent_scores}, "ENTS")
    )

    return enriched_cell


def flair_atterize_fn(
    cell: str,
    flair_atterize_: Callable,
    tagger_pred: Callable,
    sent_fn: Callable,
) -> enricher.EnrichedCell:
    """
    This function creates an enriched cell from the original cell using the
    Flair NLP enrichment objects.

    :param cell: The original cell.
    :type cell: str
    :param flair_atterize_: The enrichment function that creates an enriched
        cell from the cell inference derived by Flair NLP.
    :type flair_atterize_: Callable
    :param tagger_pred: A Flair NLP enrichment object.
    :type tagger_pred: Callable
    :param sent_fn: A Flair NLP enrichment object.
    :type sent_fn: Callable
    :return: The enriched cell.
    :rtype: enricher.EnrichedCell
    """

    sent = sent_fn(cell)
    tagger_pred(sent)

    return flair_atterize_(sent)


def load_flair() -> Tuple:
    """
    This function creates and returns the Flair NLP objects for data enrichment.

    :return: The tuple of Flair NLP objects.
    :rtype: Tuple
    """

    from flair.data import Sentence
    from flair.models import SequenceTagger

    # import logging
    # import warnings
    # logging.getLogger("flair").setLevel(logging.ERROR)
    # warnings.filterwarnings("ignore", module="huggingface_hub")

    tagger = SequenceTagger.load("ner")

    return (tagger.predict, Sentence)


@enricher.set_enrich_fn_order()
def enrich_row(
    row: Dict[Optional[str], Optional[str]]
) -> List[enricher.EnrichedCell]:
    """
    This function enriches one row. It takes named cells of an input row and
    returns an enriched row. The global ``ENRICHMENT_ARGS`` would have
    previously been set so it can be used here.

    :param row: The dictionary of named cell values in the row.
    :type row: Dict[Optional[str], Optional[str]]
    :return: The list of enriched cell values in the row.
    :rtype: List[enricher.EnrichedCell]
    """

    assert (
        None not in row
    ), "Dataset error: there is at least one unused cell value!"
    row_values = row.values()
    assert (
        None not in row_values
    ), "Dataset error: there is at least one absent cell value!"

    atterize_fn = ENRICHMENT_ARGS[0]
    atterize_args = ENRICHMENT_ARGS[1:]

    enriched_row = []

    for cell in row_values:
        cell = str(cell)

        enriched_cell = atterize_fn(cell, *atterize_args)
        adjust_span_offsets_from_char_to_byte(cell, enriched_cell)

        enriched_row.append(enriched_cell)

    return enriched_row


def adjust_span_offsets_from_char_to_byte(
    cell: str,
    enriched_cell: enricher.EnrichedCell,
) -> enricher.EnrichedCell:
    """
    This function adjusts all the spans of an enriched cell from character
    offsets to byte offsets, since Watchful's data enrichment API takes in byte
    offsets. This is useful if your data enrichment functions and models creates
    character offsets.

    :param cell: The string value contained in the cell.
    :type cell: str
    :param enriched_cell: A list of attributes for the cell.
    :type enriched_cell: enricher.EnrichedCell
    :return: The list of attributes for the cell whose span offsets have been
        adjusted.
    :rtype: enricher.EnrichedCell
    """

    byte_offsets = {}
    byte_offset = 0

    for char_offset, ch in enumerate(cell):
        byte_offsets[char_offset] = byte_offset
        byte_offset += len(ch.encode("utf-8"))
    byte_offsets[len(cell)] = byte_offset

    for context in enriched_cell:
        spans = context[0]
        for i, span in enumerate(spans):
            (start, end) = span
            spans[i] = (byte_offsets[start], byte_offsets[end])

    return enriched_cell


def init_args(*args) -> None:
    """
    In this function, we create variables that we will store in the global
    :attr:`ENRICHMENT_ARGS`. We then later use them in :func:`enrich_row` to
    enrich our data row by row.

    This function initializes a per-process context with the user function that
    will be used in the ``multiprocessing.Pool.imap``. This is not necessarily
    thread-safe but is multiprocess-safe.

    :param args: A tuple of objects of any type, to be used for the data
        enrichment.
    :type args: Tuple
    """

    global ENRICHMENT_ARGS
    ENRICHMENT_ARGS = args


def enrich(
    in_file: str,
    out_file: str,
    enrich_row_or_col_fn: Callable,
    enrichment_args: Tuple,
) -> None:
    """
    This function enriches a dataset, using an enrichment function that enriches
    either row by row or column by column and other enrichment objects, and then
    produces the attributes.

    :param in_file: The filepath of the csv formatted original dataset or the
        dataset exported from Watchful. This latter will be the former appended
        with the Watchful columns "Hints" and "HandLabels". It follows that
        these columns are reserved for Watchful and should not be present in the
        original dataset.
    :type in_file: str
    :param out_file: The filepath where the enriched attributes in Watchful
        custom format for ingestion by Watchful application are written to.
    :type out_file: str
    :param enrich_row_or_col_fn: The user custom function for enriching every
        row or every column of the dataset.
    :type enrich_row_or_col_fn: Callable
    :param enrichment_args: The additional enrichment objects to perform the
        data enrichment.
    :type enrichment_args: Tuple
    """

    order = enrich_row_or_col_fn.order
    if order == "row":
        enrich_by_row(in_file, out_file, enrich_row_or_col_fn, enrichment_args)
    elif order == "col":
        enrich_by_col(in_file, out_file, enrich_row_or_col_fn, enrichment_args)
    else:
        raise ValueError(
            f'The enrichment order "{order}" is unrecognized; use one of '
            f"{enricher.ENRICHMENT_ORDERS}."
        )


def enrich_by_row(
    in_file: str,
    out_file: str,
    enrich_row_fn: Callable,
    enrichment_args: Tuple,
) -> None:
    """
    This function enriches a dataset, using an enrichment function that enriches
    row by row and other enrichment objects, and then produces the attributes.

    :param in_file: The filepath of the csv formatted original dataset or the
        dataset exported from Watchful. This latter will be the former appended
        with the Watchful columns "Hints" and "HandLabels". It follows that
        these columns are reserved for Watchful and should not be present in the
        original dataset.
    :type in_file: str
    :param out_file: The filepath where the enriched attributes in Watchful
        custom format for ingestion by Watchful application are written to.
    :type out_file: str
    :param enrich_row_fn: The user custom function for enriching every row of
        the dataset.
    :type enrich_row_fn: Callable
    :param enrichment_args: The additional enrichment objects to perform the
        data enrichment.
    :type enrichment_args: Tuple
    """

    with open(in_file, encoding="utf-8", newline="") as infile:
        in_reader = csv.reader(infile)
        n_cols = len(next(in_reader))
        n_rows = None
        for n_rows, _ in enumerate(in_reader, 1):
            pass

    with open(in_file, encoding="utf-8", newline="") as infile, open(
        out_file, "w", encoding="utf-8"
    ) as outfile:
        in_reader = csv.DictReader(infile)

        global ATTR_WRITER
        ATTR_WRITER = writer(outfile, n_rows, n_cols)

        if IS_MULTIPROC:
            # Parallelize to the number of available cores (not the number of
            # available hyper threads). ``psutil`` is the only standard Python
            # package that can provide this measure (with logical=False).
            # Testing revealed wall times to be quite close to using all logical
            # CPUs, with better overall system responsiveness and less thermal
            # throttling in this scenario.
            # Additionally, as Python's threading uses a GIL, it is unsuitable
            # for this task. Use its multiprocessing intsead. However,
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


def enrich_by_col(
    in_file: str,
    out_file: str,
    enrich_col_fn: Callable,
    enrichment_args: Tuple,
) -> None:
    """
    This function enriches a dataset, using an enrichment function that enriches
    column by column and other enrichment objects, and then produces the
    attributes.

    :param in_file: The filepath of the csv formatted original dataset or the
        dataset exported from Watchful. This latter will be the former appended
        with the Watchful columns "Hints" and "HandLabels". It follows that
        these columns are reserved for Watchful and should not be present in the
        original dataset.
    :type in_file: str
    :param out_file: The filepath where the enriched attributes in Watchful
        custom format for ingestion by Watchful application are written to.
    :type out_file: str
    :param enrich_col_fn: The user custom function for enriching every column of
        the dataset.
    :type enrich_col_fn: Callable
    :param enrichment_args: The additional enrichment objects to perform the
        data enrichment.
    :type enrichment_args: Tuple
    """

    with open(in_file, encoding="utf-8", newline="") as infile:
        in_reader = csv.reader(infile)
        col_names = next(in_reader)
        n_cols = len(col_names)
        n_rows = None
        for n_rows, _ in enumerate(in_reader, 1):
            pass

    with open(out_file, "w", encoding="utf-8") as outfile:

        def __row_reader_to_col_reader(col_names, in_file):
            def __get_col(col_name, in_reader):
                return col_name, map(lambda row: row[col_name], in_reader)

            for col_name in col_names:
                with open(in_file, encoding="utf-8", newline="") as infile:
                    in_reader = csv.DictReader(infile)
                    yield __get_col(col_name, in_reader)

        def __enriched_cols_to_enriched_rows(enriched_cols, n_rows):
            def __get_row(i):
                return map(lambda enriched_col: enriched_col[i], enriched_cols)

            for i in range(n_rows):
                yield __get_row(i)

        in_reader = __row_reader_to_col_reader(col_names, in_file)

        global ATTR_WRITER
        ATTR_WRITER = writer(outfile, n_rows, n_cols)

        if IS_MULTIPROC:
            # Parallelize to the number of available cores (not the number of
            # available hyper threads). ``psutil`` is the only standard Python
            # package that can provide this measure (with logical=False).
            # Testing revealed wall times to be quite close to using all logical
            # CPUs, with better overall system responsiveness and less thermal
            # throttling in this scenario.
            # Additionally, as Python's threading uses a GIL, it is unsuitable
            # for this task. Use its multiprocessing intsead. However,
            # multiprocessing uses pickle and is unable to send functions across
            # process boundaries, hence the global variable set by the
            # initializer.
            with Pool(
                initializer=init_args,
                initargs=enrichment_args,
                processes=psutil.cpu_count(logical=False),
            ) as pool:
                for enriched_row in __enriched_cols_to_enriched_rows(
                    # Put into memory for speed, but may need to trade-off speed
                    # for memory when the dataset is large, by storing into
                    # persistent storage.
                    list(
                        pool.imap(
                            func=enrich_col_fn,
                            iterable=in_reader,
                            chunksize=MULTIPROC_CHUNKSIZE,
                        )
                    ),
                    n_rows,
                ):
                    proc_enriched_row(enriched_row)
        else:
            init_args(*enrichment_args)
            for enriched_row in __enriched_cols_to_enriched_rows(
                list(map(enrich_col_fn, in_reader)), n_rows
            ):
                proc_enriched_row(enriched_row)

        del ATTR_WRITER


def proc_enriched_row(enriched_row: List[enricher.EnrichedCell]) -> None:
    """
    This function is iterated over every enriched row. Optionally, you may add
    code if you wish to do something auxiliary with every row.

    :param enriched_row: A list of enriched cells.
    :type enriched_row: List[enricher.EnrichedCell]
    """

    # Do not edit this code.
    for enriched_cell in enriched_row:
        proc_enriched_cell(enriched_cell)

    # Edit the code below if you wish to do something auxiliary with every row.
    # Here we simply print to stdout at the end of every `enriched_row`.
    # print(f'{"*" * 20} end of row {"*" * 20}')


def proc_enriched_cell(enriched_cell: enricher.EnrichedCell) -> None:
    """
    This function is iterated over every enriched cell. Optionally, you may add
    code if you wish to do something auxiliary with every cell.

    :param enriched_cell: An enriched cell.
    :type enriched_cell: enricher.EnrichedCell
    """

    # Do not edit this code.
    ATTR_WRITER(enriched_cell)

    # Edit the code below if you wish to do something auxiliary with every cell.
    # Here we simply print every ``enriched_cell`` to stdout.
    # print("Enriched cell: ")
    # pprint(enriched_cell)


def get_vars_for_enrich_row_with_attribute_data(
    attr_names: str,
    attr_filepath: str,
) -> Tuple[Callable, List[str], csv.reader]:
    """
    This function takes in a comma-delimited string of attribute names and the
    csv attributes filepath. It returns the attribute names as a list, the csv
    attribute reader, and a function that takes in a full row of attributes and
    returns the desired attributes.

    :param attr_names: The comma-delimited attribute names.
    :type attr_names: str
    :param attr_filepath: The attributes csv filepath.
    :type attr_filepath: str
    :return: The list of attribute names, csv attribute reader and a function
        that takes in a full row of attributes and returns the desired
        attributes.
    :rtype: Tuple[Callable, List[str], csv.reader]
    """
    f = open(  # pylint: disable=consider-using-with
        attr_filepath, encoding="utf-8", newline=""
    )
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

    return get_attr_row, attr_name_list, attr_reader


@enricher.set_enrich_fn_order()
def enrich_row_with_attribute_data(
    row: Dict[Optional[str], Optional[str]],
) -> List[enricher.EnrichedCell]:
    """
    This function extracts the attributes from a row of an attributes file.
    Attributes are associated to the entire text in each named cell of the input
    dataset row. The entire text in each cell of the input dataset row is
    identified by its byte start index and byte end index.

    :param row: The dictionary of named cell values in the row.
    :type row: Dict[Optional[str], Optional[str]]
    :return: The list of enriched cell values in the row.
    :rtype: List[enricher.EnrichedCell]
    """

    assert (
        None not in row
    ), "Dataset error: there is at least one unused cell value!"
    row_values = row.values()
    assert (
        None not in row_values
    ), "Dataset error: there is at least one absent cell value!"

    get_attr_row_fn = ENRICHMENT_ARGS[0]
    attr_names = ENRICHMENT_ARGS[1]
    attr_row_reader = ENRICHMENT_ARGS[2]

    attr_row = get_attr_row_fn(next(attr_row_reader))
    enriched_row = []

    for cell in row_values:
        # Just one span for example-level attributes.
        span = [(0, len(str(cell)))]

        # ``enriched_cell`` is to be appended over the len of attr_names.
        enriched_cell = []

        for attr_name, attr_val in zip(attr_names, attr_row):
            # This is the attribute representation per input dataset cell as
            # required by Watchful application interface. Watchful application
            # has a restriction of uppercase characters and maximum 6 characters
            # on attr_representn[2].
            # attr_representn = (
            #     span, {attr_name: [attr_val]},
            #     f"EX{attr_name.replace("_", "").upper()}"
            # )
            # attr_representn = (
            #     span, {attr_name: [attr_val]},
            #     "SOME_GENERAL_INFO_ABOUT_ALL_ATTRIBUTES"
            # )
            attr_representn = (span, {attr_name: [attr_val]})
            enriched_cell.append(attr_representn)

        # Another possible representation.
        # attr_dict = {}
        # for attr_name, attr_val in zip(attr_names, attr_row):
        #     attr_dict[attr_name] = [attr_val]
        # attr_representn = (span, attr_dict, "EXATTR")
        # enriched_cell.append(attr_representn)

        enriched_cell.append((span, {}, "EX"))
        adjust_span_offsets_from_char_to_byte(cell, enriched_cell)

        enriched_row.append(enriched_cell)

    return enriched_row


def validate_attribute_names(attr_names: str, attr_filepath: str) -> bool:
    """
    This function checks that all attribute names are present in the attribute
    file. It returns False as soon as an attribute name is absent, or True when
    all attribute names match.

    :param attr_names: The comma-delimited attribute names.
    :type attr_names: str
    :param attr_filepath: The attributes filepath.
    :type attr_filepath: str
    :return: The boolean indicating if all the attribute names are present in
        the attributes file.
    :rtype: bool
    """

    with open(attr_filepath, encoding="utf-8", newline="") as f:
        attr_row_reader = csv.reader(f)
        attr_names_all = next(attr_row_reader)

    for attr_name in attr_names.split(","):
        if attr_name not in attr_names_all:
            return False

    return True


def atterize_values_in_cell(
    cell: str,
    attribute_name: str,
    values: List[re.Pattern],
) -> enricher.EnrichedCell:
    """
    This is a helper function to ``create_attribute_for_values()`` for finding
    the spans for each value in ``values``.

    :param cell: The original cell.
    :type cell: str
    :param attribute_name: The attribute name.
    :type attribute_name: str
    :return: The enriched cell.
    :rtype: enricher.EnrichedCell
    """

    cell = str(cell)
    matches = [
        [(m.start(), m.end()) for m in re.finditer(pattern, cell)]
        for pattern in values
    ]
    spans = list(merge(*matches))
    return [(spans, {}, attribute_name)]


def create_attribute_for_values(
    attribute_name: str,
    values: List[re.Pattern],
) -> str:
    """
    This function takes an attribute name and a list of known values to create
    attributes for. The list of values will be looked up in each cell of the
    currently loaded dataset. An attributes file will be prepared to be loaded
    into the Watchful application. It returns the filename of the created
    attribute file, which can be used by the attributes action and function:
    ``api.load_attributes(dataset_id, attribute_filename)``.

    :param attribute_name: The attribute name.
    :type attribute_name: str
    :param values: The list of known values to create attributes for.
    :type values: List[str]
    :return: The used attributes filename.
    :rtype: str
    """

    in_file, out_file, out_filename = get_context(attribute_name)
    enrich(
        in_file,
        out_file,
        enrich_row,
        (atterize_values_in_cell, attribute_name, values),
    )
    return out_filename


def get_context(attribute_filename: str) -> Tuple[str, str, str]:
    """
    This function takes in an attributes filename, finds the current dataset
    file loaded in Watchful and returns the context needed to enrich that
    dataset. This context includes the filename of the file used by the
    attributes action and function:
    ``client.load_attributes(dataset_id, attribute_filename)``.

    :param attribute_filename: The input attributes filename.
    :type attribute_filename: str
    :return: The dataset filepath, used attributes filepath and used attributes
        filename.
    :rtype: Tuple[str, str, str]
    """

    summary = client.get()
    attrs_dir = os.path.join(summary["watchful_home"], "datasets", "attrs")
    os.makedirs(attrs_dir, exist_ok=True)
    _, _, in_file = get_dataset_id_dir_filepath(summary)
    in_filename = os.path.basename(in_file)
    out_file = os.path.join(
        attrs_dir, f"{in_filename}_{attribute_filename}.attrs"
    )
    out_filename = os.path.basename(out_file)
    return in_file, out_file, out_filename


def get_dataset_id_dir_filepath(
    summary: Dict,
    in_file: Optional[str] = "",
    is_local: Optional[bool] = True,
) -> Tuple[str, str, str]:
    """
    This function returns the id, directory and filepath of the currently opened
    dataset.

    :param summary: The dictionary of the HTTP response from a connection
        request, defaults to None.
    :type summary: Dict
    :param in_file: The dataset filepath, defaults to "".
    :type in_file: str, optional
    :param is_local: Boolean indicating whether the Watchful application is
        local (otherwise hosted), defaults to True.
    :type is_local: bool, optional
    :return: The id, directory and filepath of the currently opened dataset.
    :rtype: Tuple[str, str, str]
    """

    summary = client._assert_success(summary)
    dataset_id = client.get_dataset_id(summary)
    datasets_dir = client.get_datasets_dir(summary, is_local)

    if in_file != "":
        # Check that ``in_file`` exists.
        if not os.path.isfile(in_file):
            raise Exception(f"File {in_file} does not exist.")
        dataset_filepath = in_file
    else:
        dataset_filepath = client.get_dataset_filepath(summary, is_local)

    return dataset_id, datasets_dir, dataset_filepath
