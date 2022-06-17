import json
import csv
import argparse
#import pandas as pd
import os
import shutil
import pathlib
import datetime
import numbers
from watchful.api import external, get
from multiprocessing import Pool
import psutil
import re


# Constants for encoding spans into compact strings
base = 64
comp_len = 8

#'0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmno'
#numerals = "".join(map(lambda x: chr(x), range(48, 48+base)))
# better performance when profiled over numbers in the order of 10^6 to 10^7.
numerals = dict(map(lambda ic: (ic[0], chr(ic[1])), enumerate(range(48, 48+base))))

#"#$%&'()*"
#compressed = "".join(map(lambda x: chr(x), range(35, 35+comp_len)))
# similar to the suggestion above for numerals.
compressed = dict(map(lambda ic: (ic[0], chr(ic[1])), enumerate(range(35, 35+comp_len))))


def base64(num):
    if num == 0:
        return numerals[0]

    ret = ""
    while num > 0:
        ret += numerals[num % base]
        num = num // base
    return ret[::-1]

"""
Takes a collection of numbers and returns a collection with strings representing
those numbers in base64.

Additional compression is done by concatenating consecutive base64 strings of
the same length. This compressed encoding is detected by inspecting the first
character in the ASCII range 35 inclusive to 42 inclusive. The rest of the string should be partitioned
into their own strings with length ascii_code(s[0]) - 34. For example:
* String: "#1234" represents "1,2,3,4"
* String: "$1234" represents "12,34"
* String: "&1234" represents "1234", (which is never compressed since the value is smaller than the compressed)
(compression will not be done for a base64 encoded number if it is preceded or suceeded by a base64 encoded number of a different length)
Here are the ascii codes for the prefixes in the examples above:
* "#" => 35
* "$" => 36
* "&" => 38
The range 35 inclusive to 42 inclusive was chosen because it contains characters that do not need to be escaped in JSON, nor does
the range contain comma (','), which is used as a delimiter to concat all of the strings.
"""
def base64str(coll):
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

# decode spans to contig_spans/2 with [gap_len,span_len,...,gap_len_N,span_len_N]
# Takes a list of (start, end)
def contig_spans(spans):
	contig = []
	offset = 0
	for (a, b) in spans:
		contig.append(a - offset)
		contig.append(b - a)
		offset = b
	return contig



"""
Takes the output file and the number of rows & columns of the original dataset.
Returns a write function that takes all of the attributes for a
cell in the original dataset, where a cell is a single row and column pair.
The cells attributes should be in this shape:
  [(spans: Vec<(int, int)>, attr_vals: Map<String, Vec<Any>>, name: Option<String>)
   ..
  ]
* `spans` is a sorted vector of span [start, end) in the cell.
* `attr_vals` is a map from attribute name to values for the `spans`. None means that
  the attribute has no value for that token defined by its span.
* `name` is an optional parameter which can be used to give a name to the spans, where
  the attribute value of that name is the content of the spans themselves. Examples
  of this is: sentences, noun_chunks, tokens or collage_names.
"""
def writer(out, n_rows, n_cols):

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

		# For each span data tuple for the cell with the span and attribute values for those span, do:
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
						raise Exception("Attribute value must be a string, None or a number. Was: " + val)
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
				assert len(span) == len(vals), "Must be the same amount of spans as attribute values."
				# Not base64 the attributes to save space since there aren't that many of them.
				span_val.append(attrs[attr])
				span_val.append(base64str([values[attr][val] if val else 0 for val in vals]))

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



"""
Extracts attributes from a SpaCy document.
Attributes are associated to substrings, being tokens, entities, sentences or noun chunks.
Every Substring is identified by its character start index and character end index.
"""
def spacy_atterize(doc):
	# Return value
	cell = []

	## Token level attributes
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

	cell.append((tok_spans, {"pos":pos_vals,"tag":tag_vals,"lemma":lem_vals,"case":case_vals}, "TOKS"))

	# Entities
	ent_spans = []
	ent_vals = []
	for ent in doc.ents:
		ent_spans.append((ent.start_char, ent.end_char))
		ent_vals.append(ent.label_)
	cell.append((ent_spans, {"entity":ent_vals}))

	# Sentences and noun chunks
	cell.append(([(sent.start_char, sent.end_char) for sent in doc.sents], {}, "SENTS"))
	cell.append(([(chunk.start_char, chunk.end_char) for chunk in doc.noun_chunks], {}, "NOUNCH"))

	return cell

"""
Initializes a per-process context with the user function that will
be used in the imap.

This is not necessarily thread-safe but is multiprocess-safe.
"""
def initproc(userfn, user_fn_args):
	global user_fn_global
	global user_fn_args_global
	user_fn_global = userfn
	user_fn_args_global = user_fn_args

"""
Processes one row.  Takes an input row and returns an output row.

The user_fn_global must have previously been set.
"""
def procrow(row):
	global user_fn_global
	global user_fn_args_global
	cells = []
	for raw_cell in row:
		raw_cell = str(raw_cell)
		cell = user_fn_global(raw_cell, *user_fn_args_global)
		# Adjust all spans from character offsets to byte offsets, since Watchful's
		# attributes API takes byte offsets and spacy provides us with character offsets.
		byte_offsets = {}
		byte_offset = 0
		for char_offset, ch in enumerate(raw_cell):
			byte_offsets[char_offset] = byte_offset
			byte_offset += len(ch.encode('utf-8'))
		byte_offsets[len(raw_cell)] = byte_offset

		for context in cell:
			spans = context[0]
			for i in range(len(spans)):
				(start, end) = spans[i]
				spans[i] = (byte_offsets[start], byte_offsets[end])

		cells.append(cell)
	return cells

"""
Usage: enrich( in_file="dataset.csv", out_file="dataset.attrs", user_fn=lambda cell: spacy_atterize(nlp(cell)) )
"dataset.csv": Standard csv format
"dataset.attrs": Watchful custom format for watchful client ingestion
user_fn: user custom function
"""
def enrich(in_file, out_file, user_fn, *user_fn_args):

	in_reader = csv.reader(open(in_file, "r"))
	n_cols = len(next(in_reader))
	for n_rows, _ in enumerate(in_reader, 1):
	    pass

	in_reader = csv.reader(open(in_file, "r"))
	next(in_reader)

	with open(out_file, "w") as outfile:
		w = writer(outfile, n_rows, n_cols)

		# Parallelize to the number of available cores (not the number
		# of available hyper threads).  psutil is the only standard
		# Python package that can provide this measure (with
		# logical=False).  Testing revealed wall times to be quite close
		# to using all logical CPUs, with better overall system responsiveness
		# and less thermal throttling in this scenario.
		#
		# Additionally, as Python's threading uses a GIL, it is unsuitable
		# for this task.  Use its multiprocessing intsead.  However,
		# multiprocessing uses pickle and is unable to send functions across
		# process boundaries, hence the global variable set by the initializer.
		with Pool(initializer=initproc, initargs=[user_fn, user_fn_args],
			  processes=psutil.cpu_count(logical=False)) as pool:
			for cells in pool.imap(procrow, in_reader, chunksize=500):
				for cell in cells:
					w(cell)

"""
	Extracts attributes from an attributes file.
	Attributes are associated to the entire text in each input dataset cell.
	The entire text in each input dataset cell is identified by its character start index and character end index.
"""
def attrs_to_wf_format(in_cell, attr_names, attr_row):

	#just one span for example-level attributes
	spans = [(0, len(str(in_cell)))]

	#appended over the size of attr_names
	cell_attrs = []

	for attr_name, attr_val in zip(attr_names, attr_row):
		#this is the attribute representation per input dataset cell as required by watchful client interface
		#watchful client has a 6 uppercase characters restriction on tuple[2]
		#attr_representn = (spans, {attr_name: [attr_val]}, "EX{}".format(attr_name.replace("_", "").upper()))
		attr_representn = (spans, {attr_name: [attr_val]})
		cell_attrs.append(attr_representn)

	# #another representation
	# attr_dict = {}
	# for attr_name, attr_val in zip(attr_names, attr_row):
	# 	attr_dict[attr_name] = [attr_val]
	# attr_representn = (spans, attr_dict, "EXATTR")
	# cell_attrs.append(attr_representn)

	cell_attrs.append((spans, {}, "EX"))

	return cell_attrs



"""
	usage: enrich_with_attr_file(
		in_file="dataset.csv",
		out_file="dataset.attrs",
		user_fn=lambda cell: attrs_to_wf_format(cell),
		attr_file="attrs.csv",
		attr_names="attr_name1,attr_name2,..."
	)
	arguments:
		in_file: csv formatted file
		out_file: Watchful custom formatted file for watchful client ingestion
		user_fn: user custom function
		attr_file: csv formatted attributes file created from an external pipeline
		attr_names: comma delimited attribute column names to be used from in_file, None for all columns
"""
def enrich_with_attr_file(in_file, out_file, user_fn, attr_file, attr_names=None):

	in_reader = csv.reader(open(in_file, "r"))
	n_cols = len(next(in_reader))
	for n_rows, _ in enumerate(in_reader, 1):
	    pass

	in_reader = csv.reader(open(in_file, "r"))
	next(in_reader)

	attrs_reader = csv.reader(open(attr_file, "r"))
	attr_names_all = next(attrs_reader)

	if not attr_names:
		attr_names = attr_names_all

		def __get_attr_row(attr_row_all):
			return attr_row_all
	else:
		attr_names = attr_names.split(",")
		attr_locs = list(map(attr_names_all.index, attr_names))

		def __get_attr_row(attr_row_all):
			attr_row = list(map(lambda i: attr_row_all[i], attr_locs))
			return attr_row

	with open(out_file, "w") as outfile:
		w = writer(outfile, n_rows, n_cols)

		for in_row, attr_row_all in zip(in_reader, attrs_reader):
			attr_row = __get_attr_row(attr_row_all)

			for cell_attr_val in map(lambda in_cell: user_fn(in_cell, attr_names, attr_row), in_row):
				w(cell_attr_val)

"""
   Helper function to `create_attribute_for_values()` for finding the spans for each value.
"""
def atterize_values_in_cell(cell, name, values):
	from heapq import merge
	cell = str(cell)
	matches = [[(m.start(), m.end()) for m in re.finditer(pattern, cell)] for pattern in values]
	spans = list(merge(*matches))
	return [(spans, {}, name)]

"""
   Takes an attribute name and a list of known values to create attributes for.
   The list of values will be looked up in each cell of the currently loaded dataset.
   An attributes file will be prepared to be loaded in watchful.
   Returns the filename of the attribute file created, which can be used by the attributes
   action and function: watchful.load_attributes(dataset_id, attribute_filename)
"""
def create_attribute_for_values(attribute_name, values):
	in_file, out_file, out_filename = get_context(attribute_name)
	enrich(in_file, out_file, atterize_values_in_cell, attribute_name, values)
	return out_filename


"""
   Given a filename, finds the current dataset file loaded in Watchful and produces
   the context needed to enrich that dataset.
   Also returns the filename of the file used for the Attributes action.
   See: watchful.load_attributes(dataset_id, attribute_filename)
"""
def get_context(attribute_filename):
	summary = get()
	attrs_dir = os.path.join(summary['watchful_home'], "datasets", "attrs")
	os.makedirs(attrs_dir, exist_ok=True)
	in_file = get_dataset_file(summary)
	in_filename = os.path.basename(in_file)
	out_file = os.path.join(attrs_dir, "{}_{}.attrs".format(in_filename, attribute_filename))
	out_filename = os.path.basename(out_file)
	return in_file, out_file, out_filename


# check if all attributes names are present in the attribute file
# returns as soon as an attribute name is absent, or when all attribute names match
def validate_attr_names(attr_names, attr_file):

	attrs_reader = csv.reader(open(attr_file, "r"))
	attr_names_all = next(attrs_reader)

	for attr_name in attr_names.split(","):
		if attr_name not in attr_names_all:
			return False

	return True

def get_dataset_file(summary):
	user_home_path = os.path.expanduser("~")
	dataset_id = ""
	if "error_msg" in summary and summary["error_msg"]:
		print("Error getting dataset id")
		exit(1)
	else:
		# dataset_ids should only hold zero or one dataset id
		dataset_ids = summary["datasets"]
		if len(dataset_ids) == 0:
			print("No dataset is currently opened in Watchful.")
			exit(1)
		else:
			dataset_id = dataset_ids[0]

	if 'watchful_home' in summary:
		watchful_home = summary['watchful_home']
	else:
		watchful_home = os.path.join(user_home_path, 'watchful')
	datasets_dir = os.path.join(watchful_home, "datasets")

	dataset_ref_path = os.path.join(datasets_dir, "refs", dataset_id)
	# check that dataset ref exists
	if not os.path.isfile(dataset_ref_path):
		print("File <{}> does not exist.".format(dataset_ref_path))
		exit(1)

	dataset_ref = open(dataset_ref_path).readline()
	dataset_file = os.path.join(datasets_dir, "raw", dataset_ref)
	# check that dataset_file exists
	if not os.path.isfile(dataset_file):
		print("File <{}> does not exist.".format(dataset_file))
		exit(1)

	return dataset_file

if __name__ == "__main__":

	parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

	# use the dataset currently opened in watchful if no dataset file is provided
	parser.add_argument("--in_file", type=str, default="",
		help="optional original csv dataset filepath, if not given use the current dataset opened in watchful")
	# specific formatting of attributes for integration to Watchful
	parser.add_argument("--out_file", type=str, default="",
		help="optional output attribute filepath; if given must end with \".attrs\" extension.")
	# create the initial spacy attributes if no attribute file is provided
	parser.add_argument("--attr_file", type=str, default="",
		help="optional input csv attribute filepath, if not given create the initial spacy attributes")
	# columns in the attr_file csv that to use as attributes; use all attributes if not provided
	parser.add_argument("--attr_names", type=str, default="",
		help="optional comma delimited string of attribute names to be used, if not given use all attribute names")
	# use port 9001 if no port is provided
	parser.add_argument("--wf_port", type=str, default="9001",
		help="optional string port number running Watchful, if not given use \"9001\"")

	args = parser.parse_args()

	external(port=args.wf_port)

	if args.in_file:
		# check that in_file exists
		if not os.path.isfile(args.in_file):
			print("File <{}> does not exist.".format(args.in_file))
			exit(1)
	else:
		args.in_file = get_dataset_file(get())

	if args.out_file:
		# check that out_file has .attrs extension
		try:
			is_zero = args.out_file[::-1].index(".attrs"[::-1]) == 0
			if is_zero:
				del_out_file = False
				# check that out_file's directory exists
				out_file_dir = pathlib.Path(args.out_file).parent
				if not os.path.isdir(out_file_dir):
					print("Directory <{}> does not exist.".format(out_file_dir))
					exit(1)
			else:
				print("out_file <{}> must end with \".attrs\" extension.".format(args.out_file))
				exit(1)
		except Exception as err_msg:
			print(err_msg)
			print("out_file <{}> must end with \".attrs\" extension.".format(args.out_file))
			exit(1)
	else:
		del_out_file = True
		# create a temporary out_file and mark it for deletion
		out_dirs_file = args.in_file.split(os.path.sep)
		args.out_file = os.path.join(os.path.sep.join(out_dirs_file[:-1]), "{}.attrs".format(out_dirs_file[-1].split(".")[0]))



	# enrich with attributes created from an external pipeline
	if args.attr_file:
		user_fn = lambda in_cell, attr_names, attr_row: attrs_to_wf_format(in_cell, attr_names, attr_row)

		# enrich with all attributes
		if not args.attr_names:
			print("Enriching <{}> using all attributes from <{}> ...".format(args.in_file, args.attr_file))
			enrich_with_attr_file(args.in_file, args.out_file, user_fn, args.attr_file)
			if not del_out_file:
				print("Wrote attributes to <{}>.".format(args.out_file))

		# enrich with specified attributes
		else:
			val_success = validate_attr_names(args.attr_names, args.attr_file)

			if val_success:
				print("Enriching <{}> using <{}> attributes from <{}> ...".format(args.in_file, args.attr_names, args.attr_file))
				enrich_with_attr_file(args.in_file, args.out_file, user_fn, args.attr_file, args.attr_names)
				if not del_out_file:
					print("Wrote attributes to <{}>.".format(args.out_file))
			else:
				print("At least one of your attribute names in <{}> do not match those in the attribute input file <{}>.".format(args.attr_names, args.attr_file))
				exit(1)

	# do spacy
	else:
		import spacy
		#nlp = spacy.load("en_core_web_sm", exclude=["parser"])
		nlp = spacy.load("en_core_web_sm")
		nlp.enable_pipe("senter")
		print("Enriching: ", args.in_file, "...")
		user_fn = lambda cell: spacy_atterize(nlp(cell))
		enrich(args.in_file, args.out_file, user_fn)
		if not del_out_file:
			print("Wrote attributes to: ", args.out_file)



	# copy created attributes output file to watchful home attributes directory
	# format timestamp as "yyyy-mm-dd_hh-mm-ss-ssssss"
	timestamp = str(datetime.datetime.now()).replace(" ", "_").replace(":", "-").replace(".", "-")
	dest_file = "__{}.".format(timestamp).join(args.out_file.split(os.path.sep)[-1].split("."))
	orig_dest_files = args.out_file, os.path.join(datasets_dir, "attrs", dest_file)

	if del_out_file:
		try:
			shutil.copyfile(*orig_dest_files)
		except Exception as err_msg:
			print(err_msg)
			print("Error copying attribute output file from <{}> to <{}>.".format(*orig_dest_files))
			exit(1)

		try:
			os.remove(args.out_file)
		except Exception as err_msg:
			print(err_msg)
			print("Error removing temporary attribute output file from <{}>.".format(args.out_file))
			exit(1)

		print("Wrote attributes to <{}>.".format(orig_dest_files[-1]))
