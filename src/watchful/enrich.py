"""
This script is run on the Python command line or on the equivalent `main` function to 
execute data enrichment.
"""

import argparse
import datetime
import os
import pathlib
import shutil
import sys

try:
    from watchful.api import external, get, load_attributes
    from watchful.attributes import attrs_to_wf_format, enrich, enrich_with_attr_file, \
        get_dataset_dir_file_id, load_spacy, spacy_atterize_fn, validate_attr_names
except (ImportError, ModuleNotFoundError):
    from api import external, get, load_attributes
    from attributes import attrs_to_wf_format, enrich, enrich_with_attr_file, \
        get_dataset_dir_file_id, load_spacy, spacy_atterize_fn, validate_attr_names


def main(args=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # use the dataset currently opened in watchful if no dataset file is provided
    parser.add_argument(
        "--in_file", type=str, default="",
        help="optional original csv dataset filepath, if not given use the current \
        dataset opened in watchful"
    )
    # specific formatting of attributes for integration to Watchful
    parser.add_argument(
        "--out_file", type=str, default="",
        help="optional output attribute filepath; if given must end with \".attrs\" \
        extension."
    )
    # create the initial spacy attributes if no attribute file is provided
    parser.add_argument(
        "--attr_file", type=str, default="",
        help="optional input csv attribute filepath, if not given create the initial \
        spacy attributes"
    )
    # columns in the attr_file csv that to use as attributes; use all attributes if 
    # not provided
    parser.add_argument(
        "--attr_names", type=str, default="",
        help="optional comma delimited string of attribute names to be used, if not \
        given use all attribute names"
    )
    # use port 9001 if no port is provided
    parser.add_argument(
        "--wf_port", type=str, default="9001",
        help="optional string port number running Watchful, if not given use \"9001\""
    )

    args = parser.parse_args(args=args)


    external(port=args.wf_port)


    datasets_dir, args.in_file, dataset_id = get_dataset_dir_file_id(get(), args.in_file)


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
                print("out_file <{}> must end with \".attrs\" extension.".format(
                    args.out_file))
                exit(1)
        except Exception as err_msg:
            print(err_msg)
            print("out_file <{}> must end with \".attrs\" extension.".format(
                args.out_file))
            exit(1)
    else:
        del_out_file = True
        # create a temporary out_file and mark it for deletion
        out_dirs_file = args.in_file.split(os.path.sep)
        args.out_file = os.path.join(
            os.path.sep.join(out_dirs_file[:-1]),
            "{}.attrs".format(out_dirs_file[-1].split(".")[0])
        )


    # enrich with attributes created from an external pipeline
    if args.attr_file:
        user_fn = lambda in_cell, attr_names, attr_row: attrs_to_wf_format(
            in_cell, attr_names, attr_row)

        # enrich with all attributes
        if not args.attr_names:
            print("Enriching <{}> using all attributes from <{}> ...".format(
                args.in_file, args.attr_file))
            enrich_with_attr_file(args.in_file, args.out_file, user_fn, args.attr_file)
            if not del_out_file:
                print("Wrote attributes to <{}>.".format(args.out_file))

        # enrich with specified attributes
        else:
            val_success = validate_attr_names(args.attr_names, args.attr_file)

            if val_success:
                print("Enriching <{}> using <{}> attributes from <{}> ...".format(
                    args.in_file, args.attr_names, args.attr_file))
                enrich_with_attr_file(
                    args.in_file, args.out_file, user_fn, args.attr_file, args.attr_names)
                if not del_out_file:
                    print("Wrote attributes to <{}>.".format(args.out_file))
            else:
                print(
                    "At least one of your attribute names in <{}> do not match those in \
                    the attribute input file <{}>.".format(args.attr_names, args.attr_file))
                exit(1)


    # do spacy
    else:
        nlp = load_spacy()
        # Want to know what pipes are used?  Uncomment these:
        #import pprint
        #pprint.PrettyPrinter(indent=4).pprint(nlp.analyze_pipes())
        print("Enriching: ", args.in_file, "...")
        enrich(args.in_file, args.out_file, spacy_atterize_fn, nlp)
        if not del_out_file:
            print("Wrote attributes to: ", args.out_file)


    # copy created attributes output file to watchful home attributes directory
    # format timestamp as "yyyy-mm-dd_hh-mm-ss-ssssss"
    # use the full timestamp for completeness; though it's reasonable "yyyy-mm-dd_hh-mm-ss" 
    # should work uniquely too.
    # this timestamp format is:
    #     - usable for pulling out the latest attributes
    #     - usable for pulling out a time-series progression of attributes
    #     - consistent here and in kubeflow pipelines integration
    # the final filename format is "filename__yyyy-mm-dd_hh-mm-ss-ssssss.attrs"
    # note the delimiters for parsing
    timestamp = str(
        datetime.datetime.now()
    ).replace(" ", "_").replace(":", "-").replace(".", "-")
    dest_file = "__{}.".format(timestamp).join(
        args.out_file.split(os.path.sep)[-1].split(".")
    )
    orig_dest_files = args.out_file, os.path.join(datasets_dir, "attrs", dest_file)

    try:
        os.makedirs(os.path.dirname(orig_dest_files[1]), exist_ok=True)
        shutil.copyfile(*orig_dest_files)
    except Exception as err_msg:
        print(err_msg)
        print("Error copying attribute output file from <{}> to <{}>.".format(
            *orig_dest_files))
        exit(1)

    if del_out_file:
        try:
            os.remove(args.out_file)
        except Exception as err_msg:
            print(err_msg)
            print("Error removing temporary attribute output file from <{}>.".format(
                args.out_file))
            exit(1)


    # load attributes into watchful
    """
        usage:
            curl -iX POST http://localhost:9001/api \
              --header "Content-Type: application/json" \
              --data '{"verb":"attributes","id":"9570b0b5-4a58-445f-9b51-b434caca2650","file":"attrs_file.attrs"}'
        arguments:
            port: watchful client port
            id: dataset id,
            file: attributes file
    """
    load_attrib_res = load_attributes(dataset_id, dest_file)

    msg = "attributes via watchful client port <{}> to dataset id <{}>.".format(
        args.wf_port, dataset_id)
    if "error_msg" in load_attrib_res and load_attrib_res["error_msg"]:
        print("Error ingesting {}".format(msg))
    else:
        print("Ingested {}".format(msg))


if __name__ == "__main__":
    main(sys.argv[1:])
