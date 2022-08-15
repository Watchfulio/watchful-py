"""
This script is run on the Python command line or on the equivalent `main`
function to execute data enrichment.
"""
################################################################################


import argparse
import datetime
import os
import pathlib
import shutil
import sys

try:
    from watchful.client import external, get, load_attributes
    from watchful.attributes import set_multiprocessing, enrich, enrich_row, \
        get_vars_for_enrich_row_with_attribute_data, \
        enrich_row_with_attribute_data, load_spacy, spacy_atterize, \
        spacy_atterize_fn, load_flair, flair_atterize, flair_atterize_fn, \
        validate_attr_names, get_dataset_dir_file_id
    from watchful.enricher import Enricher
except (ImportError, ModuleNotFoundError):
    from client import external, get, load_attributes
    from attributes import set_multiprocessing, enrich, enrich_row, \
        get_vars_for_enrich_row_with_attribute_data, \
        enrich_row_with_attribute_data, load_spacy, spacy_atterize, \
        spacy_atterize_fn, load_flair, flair_atterize, flair_atterize_fn, \
        validate_attr_names, get_dataset_dir_file_id
    from enricher import Enricher


def enrich_dataset(custom_enricher_cls: Enricher, args=[]) -> None:
    """
    This is the function to use for performing custom data enrichment. Custom
    data enrichment variables and functions defined in custom_enricher_cls
    are used to perform the data enrichment.
    """
    set_multiprocessing(False)
    custom_enricher = custom_enricher_cls()

    main(args, custom_enricher)


def main(args=None, custom_enricher=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # The dataset file; if not provided use the dataset currently opened in
    # Watchful.
    parser.add_argument(
        "--in_file", type=str, default="",
        help="optional original csv dataset filepath, if not given use the \
            current dataset opened in watchfu."
    )
    # This is the attributes output file that is specifically formatted for
    # integration to Watchful.
    parser.add_argument(
        "--out_file", type=str, default="",
        help="optional output attribute filepath; if given must end with \
            \".attrs\" extension."
    )
    # Create SpaCy attributes if no custom attribute file is provided.
    parser.add_argument(
        "--attr_file", type=str, default="",
        help="optional input csv attribute filepath, if not given create the \
            initial spacy attributes."
    )
    # Columns in the `attr_file` csv file to use as attributes; use all
    # attributes if not provided.
    parser.add_argument(
        "--attr_names", type=str, default="",
        help="optional comma delimited string of attribute names to be used, \
            if not given use all attribute names."
    )
    # The port to use; if not provided use port 9001.
    parser.add_argument(
        "--wf_port", type=str, default="9001",
        help="optional string port number running Watchful, if not given use \
            \"9001\"."
    )
    parser.add_argument(
        "--standard_nlp", type=str, default="spacy",
        help="Standard NLP to use, currently \"spacy\" and \"flair\" are \
            available; \"spacy\" if unspecified."
    )
    parser.add_argument(
        "--multiprocessing", action="store_true",
        help="Explicit use of multiprocessing on available physical cpu \
            cores; no explicit use if unspecified."
    )
    args = parser.parse_args(args=args)


    set_multiprocessing(args.multiprocessing)


    external(port=args.wf_port)


    datasets_dir, args.in_file, dataset_id = get_dataset_dir_file_id(
        get(), args.in_file)


    if args.out_file:
        # Check that `out_file` has ".attrs" extension.
        try:
            is_zero = args.out_file[::-1].index(".attrs"[::-1]) == 0
            if is_zero:
                del_out_file = False
                # check that out_file's directory exists
                out_file_dir = pathlib.Path(args.out_file).parent
                if not os.path.isdir(out_file_dir):
                    print("Directory <{}> does not exist.".format(out_file_dir))
                    sys.exit(1)
            else:
                print("out_file <{}> must end with \".attrs\" extension.".format(
                    args.out_file))
                sys.exit(1)
        except Exception as err_msg:
            print(err_msg)
            print("out_file <{}> must end with \".attrs\" extension.".format(
                args.out_file))
            sys.exit(1)
    else:
        del_out_file = True
        # Create a temporary `out_file` and mark it for deletion.
        out_dirs_file = args.in_file.split(os.path.sep)
        args.out_file = os.path.join(
            os.path.sep.join(out_dirs_file[:-1]),
            "{}.attrs".format(out_dirs_file[-1].split(".")[0])
        )


    # Enrich with attributes from a csv file, that is, already created from an
    # external pipeline.
    if args.attr_file:

        # Enrich with all attributes.
        if not args.attr_names:
            print("Enriching <{}> using all attributes from <{}> ...".format(
                args.in_file, args.attr_file))
            enrich(
                args.in_file,
                args.out_file,
                enrich_row_with_attribute_data,
                get_vars_for_enrich_row_with_attribute_data(
                    args.attr_names, args.attr_file
                )
            )
            if not del_out_file:
                print("Wrote attributes to <{}>.".format(args.out_file))

        # Enrich with specified attributes.
        else:
            val_success = validate_attr_names(args.attr_names, args.attr_file)

            if val_success:
                print("Enriching <{}> using <{}> attributes from <{}> ..."
                    .format(args.in_file, args.attr_names, args.attr_file))
                enrich(
                    args.in_file,
                    args.out_file,
                    enrich_row_with_attribute_data,
                    get_vars_for_enrich_row_with_attribute_data(
                        args.attr_names, args.attr_file
                    )
                )
                if not del_out_file:
                    print("Wrote attributes to <{}>.".format(args.out_file))
            else:
                print(
                    "At least one of your attribute names in <{}> do not match "
                    "those in the attribute input file <{}>.".format(
                        args.attr_names, args.attr_file))
                sys.exit(1)


    # Do data enrichment from standard NLPs.
    elif custom_enricher is None:

        # Do SpaCy.
        if args.standard_nlp == "spacy":
            nlp = load_spacy()
            # Want to know what pipes are used? Uncomment these:
            #import pprint
            #pprint.PrettyPrinter(indent=4).pprint(nlp.analyze_pipes())

            # `enrich_row` is the user custom function for enriching every row
            # of the dataset. `spacy_atterize_fn`, `spacy_atterize` and `nlp`
            # are the additional user variables to perform the data enrichment.
            print("Using: ", args.standard_nlp, "...")
            print("Enriching: ", args.in_file, "...")
            enrich(
                args.in_file, args.out_file,
                enrich_row,
                (spacy_atterize_fn, spacy_atterize, nlp)
            )
            if not del_out_file:
                print("Wrote attributes to: ", args.out_file)

        # Do Flair.
        elif args.standard_nlp == "flair":
            # `enrich_row` is the user custom function for enriching every row
            # of the dataset. `flair_atterize_fn`, `flair_atterize` and
            # `*load_flair()` are the additional user variables to perform the
            # data enrichment.
            print("Using: ", args.standard_nlp, "...")
            print("Enriching: ", args.in_file, "...")
            enrich(
                args.in_file, args.out_file,
                enrich_row,
                (flair_atterize_fn, flair_atterize, *load_flair())
            )
            if not del_out_file:
                print("Wrote attributes to: ", args.out_file)

        else:
            print(
                "No such standard NLP implemented:",
                args.standard_nlp,
                "\nNo enrichment performed."
            )
            sys.exit(1)


    # Do custom data enrichment.
    else:
        # Perform custom data enrichment. Custom data enrichment variables and
        # functions in custom_enricher are used to perform the data enrichment.
        print("Using your custom enricher...")
        print("Enriching: ", args.in_file, "...")
        enrich(
            args.in_file, args.out_file,
            custom_enricher.enrich_row,
            custom_enricher.enrichment_args
        )
        if not del_out_file:
            print("Wrote attributes to: ", args.out_file)


    # Copy created attributes output file to watchful home attributes directory.
    # Format the timestamp as "yyyy-mm-dd_hh-mm-ss-ssssss".
    # Use the full timestamp for completeness; though it's reasonable
    # "yyyy-mm-dd_hh-mm-ss" should work uniquely too.
    # This timestamp format is:
    #     - usable for pulling out the latest attributes
    #     - usable for pulling out a time-series progression of attributes
    #     - consistent here and in kubeflow pipelines integration
    # The final filename format is "filename__yyyy-mm-dd_hh-mm-ss-ssssss.attrs".
    timestamp = str(
        datetime.datetime.now()
    ).replace(" ", "_").replace(":", "-").replace(".", "-")
    dest_file = "__{}.".format(timestamp).join(
        args.out_file.split(os.path.sep)[-1].split(".")
    )
    orig_dest_files = args.out_file, os.path.join(
        datasets_dir, "attrs", dest_file
    )

    try:
        os.makedirs(os.path.dirname(orig_dest_files[1]), exist_ok=True)
        shutil.copyfile(*orig_dest_files)
    except Exception as err_msg:
        print(err_msg)
        print("Error copying attribute output file from <{}> to <{}>.".format(
            *orig_dest_files))
        sys.exit(1)

    if del_out_file:
        try:
            os.remove(args.out_file)
        except Exception as err_msg:
            print(err_msg)
            print("Error removing temporary attribute output file from <{}>."
                "".format(args.out_file))
            sys.exit(1)


    # Load attributes into Watchful.
    """
        Example usage:
            curl -iX POST http://localhost:9001/api \
              --header "Content-Type: application/json" \
              --data '{"verb":"attributes","id":"9570b0b5-4a58-445f-9b51-b434caca2650","file":"attrs_file.attrs"}'
        Arguments:
            port: watchful client port
            id: dataset id
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
