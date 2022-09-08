"""
This script is run on the Python command line or on the equivalent `main`
function to execute data enrichment. If a custom enricher is used, then the
`enrich_dataset` function is used.
"""
################################################################################


import argparse
import datetime
import os
import pathlib
import shutil
import sys
from typing import List, Type
from uuid import uuid4
from watchful import client, attributes
from watchful.enricher import Enricher


def enrich_dataset(
    custom_enricher_cls: Type[Enricher],
    args: List[str]=None) -> None:
    """
    This is the function to use for performing custom data enrichment. Custom
    data enrichment variables and functions defined in custom_enricher_cls
    are used to perform the data enrichment.
    """
    if args is None:
        args = []
    attributes.set_multiprocessing(False)
    custom_enricher = custom_enricher_cls()

    main(args, custom_enricher)


def main(args: List[str]=None, custom_enricher: Enricher=None) -> None:
    if args is None:
        args = []

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # The dataset file; if not provided use the dataset currently opened in
    # Watchful. Not used in remote enrichment as we will retrieve the dataset
    # from the remote Watchful application.
    parser.add_argument(
        "--in_file", type=str, default="",
        help="optional original csv dataset filepath, if not given use the \
            current dataset opened in watchfu."
    )
    # This is the attributes output file that is specifically formatted for
    # integration with the Watchful application.
    parser.add_argument(
        "--out_file", type=str, default="",
        help="optional output attribute filepath; if given must end with \
            \".attrs\" extension."
    )
    # The attribute file to ingest from, if it is available.
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
    # The host to use; if not provided use localhost.
    parser.add_argument(
        "--wf_host", type=str, default="localhost",
        help="optional string host running Watchful, if not given use \
            \"localhost\"."
    )
    # The port to use; if not provided use port 9001.
    parser.add_argument(
        "--wf_port", type=str, default="9001",
        help="optional string port number running Watchful, if not given use \
            \"9001\"."
    )
    # The out-of-the-box NLP to use if no `attr_file` is provided.
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


    attributes.set_multiprocessing(args.multiprocessing)


    client.external(host=args.wf_host, port=args.wf_port)


    is_local = args.wf_host in ["localhost", "127.0.0.1"]


    summary = client.get()
    project_id = client.get_project_id(summary)
    dataset_id, datasets_dir, args.in_file \
        = attributes.get_dataset_id_dir_filepath(
            summary,
            args.in_file,
            is_local
        )


    # `args.in_file` will still be returned as "" if Watchful application is
    # remote. Therefore, we create a temporary filepath for `args.in_file` to
    # download the original dataset to.
    if not is_local:
        if args.in_file == "":
            user_home_path = os.path.expanduser("~")
            working_dir = os.path.join(user_home_path, "watchful", "working")
            os.makedirs(working_dir, exist_ok=True)
            args.in_file = os.path.join(working_dir, str(uuid4()))
            dataset_export_stream = client.export_dataset()
            with open(args.in_file, "wb") as f:
                for line in dataset_export_stream:
                    f.write(line)
        else:
            print(
                "in_file must be initially \"\" for enrichment to a remote " \
                f"Watchful applcation; got \"{args.out_file}\" instead."
            )
            sys.exit(1)


    if args.out_file:
        # Check that `out_file` has ".attrs" extension.
        try:
            is_ext_attrs = os.path.splitext(args.out_file)[1] == ".attrs"
            if is_ext_attrs:
                del_out_file = False
                # check that out_file's directory exists
                out_file_dir = pathlib.Path(args.out_file).parent
                if not os.path.isdir(out_file_dir):
                    print(f"Directory {out_file_dir} does not exist.")
                    sys.exit(1)
            else:
                print(
                    f"out_file {args.out_file} must end with \".attrs\" " \
                    "extension."
                )
                sys.exit(1)
        except OSError as err_msg:
            print(err_msg)
            print(
                f"out_file {args.out_file} must end with \".attrs\" extension."
            )
            sys.exit(1)
    else:
        del_out_file = True
        # Create a temporary `out_file` and mark it for deletion.
        args.out_file = f"{os.path.splitext(args.in_file)[0]}.attrs"


    # Enrich with attributes from a csv file, that is, already created from an
    # external pipeline.
    if args.attr_file:

        # Enrich with all attributes.
        if not args.attr_names:
            print(
                f"Enriching {args.in_file} using all attributes from " \
                f"{args.attr_file} ..."
            )
            attributes.enrich(
                args.in_file,
                args.out_file,
                attributes.enrich_row_with_attribute_data,
                attributes.get_vars_for_enrich_row_with_attribute_data(
                    args.attr_names, args.attr_file
                )
            )
            if not del_out_file:
                print(f"Wrote attributes to {args.out_file}.")

        # Enrich with specified attributes.
        else:
            val_success = attributes.validate_attr_names(
                args.attr_names, args.attr_file
            )

            if val_success:
                print(
                    f"Enriching {args.in_file} using {args.attr_names} " \
                    f"attributes from {args.attr_file} ..."
                )
                attributes.enrich(
                    args.in_file,
                    args.out_file,
                    attributes.enrich_row_with_attribute_data,
                    attributes.get_vars_for_enrich_row_with_attribute_data(
                        args.attr_names, args.attr_file
                    )
                )
                if not del_out_file:
                    print(f"Wrote attributes to {args.out_file}.")
            else:
                print(
                    f"At least one of your attribute names in " \
                    f"{args.attr_names} do not match those in the attribute "
                    f"input file {args.attr_file}."
                )
                sys.exit(1)


    # Do data enrichment from standard NLPs.
    elif custom_enricher is None:

        # Do SpaCy.
        if args.standard_nlp == "spacy":
            # Want to know what pipes are used? Uncomment these:
            #nlp = attributes.load_spacy()
            #import pprint
            #pprint.PrettyPrinter(indent=4).pprint(nlp.analyze_pipes())

            # `enrich_row` is the user custom function for enriching every row
            # of the dataset. `spacy_atterize_fn`, `spacy_atterize` and
            # `load_spacy()` are the additional user variables to perform the
            # data enrichment.
            print(f"Using {args.standard_nlp} ...")
            print(f"Enriching {args.in_file} ...")
            attributes.enrich(
                args.in_file,
                args.out_file,
                attributes.enrich_row,
                (
                    attributes.spacy_atterize_fn,
                    attributes.spacy_atterize,
                    attributes.load_spacy()
                )
            )
            if not del_out_file:
                print(f"Wrote attributes to {args.out_file}.")

        # Do Flair.
        elif args.standard_nlp == "flair":
            # `enrich_row` is the user custom function for enriching every row
            # of the dataset. `flair_atterize_fn`, `flair_atterize` and
            # `*load_flair()` are the additional user variables to perform the
            # data enrichment.
            print(f"Using {args.standard_nlp} ...")
            print(f"Enriching {args.in_file} ...")
            attributes.enrich(
                args.in_file,
                args.out_file,
                attributes.enrich_row,
                (
                    attributes.flair_atterize_fn,
                    attributes.flair_atterize,
                    *attributes.load_flair()
                )
            )
            if not del_out_file:
                print(f"Wrote attributes to {args.out_file}.")

        else:
            print(
                f"The nlp {args.standard_nlp} is not implemented.\nNo " \
                "enrichment done."
            )
            sys.exit(1)


    # Do custom data enrichment.
    else:
        # Perform custom data enrichment. Custom data enrichment variables and
        # functions in custom_enricher are used to perform the data enrichment.
        print("Using your custom enricher ...")
        print(f"Enriching {args.in_file} ...")
        attributes.enrich(
            args.in_file,
            args.out_file,
            custom_enricher.enrich_row,
            custom_enricher.enrichment_args
        )
        if not del_out_file:
            print(f"Wrote attributes to {args.out_file}.")


    # If Watchful application is remote, delete the downloaded dataset as the
    # data enrichment is completed.
    if not is_local:
        try:
            os.remove(args.in_file)
        except FileNotFoundError as err_msg:
            print(err_msg)
            print(
                f"Error removing downloaded dataset file from {args.in_file}."
            )
            sys.exit(1)


    # Check that the active project and the opened dataset have not changed.
    summary = client.get()
    current_project_id = client.get_project_id(summary)
    if project_id != current_project_id:
        print(
            f"Current project {current_project_id} is different from the " \
            f"enriched project {project_id}!"
        )
        sys.exit(1)
    current_dataset_id, *_ = attributes.get_dataset_id_dir_filepath(
        summary,
        args.in_file,
        is_local
    )
    if dataset_id != current_dataset_id:
        print(
            f"Current dataset {current_dataset_id} is different from the " \
            f"enriched dataset {dataset_id}!"
        )
        sys.exit(1)


    # Format the attributes file timestamp as "yyyy-mm-dd_hh-mm-ss-ssssss".
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
    dest_attr_filename = f"__{timestamp}.".join(
        os.path.basename(args.out_file).split(".")
    )


    # Copy created attributes output file to watchful home attributes directory
    # if the watchful application is local.
    if is_local:
        dest_attr_filepath = os.path.join(
            datasets_dir,
            "attrs",
            dest_attr_filename
        )
        try:
            os.makedirs(os.path.dirname(dest_attr_filepath), exist_ok=True)
            shutil.copyfile(args.out_file, dest_attr_filepath)
        except OSError as err_msg:
            print(err_msg)
            print(
                f"Error copying attribute output file from {args.out_file} " \
                f"to {dest_attr_filepath}."
            )
            sys.exit(1)
    # Upload created attributes output file to Watchful application if it is
    # remote.
    else:
        client.upload_attributes(
            dataset_id,
            dest_attr_filename,
            args.out_file,
            project_id
        )

    if del_out_file:
        try:
            os.remove(args.out_file)
        except FileNotFoundError as err_msg:
            print(err_msg)
            print(
                "Error removing temporary attribute output file from " \
                f"{args.out_file}."
            )
            sys.exit(1)


    # Load attributes filepath into Watchful application.
    # Example usage:
        # curl -iX POST http://localhost:9001/api \
        #   --header "Content-Type: application/json" \
        #   --data '{"verb":"attributes","id":"9570b0b5-4a58-445f-9b51-b434caca2650",
        #         "filepath":"/path/to/attributes_file.attrs"}'
    # Arguments:
    #     id: dataset id
    #     filepath: attributes filepath
    load_attrib_res = client.load_attributes(dataset_id, dest_attr_filename)

    msg = f"attributes via watchful {args.wf_host}:{args.wf_port} to dataset " \
            f"id {dataset_id}."
    if "error_msg" in load_attrib_res and load_attrib_res["error_msg"]:
        print(f"Error ingesting {msg}")
    else:
        print(f"Ingested {msg}")


if __name__ == "__main__":
    main(sys.argv[1:])
