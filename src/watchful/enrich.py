"""
This script is run on the Python command line to execute data enrichment. If a
custom enricher is used, then the :func:`enrich_dataset` function is used.
"""
################################################################################


import argparse
import datetime
import os
import pathlib
import shutil
import sys
from typing import List, Type
from watchful import client, attributes
from watchful.enricher import Enricher


def enrich_dataset(
    custom_enricher_cls: Type[Enricher],
    args: List[str] = None,
) -> None:
    """
    This is the function to use for performing custom data enrichment. Custom
    data enrichment variables, functions and mdoels can be defined in
    ``custom_enricher_cls`` and are used to perform the data enrichment.

    :param custom_enricher_cls: A custom enricher class that inherited
        :class:`Enricher` and implemented its abstract methods.
    :type custom_enricher_cls: Enricher
    :param args: A list containing optional input arguments as defined in
        :func:`main`.
    :type args: List[str]
    """

    if args is None:
        args = []
    attributes.set_multiprocessing(False)
    custom_enricher = custom_enricher_cls()

    main(args, custom_enricher)


def main(args: List[str] = None, custom_enricher: Enricher = None) -> None:
    """
    This is the utility function for performing data enrichment without a custom
    enricher; it is generally not called directly but invoked via the Python
    command line by the user. To perform data enrichment with a custom enricher,
    use :func:`enrich_dataset` instead.

    This function contains the logic and pipelining for the data enrichment.

    :param args: A list containing optional input arguments as defined in
        the ``parser`` arguments below.
    :type args: List[str]
    """

    if args is None:
        args = []

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # The dataset file; use the dataset currently opened in Watchful if it is
    # not provided. This is not used in remote enrichment as we will retrieve
    # the dataset from the remote Watchful application.
    parser.add_argument(
        "--in_file",
        type=str,
        default="",
        help="Optional original csv dataset filepath; if not given use the \
            current dataset opened in Watchful.",
    )

    # The attributes output file that is specifically formatted for integration
    # with the Watchful application.
    parser.add_argument(
        "--out_file",
        type=str,
        default="",
        help='Optional output attribute filepath; if given must end with \
            ".attrs" extension.',
    )

    # The attribute file to ingest from, if it is available.
    parser.add_argument(
        "--attr_file",
        type=str,
        default="",
        help="Optional input csv attribute filepath; if not given create the \
            initial spacy attributes.",
    )

    # The columns in the ``attr_file`` csv file to use as attributes; use all
    # attributes if it is not provided.
    parser.add_argument(
        "--attr_names",
        type=str,
        default="",
        help="Optional comma delimited string of attribute names to be used; \
            if not given use all attribute names.",
    )

    # The host to use; use "localhost" if it is not provided.
    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help='Optional string host running Watchful; if not given use \
            "localhost".',
    )

    # The port to use; use "9001" if it is not provided.
    parser.add_argument(
        "--port",
        type=str,
        default="9001",
        help='Optional string port number running Watchful; if not given use \
            "9001".',
    )

    # The out-of-the-box NLP to use if no ``attr_file`` is provided.
    parser.add_argument(
        "--standard_nlp",
        type=str,
        default="spacy",
        help='Optional out-of-the-box NLP to use, currently "spacy" and \
            "flair" are available; "spacy" if unspecified.',
    )

    # Option to use multiprocessing. This is still in internal alpha mode and is
    # not expected to be used by user.
    parser.add_argument(
        "--multiprocessing",
        action="store_true",
        help="Optional explicit use of multiprocessing on available physical \
            cpu cores; no explicit use if unspecified.",
    )

    # Option to use Watchful binary. This is for backward compatibility and is
    # not expected to be used by user.
    parser.add_argument(
        "--is_local",
        action="store_true",
        help="Optional use of local Watchful local application instead of a \
            hosted application; hosted application if unspecified.",
    )

    args = parser.parse_args(args=args)

    attributes.set_multiprocessing(args.multiprocessing)

    client.external(host=args.host, port=args.port)

    summary = client.get()
    project_id = client.get_project_id(summary)
    (
        dataset_id,
        datasets_dir,
        args.in_file,
    ) = attributes.get_dataset_id_dir_filepath(
        summary, args.in_file, args.is_local
    )

    # ``args.in_file`` will still be returned as "" if Watchful application is
    # remote. Therefore, we create a temporary filepath for ``args.in_file`` to
    # download the original dataset to.
    if not args.is_local:
        if args.in_file == "":
            user_home_path = os.path.expanduser("~")
            working_dir = os.path.join(user_home_path, "watchful", "working")
            os.makedirs(working_dir, exist_ok=True)
            args.in_file = os.path.join(working_dir, dataset_id)
            client.export_dataset_to_path(args.in_file, summary["field_names"])
        else:
            print(
                'in_file must be initially "" for enrichment to a remote '
                f'Watchful applcation; got "{args.in_file}" instead.'
            )
            sys.exit(1)

    if args.out_file:
        # Check that ``out_file`` has ".attrs" extension.
        try:
            is_ext_attrs = os.path.splitext(args.out_file)[1] == ".attrs"
            if is_ext_attrs:
                del_out_file = False
                # check that ``out_file``'s directory exists.
                out_file_dir = pathlib.Path(args.out_file).parent
                if not os.path.isdir(out_file_dir):
                    print(f"Directory {out_file_dir} does not exist.")
                    sys.exit(1)
            else:
                print(
                    f'out_file {args.out_file} must end with ".attrs" '
                    "extension."
                )
                sys.exit(1)
        except OSError as err_msg:
            print(err_msg)
            print(f'out_file {args.out_file} must end with ".attrs" extension.')
            sys.exit(1)
    else:
        del_out_file = True
        # Create a temporary ``out_file`` and mark it for deletion.
        args.out_file = f"{os.path.splitext(args.in_file)[0]}.attrs"

    # Enrich with attributes from a csv file, that is, already created from an
    # external pipeline.
    if args.attr_file:

        # Enrich with all attributes.
        if not args.attr_names:
            print(
                f"Enriching {args.in_file} using all attributes from "
                f"{args.attr_file} ..."
            )
            attributes.enrich(
                args.in_file,
                args.out_file,
                attributes.enrich_row_with_attribute_data,
                attributes.get_vars_for_enrich_row_with_attribute_data(
                    args.attr_names, args.attr_file
                ),
            )
            if not del_out_file:
                print(f"Wrote attributes to {args.out_file}.")

        # Enrich with specified attributes.
        else:
            val_success = attributes.validate_attribute_names(
                args.attr_names, args.attr_file
            )

            if val_success:
                print(
                    f"Enriching {args.in_file} using {args.attr_names} "
                    f"attributes from {args.attr_file} ..."
                )
                attributes.enrich(
                    args.in_file,
                    args.out_file,
                    attributes.enrich_row_with_attribute_data,
                    attributes.get_vars_for_enrich_row_with_attribute_data(
                        args.attr_names, args.attr_file
                    ),
                )
                if not del_out_file:
                    print(f"Wrote attributes to {args.out_file}.")
            else:
                print(
                    f"At least one of your attribute names in "
                    f"{args.attr_names} do not match those in the attribute "
                    f"input file {args.attr_file}."
                )
                sys.exit(1)

    # Enrich using out-of-the-box NLPs.
    elif custom_enricher is None:

        # SpaCy NLP.
        if args.standard_nlp == "spacy":
            # Want to know what pipes are used? Uncomment these:
            # nlp = attributes.load_spacy()
            # import pprint
            # pprint.PrettyPrinter(indent=4).pprint(nlp.analyze_pipes())

            # ``enrich_row`` is the user custom function for enriching every row
            # of the dataset. ``spacy_atterize_fn``, ``spacy_atterize`` and
            # ``load_spacy()`` are the additional user variables to perform the
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
                    *attributes.load_spacy(),
                ),
            )
            if not del_out_file:
                print(f"Wrote attributes to {args.out_file}.")

        # Flair NLP.
        elif args.standard_nlp == "flair":
            # ``enrich_row`` is the user custom function for enriching every row
            # of the dataset. ``flair_atterize_fn``, ``flair_atterize`` and
            # ``*load_flair()`` are the additional user variables to perform the
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
                    *attributes.load_flair(),
                ),
            )
            if not del_out_file:
                print(f"Wrote attributes to {args.out_file}.")

        else:
            print(
                f"The nlp {args.standard_nlp} is not implemented.\nNo "
                "enrichment done."
            )
            sys.exit(1)

    # Enrich using custom enricher class.
    else:
        # Perform custom data enrichment. Custom data enrichment variables,
        # functions and models in ``custom_enricher`` are used to perform the
        # data enrichment.
        print("Using your custom enricher ...")
        print(f"Enriching {args.in_file} ...")
        attributes.enrich(
            args.in_file,
            args.out_file,
            custom_enricher.enrich_row,
            custom_enricher.enrichment_args,
        )
        if not del_out_file:
            print(f"Wrote attributes to {args.out_file}.")

    # If Watchful application is remote, delete the downloaded dataset as the
    # data enrichment is completed.
    if not args.is_local:
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
            f"Current project {current_project_id} is different from the "
            f"enriched project {project_id}!"
        )
        sys.exit(1)
    current_dataset_id = client.get_dataset_id(summary)
    if dataset_id != current_dataset_id:
        print(
            f"Current dataset {current_dataset_id} is different from the "
            f"enriched dataset {dataset_id}!"
        )
        sys.exit(1)

    # Format the attributes file timestamp as "yyyy-mm-dd_hh-mm-ss-ssssss".
    # Use the full timestamp for completeness; though it's reasonable
    # "yyyy-mm-dd_hh-mm-ss" should work uniquely too.
    # This timestamp format is:
    #     - usable for pulling out the latest attributes
    #     - usable for pulling out a time-series progression of attributes
    #     - consistent with Kubeflow pipelines integration
    # The final filename format is "filename__yyyy-mm-dd_hh-mm-ss-ssssss.attrs".
    timestamp = (
        str(datetime.datetime.now())
        .replace(" ", "_")
        .replace(":", "-")
        .replace(".", "-")
    )
    dest_attr_filename = f"__{timestamp}.".join(
        os.path.basename(args.out_file).split(".")
    )

    # Copy the created attributes output file to Watchful home attributes
    # directory if the Watchful application is local.
    if args.is_local:
        dest_attr_filepath = os.path.join(
            datasets_dir, "attrs", dest_attr_filename
        )
        try:
            os.makedirs(os.path.dirname(dest_attr_filepath), exist_ok=True)
            shutil.copyfile(args.out_file, dest_attr_filepath)
        except OSError as err_msg:
            print(err_msg)
            print(
                f"Error copying attribute output file from {args.out_file} "
                f"to {dest_attr_filepath}."
            )
            sys.exit(1)
        # Load attributes filepath into Watchful application.
        # Example usage:
        # curl -iX POST http://localhost:9001/api \
        #   --header "Content-Type: application/json" \
        #   --data "{\"verb\":\"attributes\",\
        # \"id\":\"9570b0b5-4a58-445f-9b51-b434caca2650\",\
        # \"filepath\":\"/path/to/attributes_file.attrs\"}"
        # Arguments:
        #     id: dataset id
        #     filepath: attributes filepath
        res = client.load_attributes(dataset_id, dest_attr_filename)

    # Upload the created attributes output file to Watchful application if it is
    # remote.
    else:
        res = client.upload_attributes(dataset_id, args.out_file)

    msg = (
        f"attributes via watchful {args.host}:{args.port} to dataset "
        f"id {dataset_id}."
    )
    if "error_msg" in res and res["error_msg"]:
        print(f"Error ingesting {msg}")
    else:
        print(f"Ingested {msg}")

    # Remove temporary attributes output file.
    if del_out_file:
        try:
            os.remove(args.out_file)
        except FileNotFoundError as err_msg:
            print(err_msg)
            print(
                "Error removing temporary attribute output file from "
                f"{args.out_file}."
            )
            sys.exit(1)


if __name__ == "__main__":
    # This is the Python command line to use for performing data enrichment
    # without a custom enricher. To perform data enrichment with a custom
    # enricher, use :func:`enrich_dataset` instead.
    # Refer to :func:`main`for the optional input arguments.

    main(sys.argv[1:])
