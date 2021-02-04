#!/usr/bin/python3

""" Panel operations

3 subparsers:
- check:
    - panel: Check the panelapp dump folder given against the database
- generate:
    - panelapp_gms: Generate panelapp dump using only GMS panels
    - panelapp_all: Generate panelapp dump using only all panels
    - gene_files: Generate files for panels with gene symbols only
    - json: Generate django fixture using panelapp dump folder
    - genepanels: Generate genepanels file
    - manifest: Generate manifest type file for reports using Gemini db dump
    - g2t: Generate genes2transcript file
- mod_db:
    - initial_import: Import given django fixture in the database
    - hgnc: Import HGNC data dump in the database
"""

import argparse
import sys

import ops

sys.path.append(ops.config.path_to_panel_config)

import config_panel_db


def main(**param):
    user = config_panel_db.user_ro
    passwd = config_panel_db.passwd_ro
    host = config_panel_db.host

    if param["test_xls"]:
        # gather data from the test directory
        clinind_data = ops.utils.parse_test_directory(param["test_xls"])
        clean_clinind_data = ops.utils.clean_targets(clinind_data)

        # get the single genes in the test directory
        single_genes = ops.utils.gather_single_genes(clean_clinind_data)

    if param["command"] == "check":
        assert clean_clinind_data is not None, (
            "-t option is needed for check cmd"
        )
        # Check integrity of database for all things panel
        if param["dumps"]:
            files = {
                ele.split("=")[0]: ele.split("=")[1]
                for ele in param["dumps"]
            }

            session, meta = ops.utils.connect_to_db(
                user, passwd, host, "panel_database"
            )
            (
                panelapp_dict, superpanel_dict, gene_dict
            ) = ops.utils.create_panelapp_dict(
                files["panels"].split(";"), config_panel_db.panel_types,
                single_genes
            )
            (
                hgnc_data, symbol_dict, alias_dict, prev_dict
            ) = ops.utils.parse_hgnc_dump(files["hgnc"])
            nirvana_data = ops.utils.get_nirvana_data_dict(
                files["nirvana"], symbol_dict, alias_dict, prev_dict
            )
            check = ops.check.check_db(
                files, session, meta, panelapp_dict, superpanel_dict,
                gene_dict, nirvana_data, hgnc_data, clean_clinind_data
            )

    elif param["command"] == "generate":
        # Generate panelapp dump
        if param["panelapp_all"] is True:
            all_panels = ops.utils.get_all_panels()
            panelapp_dump = ops.generate.generate_panelapp_dump(
                all_panels, "all"
            )

        # Generate panelapp dump for GMS panels
        if param["panelapp_gms"] is True:
            gms_panels = ops.utils.get_GMS_panels()
            panelapp_dump = ops.generate.generate_panelapp_dump(
                gms_panels, "GMS"
            )

        # Generate panelapp dump for non-GMS panels
        if param["panelapp_non_gms"] is True:
            non_gms_panels = ops.utils.get_non_GMS_panels()
            panelapp_dump = ops.generate.generate_panelapp_dump(
                non_gms_panels, "non_GMS"
            )

        # Generate a bioinformatic manifest type file for reports
        if param["manifest"]:
            session, meta = ops.utils.connect_to_db(
                user, passwd, host, "panel_database"
            )
            sample2panels = ops.generate.generate_manifest(
                session, meta, param["manifest"]
            )

        # Generate django fixture using given panelapp dump
        if param["json"]:
            assert clean_clinind_data is not None, (
                "-t option is needed for generate json cmd"
            )
            files = {
                ele.split("=")[0]: ele.split("=")[1]
                for ele in param["json"]
            }

            # Primary keys for importing the data in the database
            pk_dict = {
                # they start at 1 because I loop over those elements
                # makes it easier to keep track off
                "clinind": 0, "panel": 0, "reference": 0, "feature_type": 0,
                "clinind_panels": 0, "feature": 0, "panel_feature": 0,
                "gene": 0, "panel_type": 0, "transcript": 0, "g2t": 0
            }

            (
                hgnc_data, symbol_dict, alias_dict, prev_dict
            ) = ops.utils.parse_hgnc_dump(files["hgnc"])
            nirvana_data = ops.utils.get_nirvana_data_dict(
                files["nirvana"], symbol_dict, alias_dict, prev_dict
            )

            # Generate the jsons for the import
            ops.generate.generate_django_jsons(
                files["panels"].split(";"), clean_clinind_data, hgnc_data,
                nirvana_data, single_genes, config_panel_db.references,
                config_panel_db.feature_types, config_panel_db.panel_types,
                pk_dict
            )

        # Generate genepanels file from database
        if param["genepanels"]:
            session, meta = ops.utils.connect_to_db(
                user, passwd, host, "panel_database"
            )
            ops.generate.generate_genepanels(session, meta)

        # Generate genes2transcripts file from database
        if param["g2t"]:
            session, meta = ops.utils.connect_to_db(
                user, passwd, host, "panel_database"
            )

            ops.generate.generate_g2t(session, meta)

    elif param["command"] == "mod_db":
        if (
            param["user"] == config_panel_db.user_admin and
            param["passwd"] == config_panel_db.passwd_admin
        ):
            # Import given django fixture to the database
            if param["initial_import"]:
                ops.mod_db.import_django_fixture(param["initial_import"])

            # Import HGNC dump file
            if param["hgnc"]:
                args = {
                    ele.split("=")[0]: ele.split("=")[1]
                    for ele in param["hgnc"]
                }
                ops.mod_db.import_hgnc_dump(args["hgnc"], args["date"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Prepare and import data for Panelapp database"
    )

    parser.add_argument("-t", "--test_xls", help="NHS test directory")

    subparser = parser.add_subparsers(dest="command")

    generate = subparser.add_parser("generate")
    generate.add_argument(
        "-gms", "--panelapp_gms", action="store_true",
        help="Generate panelapp GMS dump"
    )
    generate.add_argument(
        "-non-gms", "--panelapp_non_gms", action="store_true",
        help="Generate panelapp non-GMS dump"
    )
    generate.add_argument(
        "-all", "--panelapp_all", action="store_true",
        help="Generate all panelapp dump"
    )
    generate.add_argument(
        "-j", "--json", metavar="KEY=VALUE", nargs=3,
        help=(
            "Generate django json that need to be imported in the database. "
            "Generating the jsons files require 3 files: "
            "the panelapp dump(s)/custom dump(s) to actually import, "
            "hgnc file, nirvana gff file. They need to be given as "
            "following: \"panels=file;file, hgnc=file, nirvana=file\". "
            "The panelapp dump file path should contain the following string: "
            "gms, non-gms, in-house, single_gene. This allows to find the "
            "type of panel the folder contains."
        )
    )
    generate.add_argument(
        "-g2t", "--g2t", action="store_true",
        help="Generate genes2transcripts file"
    )
    generate.add_argument(
        "-gp", "--genepanels", action="store_true",
        help="Generate genepanels"
    )
    generate.add_argument("-m", "--manifest", help="Gemini database xls dump")

    check = subparser.add_parser("check")
    check.add_argument(
        "dumps", metavar="KEY=VALUE", nargs=3,
        help=(
            "Provide panelapp dump, hgnc dump and nirvana gff. The format for "
            "passing those arguments is: panels=folder hgnc=file "
            "nirvana=file."
        )
    )

    mod_db = subparser.add_parser("mod_db")
    mod_db.add_argument("user", help="Admin username for panel_database")
    mod_db.add_argument("passwd", help="Admin passwd for panel_database")

    mod_db.add_argument(
        "-i", "--initial_import",
        help="Import pointed json in the database"
    )
    mod_db.add_argument(
        "-hgnc", "--hgnc", metavar="KEY=VALUE", nargs=2,
        help=(
            "Import hgnc dump in the database. Need to provide "
            "hgnc=file date=yymmdd"
        )
    )

    args = vars(parser.parse_args())
    main(**args)
