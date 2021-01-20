#!/usr/bin/python3

""" Panel operations

3 subparsers:
- check:
    - panel: check the panelapp dump folder given against the database
- generate:
    - panelapp_gms: generate panelapp dump using only GMS panels
    - panelapp_all: generate panelapp dump using only all panels
    - gene_files: generate files for panels with gene symbols only
    - json: generate django fixture using panelapp dump folder
    - genepanels: generate genepanels file
    - manifest: generate manifest type file for reports using Gemini db dump
- mod_db:
    - initial_import: Import given django fixture in the database
"""

import argparse
import sys

import ops

sys.path.append(ops.config.path_to_panel_config)

import config_panel_db


def main(**param):
    # gather data from the test directory
    clinind_data = ops.utils.parse_test_directory(param["test_xls"])
    clean_clinind_data = ops.utils.clean_targets(clinind_data)

    # get the single genes in the test directory
    single_genes = ops.utils.gather_single_genes(clean_clinind_data)

    user = config_panel_db.user_admin
    passwd = config_panel_db.passwd_admin
    host = config_panel_db.host

    if param["command"] == "check":
        # Check integrity of database for all things panel
        if param["folder"]:
            session, meta = ops.utils.connect_to_db(user, passwd, host)
            (
                panelapp_dict, superpanel_dict, gene_dict
            ) = ops.utils.create_panelapp_dict(
                param["folder"], config_panel_db.panel_types, single_genes
            )
            check = ops.check.check_db(
                param["folder"], session, meta, panelapp_dict, superpanel_dict,
                clean_clinind_data
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
            session, meta = ops.utils.connect_to_db(user, passwd, host)
            sample2panels = ops.generate.generate_manifest(
                session, meta, param["manifest"]
            )

        # Generate django fixture using given panelapp dump
        if param["json"]:
            # Primary keys for importing the data in the database
            pk_dict = {
                # they start at 1 because I loop over those elements
                # makes it easier to keep track off
                "clinind": 1, "panel": 1, "reference": 1,
                "clinind_panels": 0, "feature": 0, "panel_feature": 0,
                "gene": 0, "str": 0, "cnv": 0, "panel_type": 1
            }

            (
                panelapp_dict, superpanel_dict, gene_dict
            ) = ops.utils.create_panelapp_dict(
                param["json"], config_panel_db.panel_types, single_genes
            )

            # Create the list of reference table
            reference_json = ops.utils.gather_ref_django_json(
                config_panel_db.references
            )
            # Create the list of panel_type table
            paneltype_json = ops.utils.gather_panel_types_django_json(
                config_panel_db.panel_types
            )
            # Create the list of feature_type table
            featuretype_json = ops.utils.gather_feature_types_django_json(
                config_panel_db.feature_types
            )

            # Create the list for data associated with panels
            (
                panel_json, feature_json, panelfeature_json, featuretype_json,
                gene_json, pk_dict
            ) = ops.utils.gather_panel_data_django_json(
                panelapp_dict, gene_dict, featuretype_json, paneltype_json,
                reference_json, pk_dict
            )

            # Add the superpanels to the list of panel objects
            (
                panel_json, panelfeature_json
            ) = ops.utils.gather_superpanel_data_django_json(
                superpanel_dict, panel_json, paneltype_json, reference_json,
                panelfeature_json, pk_dict
            )

            # Create the list of clinical indication
            (
                clinical_indication_json, clinical_indication2panels_json
            ) = ops.utils.gather_clinical_indication_data_django_json(
                clean_clinind_data, panel_json, pk_dict
            )

            # Generate the jsons for the import
            ops.generate.generate_django_jsons(
                ref=reference_json, panel_type=paneltype_json,
                feature_type=featuretype_json, panel_feature=panelfeature_json,
                gene=gene_json, clinical_indication=clinical_indication_json,
                feature=feature_json, panel=panel_json,
                clinical_indication2panels=clinical_indication2panels_json
            )

        # Generate genepanels file from database
        if param["genepanels"]:
            session, meta = ops.utils.connect_to_db(user, passwd, host)
            ops.generate.generate_genepanels(session, meta)

    elif param["command"] == "mod_db":
        # Import given django fixture to the database
        if param["initial_import"]:
            ops.mod_db.import_django_fixture(
                param["initial_import"]
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Prepare and import data for Panelapp database"
    )

    subparser = parser.add_subparsers(dest="command")

    parser.add_argument("test_xls", help="NHS test directory")

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
        "-j", "--json", nargs="+",
        help=(
            "Dump folders to generate json to import data directly, "
            "must contain: gms, non_gms, in_house"
        )
    )
    generate.add_argument(
        "-gp", "--genepanels", action="store_true",
        help="Generate genepanels"
    )
    generate.add_argument("-m", "--manifest", help="Gemini database xls dump")

    check = subparser.add_parser("check")
    check.add_argument(
        "folder", nargs="+",
        help=(
            "Folder containing panel dump and "
            "check if the panel data imported matches the panelapp dumps"
        )
    )

    mod_db = subparser.add_parser("mod_db")
    mod_db.add_argument(
        "-i", "--initial_import",
        help="Import pointed json in the database"
    )

    args = vars(parser.parse_args())
    main(**args)
