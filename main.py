#!/usr/bin/python3

""" Panel operations

4 subparsers:
- check:
    - gene: get transcripts of given gene using the nirvana GFF
    - panel: check the panelapp dump folder given against the database
    - test: check the National test directory file against the database
- generate:
    - panelapp_gms: generate panelapp dump using only GMS panels
    - panelapp_all: generate panelapp dump using only all panels
    - g2t: generate genes2transcripts file + no transcripts file
    - gene_files: generate files for panels with gene symbols only
    - json: generate django fixture using panelapp dump folder
    - genepanels: generate genepanels file
    - gemini: generate dump of gemini names
    - manifest: generate manifest type file for reports using Gemini db dump
    - panel_names_gms: generate file with GMS panel names
    - panel_names_all: generate file with all panel names
- query:
    - gemini_name: Query to get the full gemini name given a substring of that name
    - gene_test: Query to get all the genes for a gemini name
- mod_db:
    - initial_import: Import given django fixture in the database
"""

import argparse
import sys

import ops

sys.path.append(ops.config.path_to_panel_config)

import config_panel_db


def main(**param):
    hgmd_dict = ops.utils.parse_HGMD()
    nirvana_dict = ops.utils.get_nirvana_data_dict(param["gff"])
    ci_dict, test_dict = ops.utils.parse_tests_xls(param["test_xls"])
    test2targets = ops.utils.clean_targets(ci_dict, test_dict)

    user = config_panel_db.user_admin
    passwd = config_panel_db.passwd_admin
    host = config_panel_db.host

    if param["command"] == "check":
        # check which transcript given was assigned
        if param["gene"]:
            ops.check.check_gene(param["gene"], hgmd_dict, nirvana_dict)

        # Check integrity of database for all things panel
        if param["panel"]:
            session, meta = ops.utils.connect_to_db(user, passwd, host)
            panelapp_dicts = ops.utils.create_panelapp_dict(
                param["panel"], hgmd_dict, nirvana_dict
            )
            check = ops.check.check_panelapp_dump_against_db(
                param["panel"], session, meta, panelapp_dicts
            )

        # Check integrity of database for tests
        if param["test"]:
            session, meta = ops.utils.connect_to_db(user, passwd, host)
            check = ops.check.check_test_against_db(
                session, meta, test2targets
            )

    elif param["command"] == "generate":
        # Generate g2t + genes without transcript files
        if param["g2t"]:
            ops.generate.generate_g2t(
                param["g2t"], hgmd_dict, nirvana_dict
            )

        # Generate gms panel files with only green genes
        if param["gene_files"]:
            gms_panels = ops.utils.get_GMS_panels()
            ops.generate.generate_gms_panels(gms_panels)

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

        # Generate a bioinformatic manifest type file for reports
        if param["manifest"]:
            session, meta = ops.utils.connect_to_db(user, passwd, host)
            sample2panels = ops.generate.generate_sample2panels(
                session, meta, param["manifest"]
            )

        # Generate file containing all the GMS panels names stored in the db
        if param["panel_names_gms"]:
            session, meta = ops.utils.connect_to_db(user, passwd, host)
            panel_file = ops.generate.generate_panel_names(
                session, meta, gms=True
            )

        # Generate file containing all the panels names stored in the db
        if param["panel_names_all"]:
            session, meta = ops.utils.connect_to_db(user, passwd, host)
            panel_file = ops.generate.generate_panel_names(
                session, meta, gms=False
            )

        # Generate django fixture using given panelapp dump
        if param["json"]:
            # Primary keys for importing the data in the database
            pk_dict = {
                # they start at 1 because I loop over those elements
                # makes it easier to keep track off
                "test": 1, "panel": 1, "reference": 1,
                "testpanel": 0, "testgene": 0,
                "panelgene": 0, "panelstr": 0, "panelcnv": 0,
                "gene": 0, "transcript": 0, "exon": 0,
                "str": 0, "regionstr": 0,
                "cnv": 0, "regioncnv": 0,
                "region": 0, "superpanel": 0
            }
            (
                panelapp_dict, superpanel_dict, gene_dict,
                str_dict, cnv_dict, region_dict
            ) = ops.utils.create_panelapp_dict(
                param["json"], hgmd_dict, nirvana_dict
            )
            json_lists = ops.generate.create_django_json(
                ci_dict, test2targets, panelapp_dict, superpanel_dict,
                gene_dict, str_dict, cnv_dict, region_dict, pk_dict
            )
            ops.generate.write_django_jsons(json_lists)

        # Generate genepanels file from database
        if param["genepanels"]:
            session, meta = ops.utils.connect_to_db(user, passwd, host)
            ops.generate.generate_genepanels(session, meta)

        # Generate file containing the name of the tests in Gemini
        if param["gemini"]:
            session, meta = ops.utils.connect_to_db(user, passwd, host)
            ops.generate.generate_gemini_names(session, meta, test2targets)

    elif param["command"] == "query":
        session, meta = ops.utils.connect_to_db(user, passwd, host)

        # Return the full gemini name given a substring of that name
        if param["gemini_name"]:
            ops.queries.get_gemini_name(session, meta, param["gemini_name"])

        # Return the genes given a substring of a gemini name
        elif param["gene_test"]:
            ops.queries.get_genes_from_gemini_name(
                session, meta, param["gene_test"]
            )

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

    parser.add_argument("gff", help="Nirvana GFF dump")
    parser.add_argument("test_xls", help="NHS test directory")

    generate = subparser.add_parser("generate")
    generate.add_argument(
        "-gms", "--panelapp_gms", action="store_true",
        help="Generate panelapp GMS dump"
    )
    generate.add_argument(
        "-all", "--panelapp_all", action="store_true",
        help="Generate all panelapp dump"
    )
    generate.add_argument(
        "-g2t", "--g2t", help="Genes2transcript file"
    )
    generate.add_argument(
        "-gf", "--gene_files", action="store_true",
        help="Generate gene files for GMS panels"
    )
    generate.add_argument(
        "-j", "--json",
        help="Panelapp dump to generate json to import data directly"
    )
    generate.add_argument(
        "-gp", "--genepanels", action="store_true",
        help="Generate genepanels"
    )
    generate.add_argument(
        "-gd", "--gemini", action="store_true",
        help="Generate dump of database of the gemini names"
    )
    generate.add_argument("-m", "--manifest", help="Gemini database xls dump")
    generate.add_argument(
        "-pn_gms", "--panel_names_gms", action="store_true",
        help="Write gms panel names in a file"
    )
    generate.add_argument(
        "-pn_all", "--panel_names_all", action="store_true",
        help="Gemini database xls dump"
    )

    check = subparser.add_parser("check")
    check.add_argument(
        "-g", "--gene",
        help="Get transcripts of given gene using the nirvana GFF"
    )
    check.add_argument(
        "-p", "--panel",
        help=(
            "Folder containing panel dump and "
            "check if the panel data imported matches the panelapp dumps"
        )
    )
    check.add_argument(
        "-t", "--test", action="store_true",
        help="Test that the tests has been correctly imported in the db"
    )

    query = subparser.add_parser("query")
    query.add_argument(
        "-g", "--gemini_name",
        help="Query to get gemini name from part of the gemini name"
    )
    query.add_argument(
        "-t", "--gene_test",
        help="Query the genes in a test code"
    )

    mod_db = subparser.add_parser("mod_db")
    mod_db.add_argument(
        "-i", "--initial_import",
        help="Import pointed json in the database"
    )

    args = vars(parser.parse_args())
    main(**args)
