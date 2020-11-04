#!/usr/bin/python3

import argparse

import ops
from config_panel_db import user, passwd, host


def main(**param):
    hgmd_dict = ops.utils.parse_HGMD()
    nirvana_dict = ops.utils.get_nirvana_data_dict(param["gff"])
    ci_dict, test_dict = ops.utils.parse_tests_xls(param["test_xls"])
    test2targets = ops.utils.clean_targets(test_dict)

    if param["command"] == "check":
        # check which transcript given was assigned
        if param["gene"]:
            ops.check.check_gene(param["gene"], hgmd_dict, nirvana_dict)

        # Check integrity of database for all things panel
        if param["panel"]:
            session, meta = ops.utils.connect_to_db(user, passwd, host)
            check = ops.check.check_panelapp_dump_against_db(
                param["panel"], session, meta, hgmd_dict, nirvana_dict
            )

            if check is True:
                print("Check completed and nothing bad to report")

        # Check integrity of database for tests
        if param["test"]:
            session, meta = ops.utils.connect_to_db(user, passwd, host)
            check = ops.check.check_test_against_db(
                session, meta, test2targets
            )

            if check is True:
                print("Check completed and nothing bad to report")

    elif param["command"] == "generate":
        # Generate g2t + genes without transcript files
        if param["g2t"]:
            ops.generate.get_all_transcripts(
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

        # Generate django fixture using given panelapp dump
        if param["json"]:
            (
                panelapp_dict, gene_dict, str_dict, cnv_dict, region_dict
            ) = ops.utils.create_panelapp_dict(
                param["json"], hgmd_dict, nirvana_dict
            )
            json_lists = ops.generate.create_django_json(
                ci_dict, test2targets, panelapp_dict, gene_dict, str_dict,
                cnv_dict, region_dict
            )
            ops.generate.write_django_jsons(json_lists)

        # Generate genepanels file from database
        if param["genepanels"]:
            session, meta = ops.utils.connect_to_db(user, passwd, host)
            ops.generate.generate_genepanels(session, meta)

        # Generate gemini name file from database
        if param["gemini"]:
            session, meta = ops.utils.connect_to_db(user, passwd, host)
            ops.generate.generate_gemini_names(session, meta, test2targets)

    elif param["command"] == "update":
        (
            panelapp_dict, gene_dict, str_dict, cnv_dict, region_dict
        ) = ops.utils.create_panelapp_dict(
            param["panelapp_dump"], hgmd_dict, nirvana_dict
        )
        ops.update.update_django_tables(panelapp_dict)


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

    update = subparser.add_parser("update")
    update.add_argument(
        "panelapp_dump", action="store_true",
        help="Update the database using the panels in the folder given in -p"
    )

    args = vars(parser.parse_args())
    main(**args)
