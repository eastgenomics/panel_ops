import argparse
import sys

from ops import generate, mod_db, utils, config

sys.path.append(config.path_to_panel_config)

import config_panel_db


def parse_args():
    """ Parse args

    Returns:
        dict: Dict with subparser used and arguments used
    """

    parser = argparse.ArgumentParser(
        description="Prepare and import data for Panelapp database"
    )

    parser.add_argument(
        "-ref", "--reference", choices=["GRCh37", "GRCh38"],
        help="Which reference build to use for importing or exporting g2t data"
    )

    parser.add_argument("-t", "--test_xls", help="NHS test directory")
    parser.add_argument("-hgnc", "--hgnc", help="HGNC dump file")

    subparser = parser.add_subparsers(dest="command")

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
        "-j", "--json", metavar="KEY=VALUE", nargs=2,
        help=(
            "Generate django json that need to be imported in the database. "
            "Generating the jsons files require 2 files: "
            "the panelapp dump(s)/custom dump(s) to actually import, "
            "genes2transcripts file. They need to be given as following: "
            "panels=folder,folder g2t=file. The panelapp dump file path "
            "should contain the following string: gms, non-gms, in-house, "
            "single_gene. This allows to find the type of panel the folder "
            "contains."
        )
    )
    generate.add_argument(
        "-gp", "--genepanels", action="store_true",
        help="Generate genepanels"
    )
    generate.add_argument("-m", "--manifest", help="Gemini database csv dump")
    generate.add_argument(
        "-g2t", "--genes2transcripts", action="store_true",
        help="Generate the genes2transcripts file"
    )

    check = subparser.add_parser("check")
    check.add_argument(
        "files", metavar="KEY=VALUE", nargs=2,
        help=(
            "Provide panelapp dump and genes2transcripts. The format for "
            "passing those arguments is: panels=folder,folder g2t=file"
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
    mod_db.add_argument(
        "-g2t", "--g2t", help="Import new g2t file in the database"
    )
    mod_db.add_argument(
        "-new_panel", "--new_panel", help="Panel form xls file"
    )
    mod_db.add_argument(
        "-update_panelapp", "--update_panelapp", metavar="KEY=VALUE", nargs=2,
        help=(
            "Provide panelapp id and panelapp panel version using the "
            "following format: panelapp_id=ID,version=VERSION"
        )
    )
    mod_db.add_argument(
        "-update_panelapp_bulk", "--update_panelapp_bulk", help=(
            "Provide file with two columns with panelapp id in the first and "
            "the required version in the second"
        )
    )
    mod_db.add_argument(
        "-deploy_td", "--deploy_test_directory", help=(
            "Output file from test_directory_parser"
        )
    )
    mod_db.add_argument(
        "-ci_to_keep", "--ci_to_keep", nargs="+", help=(
            "Clinical indications r-codes to keep in conjonction of "
            "deployment of test directory"
        )
    )

    args = vars(parser.parse_args())

    return args


def main(**param):
    user = config_panel_db.user_ro
    passwd = config_panel_db.passwd_ro
    host = config_panel_db.host

    session, meta = utils.connect_to_db(
        user, passwd, host, "panel_database"
    )

    if param["reference"]:
        reference_id = utils.get_reference_id(
            session, meta, param["reference"]
        )

    if param["test_xls"]:
        # gather data from the test directory
        clinind_data = utils.parse_test_directory(param["test_xls"])
        clean_clinind_data = utils.clean_targets(clinind_data)

        # get the single genes in the test directory to transform them into
        # single gene panels
        single_genes = utils.gather_single_genes(clean_clinind_data)

    if param["hgnc"] and not isinstance(param["hgnc"], list):
        # parse hgnc file
        hgnc_data = utils.parse_hgnc_dump(param["hgnc"])

    # Check which subparser is being used
    if param["command"] == "check":
        assert clean_clinind_data is not None, (
            "-t option is needed for check cmd"
        )
        # Check integrity of database for all things panel
        if param["files"]:
            files = {
                ele.split("=")[0]: ele.split("=")[1]
                for ele in param["files"]
            }

            # gather data from panels
            (
                panelapp_dict, superpanel_dict, gene_dict
            ) = utils.create_panelapp_dict(
                files["panels"].split(","), config_panel_db.panel_types,
                single_genes
            )

            # get all the transcripts from the nirvana gff
            g2t_data = utils.parse_g2t(files["g2t"])

            # check the database data
            check = check.check_db(
                files, session, meta, panelapp_dict, superpanel_dict,
                gene_dict, g2t_data, clean_clinind_data
            )

    elif param["command"] == "generate":
        # Generate panelapp dump
        if param["panelapp_all"]:
            all_panels = utils.get_all_panels()
            panelapp_dump = generate.generate_panelapp_tsvs(
                all_panels, "all"
            )

        # Generate panelapp dump for GMS panels
        if param["panelapp_gms"]:
            gms_panels = utils.get_GMS_panels()
            panelapp_dump = generate.generate_panelapp_tsvs(
                gms_panels, "GMS"
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
                # they start at 0 because I loop over those elements
                # makes it easier to keep track off
                # + might that in the update process later (i.e. get last ele
                # in a table and put it in the pk_dict)
                "clinind": 0, "panel": 0, "reference": 0, "feature_type": 0,
                "clinind_panels": 0, "feature": 0, "panel_feature": 0,
                "gene": 0, "panel_type": 0, "transcript": 0, "g2t": 0
            }

            # get all transcripts in nirvana gff
            g2t_data = utils.parse_g2t(files["g2t"])

            # Generate the jsons for the import
            generate.generate_django_jsons(
                files["panels"].split(","), clean_clinind_data, g2t_data,
                single_genes, config_panel_db.references,
                config_panel_db.feature_types, config_panel_db.panel_types,
                pk_dict
            )

        # Generate genepanels file from database
        if param["genepanels"]:
            assert hgnc_data is not None, (
                "-hgnc option is needed for genepanels cmd"
            )
            generate.generate_genepanels(session, meta, hgnc_data)

        # Generate a bioinformatic manifest type file for reports
        if param["manifest"]:
            assert hgnc_data is not None, (
                "-hgnc option is needed for manifest cmd"
            )
            generate.generate_manifest(
                session, meta, param["manifest"], hgnc_data
            )

        if param["genes2transcripts"]:
            assert param["reference"], (
                "-g2t option is needed for creating a g2t file"
            )
            generate.generate_g2t(session, meta, reference_id)

    elif param["command"] == "mod_db":
        # check if the credentials for panel admin are correct
        if (
            param["user"] == config_panel_db.user_admin and
            param["passwd"] == config_panel_db.passwd_admin
        ):
            # Import given django fixture to the database
            if param["initial_import"]:
                mod_db.import_django_fixture(param["initial_import"])

            # Import HGNC dump file
            if param["hgnc"]:
                args = {
                    ele.split("=")[0]: ele.split("=")[1]
                    for ele in param["hgnc"]
                }
                mod_db.import_hgnc_dump(args["hgnc"], args["date"])

            if param["g2t"]:
                assert param["reference"], (
                    "-g2t option is needed for creating a g2t file"
                )
                mod_db.import_new_g2t(param["g2t"], reference_id)

            if param["new_panel"]:
                mod_db.import_panel_form_data(param["new_panel"])

            if param["update_panelapp"]:
                panel_info = {
                    ele.split("=")[0]: ele.split("=")[1]
                    for ele in param["update_panelapp"]
                }
                mod_db.update_panelapp_panel(
                    panel_info["panelapp_id"], panel_info["version"]
                )

            if param["update_panelapp_bulk"]:
                panels = utils.parse_panelapp_update_file(
                    param["update_panelapp_bulk"]
                )

                for panel in panels:
                    mod_db.update_panelapp_panel(
                        panel["panelapp_id"], panel["version"]
                    )

            if param["deploy_test_directory"]:
                td_data = utils.parse_json_file(
                    param["deploy_test_directory"]
                )

                if param["ci_to_keep"]:
                    ci_to_keep = mod_db.gather_ci_and_panels_to_keep(
                        param["ci_to_keep"]
                    )
                else:
                    ci_to_keep = []

                mod_db.clear_old_clinical_indications_panels(ci_to_keep)

                cp_data, pf_data = mod_db.create_objects_for_td(
                    td_data, ci_to_keep
                )
                mod_db.import_td(cp_data, pf_data)


if __name__ == "__main__":
    args = parse_args()
    main(**args)
