from collections import defaultdict, OrderedDict
import datetime
import json
import os
import re
from pathlib import Path

from packaging import version
import pandas as pd
import regex
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.schema import MetaData
import xlrd

from panelapp import queries
from hgnc_queries import get_id as hq_get_id

from ops.hardcoded_tests import tests as hd_tests
from ops.logger import setup_logging, output_to_loggers


CONSOLE, UTILS = setup_logging("utils")


def get_date():
    """ Return today's date in YYMMDD format

    Returns:
        str: Date
    """

    return str(datetime.date.today())[2:].replace("-", "")


def write_new_output_folder(output_dump: str, output_suffix: str = ""):
    """ Return new folder to output files in

    Args:
        output_dump (str): Type of output folder
        output_suffix (str, optional): Suffix to be added to subfolder. Defaults to "".

    Returns:
        str: Folder path to the final output folder
    """

    output_date = get_date()
    output_index = 1

    output_folder = f"{output_dump}/{output_date}-{output_index}"

    if output_suffix:
        output_folder = f"{output_folder}_{output_suffix}"

    # don't want to overwrite files so create folders using the output index
    while Path(output_folder).is_dir():
        output_index += 1
        output_folder = f"{output_dump}/{output_date}-{output_index}"

        if output_suffix:
            output_folder = f"{output_folder}_{output_suffix}"

    # create folders
    Path(output_folder).mkdir(parents=True)

    return output_folder


def connect_to_db(user: str, passwd: str, host: str, database: str):
    """ Return cursor of panel_database

    Args:
        user (str): Username for the database
        passwd (str): Password for the user
        host (str): Host for the database
        database (str): Name of the database to connect to

    Returns:
        tuple: SQLAlchemy session obj, SQLAlchemy meta obj
    """

    try:
        db = create_engine(
            f"mysql://{user}:{passwd}@{host}/{database}"
        )
    except Exception as e:
        UTILS.error(e)
        raise e
    else:
        meta = MetaData()
        meta.reflect(bind=db)
        Session = sessionmaker(bind=db)
        session = Session()
        return session, meta


def get_all_panels():
    """ Return dict of panelapp_id to panel object

    Returns:
        dict: Dict of all panels in panelapp
    """

    signedoff_panels = queries.get_all_signedoff_panels()
    all_panels = queries.get_all_panels()

    for panel_id, panel in signedoff_panels.items():
        all_panels[panel_id] = panel

    return all_panels


def get_GMS_panels():
    """ Return dict of panelapp_id to panel object

    Returns:
        dict: Dict of GMS panels in panelapp
    """

    return queries.get_all_signedoff_panels()


def get_panel_type(type_of_panels: list, dump_folder: str):
    """ Get the panel type using the folder name

    Args:
        type_of_panels (list): Hardcoded types of panels stored in the config
        dump_folder (str): Output folder

    Raises:
        Exception: If the panel type cannot be found

    Returns:
        str: Panel type
    """
    assigned_panel_type = None

    # Try and find the panel type using the name of
    # the folder the panels were stored
    for panel_type in type_of_panels:
        if regex.search(
            panel_type, dump_folder, regex.IGNORECASE
        ):
            assigned_panel_type = panel_type

    if assigned_panel_type is None:
        error_msg = (
            "Couldn't find the panel_type using the "
            f"dump folder name: {dump_folder}"
        )
        debug_msg = (
            "Change the name of the folder to contain "
            "one of the following: "
            f"{', '.join(type_of_panels)}"
        )
        UTILS.error(error_msg)
        UTILS.error(debug_msg)
        raise Exception(error_msg)

    return assigned_panel_type


def filter_out_gene(hgnc_row: dict, header: str, string_to_match: str):
    if string_to_match in hgnc_row[header]:
        return True
    else:
        return False


def parse_hgnc_dump(hgnc_file: str):
    """ Parse the hgnc dump and return a dict of the data in the dump

    Args:
        hgnc_file (str): Path to the hgnc file

    Returns:
        dict: Dict of hgnc data, symbol data, alias data, previous symbol data
    """

    data = {}

    with open(hgnc_file) as f:
        for i, line in enumerate(f):
            # first line is headers
            if i == 0:
                reformatted_headers = []
                headers = line.strip().split("\t")

                for header in headers:
                    # need transform the header name to the table attribute name
                    if "supplied" in header:
                        # external links provided always have: "(supplied by ...)"
                        # split on the (, get the first element, strip it
                        # (there's spaces sometimes), lower the characters and
                        # replace spaces by underscores
                        header = header.split("(")[0].strip().lower().replace(" ", "_")
                        # they also need ext_ because they're external links
                        header = f"ext_{header}"
                    else:
                        header = header.lower().replace(" ", "_")

                    reformatted_headers.append(header)

            else:
                line = line.strip("\n").split("\t")

                for j, ele in enumerate(line):
                    if j == 0:
                        hgnc_id = ele
                        data.setdefault(hgnc_id, {})
                    else:
                        # we have the index of the line so we can automatically
                        # get the header and use it has a subkey in the dict
                        data[hgnc_id][reformatted_headers[j]] = ele

    return data


def parse_g2t(file):
    """ Return dict genes2transcripts from genes2transcripts file

    Args:
        file (str): Path to genes2transcripts file

    Returns:
        dict: Dict of genes2transcripts
    """

    g2t = defaultdict(lambda: defaultdict(tuple))

    with open(file) as f:
        for line in f:
            clinical_tx_status = False
            canonical_status = False
            gene, transcript, clinical_tx, canonical = line.strip().split()

            assert clinical_tx != "to_review", (
                f"{gene} has a 'to_review' status, please review it before "
                "importing the g2t file"
            )

            if not clinical_tx.startswith("not"):
                clinical_tx_status = True

            if not canonical.startswith("not"):
                canonical_status = True

            g2t[gene][transcript] = (clinical_tx_status, canonical_status)

    return g2t


def create_panelapp_dict(
    dump_folders: list, type_panels: list, single_genes: list
):
    """ Return list of dicts for the data stored in the panelapp dump folder

    Args:
        dump_folder (list): Folder(s) containing the panels
        hgmd_dict (dict): Dict of HGMD parsed data
        single_genes (list): List of single genes to be transformed into panels
                            Defaults to None

    Returns:
        list: List of dicts for the data stored in the panelapp dump folder
    """

    panelapp_dict = defaultdict(lambda: defaultdict(set))
    superpanel_dict = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(None)))
    )
    # The following dicts will contain a key called "check" for knowing whether
    # the entity has been seen before
    gene_dict = defaultdict(lambda: defaultdict(None))

    for dump_folder in dump_folders:
        if Path(dump_folder).is_dir():
            for file in os.listdir(dump_folder):
                panel_path = f"{dump_folder}/{file}"

                with open(panel_path) as f:
                    if file.endswith("_superpanel.tsv"):
                        # dealing with a superpanel
                        for line in f:
                            (
                                panel_id, panel_name, panel_version,
                                panel_signedoff, subpanel_id, subpanel, version
                            ) = line.strip().split("\t")

                            su_panel_dict = superpanel_dict[panel_id]
                            su_panel_dict["subpanels"][subpanel_id]["name"] = subpanel
                            su_panel_dict["subpanels"][subpanel_id]["id"] = subpanel_id
                            su_panel_dict["subpanels"][subpanel_id]["version"] = version
                            su_panel_dict["name"] = panel_name
                            su_panel_dict["version"] = panel_version
                            su_panel_dict["signedoff"] = panel_signedoff
                            su_panel_dict["type"] = get_panel_type(
                                type_panels, dump_folder
                            )
                    else:
                        # dealing with a normal panel
                        for line in f:
                            line = line.strip().split("\t")
                            (
                                panel_name, panel_id, version,
                                signedoff, entity_type
                            ) = line[0:5]

                            panel_dict = panelapp_dict[panel_id]
                            panel_dict["name"] = panel_name
                            panel_dict["version"] = version
                            panel_dict["signedoff"] = signedoff
                            panel_dict["type"] = get_panel_type(
                                type_panels, dump_folder
                            )

                            if entity_type == "gene":
                                gene, hgnc_id = line[5:]

                                panel_dict["genes"].add(hgnc_id)
                                gene_dict[hgnc_id]["check"] = False
                                gene_dict[hgnc_id]["symbol"] = gene

    # make the single genes from the test directory single gene panels
    for hgnc_id in single_genes:
        single_gene_id = f"{hgnc_id}_SG"
        panelapp_dict[single_gene_id]["name"] = f"{single_gene_id}_panel"
        # default panel version because if single gene panels change well...
        # they're not single gene panels anymore are they?
        panelapp_dict[single_gene_id]["version"] = "1.0"
        panelapp_dict[single_gene_id]["signedoff"] = None
        panelapp_dict[single_gene_id]["type"] = "single_gene"
        panelapp_dict[single_gene_id]["genes"].add(hgnc_id)

        gene_dict[hgnc_id]["check"] = False

    return panelapp_dict, superpanel_dict, gene_dict


def parse_test_directory(file: str):
    """ Parse the data in the National test directory

    Args:
        file (str): XLS of the National test directory

    Returns:
        tuple: Dict of clin_ind_id2clin_ind and dict of test_id2targets
    """

    clinind_data = defaultdict(lambda: defaultdict(str))

    xls = xlrd.open_workbook(file)
    sheet_with_tests = xls.sheet_by_name("R&ID indications")

    ci_dict = {}

    for row in range(sheet_with_tests.nrows):
        if row >= 2:
            (
                ci_id, ci, criteria, test_code,
                targets, method, clinical_group, comment
            ) = sheet_with_tests.row_values(row)

            if ci != "" and ci_id != "":
                ci_dict[ci_id] = ci
            else:
                ci_id, code = test_code.split(".")
                ci = ci_dict[ci_id]

            test_code = test_code.strip()

            if "panel" in method or "WES" in method or "Single gene" in method:
                clinind_data[test_code]["targets"] = targets.strip()
                clinind_data[test_code]["method"] = method.strip()
                clinind_data[test_code]["name"] = ci.strip()
                clinind_data[test_code]["version"] = file

    return clinind_data


def clean_targets(clinind_data: dict):
    """ Replace the methods from the XLS to abbreviation:
    WES -> P
    Panel -> P
    Single Gene -> G

    Args:
        clinind_data (dict): Dict of data from the test directory

    Returns:
        dict: Dict of dict for test2targets
    """

    clean_clinind_data = defaultdict(lambda: defaultdict(list))

    ci_to_remove = []

    for test_code in clinind_data:
        data = clinind_data[test_code]
        clinind = data["name"]
        targets = data["targets"]
        method = data["method"]
        version = data["version"]

        clean_clinind_data[test_code]["name"] = clinind
        clean_clinind_data[test_code]["version"] = version

        if "WES" in method:
            clean_clinind_data[test_code]["method"] = "P"

        elif "panel" in method:
            clean_clinind_data[test_code]["method"] = "P"

        elif "gene" in method:
            clean_clinind_data[test_code]["method"] = "G"

        cleaned_method = clean_clinind_data[test_code]["method"]

        clean_clinind_data[test_code]["gemini_name"] = (
            f"{test_code}_{clinind}_{cleaned_method}"
        )

        for indiv_target in targets.split(";"):
            indiv_target = indiv_target.strip()
            removed_msg = (
                f"{test_code} removed from considered tests. Text: "
                f"{indiv_target}"
            )

            if "Relevant" not in indiv_target:
                # Panels can have "As dictated by blabla" "As indicated by"
                # so I remove those
                if indiv_target.startswith("As "):
                    ci_to_remove.append(test_code)
                    output_to_loggers(removed_msg, "info", CONSOLE, UTILS)

                # check if the target has parentheses with numbers in there
                match = regex.search(r"(?P<panel_id>\(\d+\))", indiv_target)

                # it's a panel, parentheses detected, really reliable
                if match:
                    target_to_add = match.group("panel_id").strip("()")
                    clean_clinind_data[test_code]["panels"].append(
                        target_to_add
                    )

                # it's a single gene
                else:
                    target_to_add = hq_get_id(
                        indiv_target.strip(), verbose=False
                    )

                    if target_to_add is not None:
                        clean_clinind_data[test_code]["genes"].append(
                            target_to_add
                        )
                    else:
                        # only case where this happens is a
                        # As dictated by clinical indication case
                        ci_to_remove.append(test_code)
                        output_to_loggers(removed_msg, "info", CONSOLE, UTILS)
            else:
                ci_to_remove.append(test_code)
                output_to_loggers(removed_msg, "info", CONSOLE, UTILS)

        # handle the hard coded tests
        if test_code in hd_tests:
            # remove test_code from the clinical indication to remove list
            if test_code in ci_to_remove:
                ci_to_remove.remove(test_code)

            msg = f"{test_code} is added as a hardcoded test"
            output_to_loggers(msg, "info", CONSOLE, UTILS)
            clean_clinind_data[test_code]["panels"] = hd_tests[test_code]["panels"]
            clean_clinind_data[test_code]["gemini_name"] = hd_tests[test_code]["gemini_name"]
            clean_clinind_data[test_code]["tests"] = hd_tests[test_code]["tests"]
        else:
            # if they are not hardcoded tests, add the tests key for future use
            clean_clinind_data[test_code]["tests"] = []

        # convert default dict to dict because accessing absent key later on
        # will create the key with a list as default breaking what I'm doing
        # later
        clean_clinind_data[test_code] = dict(clean_clinind_data[test_code])

    # remove the clinical indication that have the Relevant panel/gene text
    for key in ci_to_remove:
        clean_clinind_data.pop(key, None)

    return clean_clinind_data


def parse_gemini_dump(gemini_dump: str):
    """ Parse the Gemini database dump

    Args:
        gemini_dump (str): Path to the gemini dump (CSV)

    Returns:
        OrderedDict: OrderedDict containing the sample2panels data
    """

    sample2panels = {}

    # windows encoding otherwise it breaks
    with open(gemini_dump, encoding="cp1252") as f:
        for index, line in enumerate(f):
            line = line.strip().split(",")

            if index == 0:
                headers = {
                    header: column
                    for column, header in enumerate(line)
                }
            else:
                # skip cancelled samples
                if line[headers["StatusDescription"]].strip() != "Cancelled Failed":
                    sample2panels.setdefault(
                        line[headers["ExomeNumber"]], []
                    ).append(
                        line[headers["PanelDescription"]]
                    )

    return OrderedDict(sorted(sample2panels.items(), key=lambda t: t[0]))


def gather_single_genes(clin_ind2targets: dict):
    """ Return list of all single genes tests

    Args:
        clin_ind2targets (dict): Dict of clinical indications

    Returns:
        list: List of single genes used in clinical indications
    """

    single_genes = []

    for test_code in clin_ind2targets:
        if "genes" in clin_ind2targets[test_code]:
            genes = clin_ind2targets[test_code]["genes"]
            single_genes += genes

    return single_genes


def get_clinical_indication_through_genes(
    session, meta, clinical_indications, hgnc_data
):
    """ Loop through given clinical indications database rows and get linked
        panels

    Args:
        session (SQLAlchemy Session): SQLAlchemy Session
        meta (SQLAlchemy Meta): SQLAlchemy Meta
        clinical_indications: rows of SQLAlchemy data from the clinical indications table
        hgnc_data (dict): Dict of parsed hgnc data from an HGNC dump

    Returns:
        dict: dict of clinical indications to dict of panels to genes
    """

    panel_tb = meta.tables["panel"]
    panel2features_tb = meta.tables["panel_features"]
    feature_tb = meta.tables["feature"]
    gene_tb = meta.tables["gene"]

    gemini2genes = defaultdict(lambda: defaultdict(lambda: set()))

    for ci, panels in clinical_indications.items():
        for panel_id, ci_version in panels:
            # query to get all genes from a panel id
            data = session.query(
                panel_tb.c.name, panel2features_tb.c.feature_id,
                panel2features_tb.c.panel_version, gene_tb.c.hgnc_id,
                panel_tb.c.panelapp_id
            ).join(panel2features_tb).join(feature_tb).join(gene_tb).filter(
                panel2features_tb.c.panel_id == panel_id
            ).all()

            # use the packaging package to parse the version and take the latest
            # version
            latest_version = get_latest_panel_version([d[2] for d in data])
            panel_genes = [(d[0], d[2], d[3], d[4]) for d in data]
            hgnc_ids = []

            for panel, panel_version, hgnc_id, panelapp_id in panel_genes:
                ci_info = f"{ci}:{panelapp_id}"
                if "|" in panel_version:
                    formatted_version = panel_version.split("|")
                else:
                    formatted_version = [panel_version, ""]

                # only get genes that are in the latest version of a given panel
                if formatted_version == latest_version:
                    # filter gene if it's RNA
                    if filter_out_gene(hgnc_data[hgnc_id], "locus_type", "RNA"):
                        continue

                    # get rid of mitochondrial genes
                    if filter_out_gene(
                        hgnc_data[hgnc_id], "approved_name", "mitochondrially encoded"
                    ):
                        continue

                    # remove TRAC and IGHM genes from genepanels and manifest
                    if hgnc_id in ["HGNC:12029", "HGNC:5541"]:
                        continue

                    hgnc_ids.append(hgnc_id)

            latest_version = "|".join(latest_version).strip("|")

            gemini2genes[ci_info][f"{panel}_{latest_version}"].update(hgnc_ids)

    return gemini2genes


def parse_panel_form(panel_form: str):
    """ Parse the panel excel form

    Args:
        panel_form (str): Excel panel form

    Returns:
        dict: Dict of dict containing data for clinical indication, panel, genes
    """

    # read in the excel sheets of interest
    metadata_df = pd.read_excel(panel_form, sheet_name="Admin details")
    gene_df = pd.read_excel(panel_form, sheet_name="Gene list")

    validate_panel_form(metadata_df, gene_df)

    # get data from hardcoded locations in the metadata sheet
    clinical_indication = metadata_df.iat[2, 1]
    panel = metadata_df.iat[3, 1]
    panel_version = ""

    if pd.notna(metadata_df.iat[4, 1]):
        panel_version = re.sub("[^0-9^.]", "", metadata_df.iat[4, 1])

    add_on = metadata_df.iat[6, 1]
    day, month, year = [int(ele) for ele in metadata_df.iat[9, 1].split("/")]
    ci_version = datetime.date(year, month, day).strftime("%Y-%m-%d")

    if pd.notna(add_on):
        # add on clinical indication version
        ci_version = f"AO_{ci_version}"
        add_on_bool = True
    else:
        # bespoke clinical indication version
        ci_version = f"BP_{ci_version}"
        add_on = None
        add_on_bool = False

    # get unique hgnc ids from the gene sheet
    genes = set(gene_df.iloc[0:, 1])

    # setup the data for future use
    data = {
        clinical_indication: {
            "version": ci_version,
            "add_on": add_on,
            "panels": {
                panel: {
                    "genes": genes,
                    "version": panel_version
                }
            }
        }
    }

    return data, add_on_bool


def get_latest_clinical_indication_data(query_result):
    """ Get the latest clinical indication data using ci_version field
    Args:
        query_result (SQLAlchemy query): Result of SQLAlchemy query
    Returns:
        dict: Dict containing the most recent clinical indication and its 
        linked panels
    """

    ci2panels = {}

    # get latest version of clinical indication
    for ci in query_result:
        gemini_name, panel_id, ci_version = ci
        source, version = ci_version.split("_")
        dated_version = datetime.date.fromisoformat(version)

        # compare dates between stored date and new date
        if gemini_name in ci2panels:
            # all dates stored should have the same date so select first
            # element of list of tuples and get the date of the ci2panel link
            stored_source, stored_version = ci2panels[gemini_name][0][1].split("_")
            dated_stored_version = datetime.date.fromisoformat(stored_version)

            # if new date is higher replace all stored dates
            if dated_version > dated_stored_version:
                ci2panels[gemini_name] = [(panel_id, ci_version)]
            # date identical we need to store the incoming date and panel id
            elif dated_version == dated_stored_version:
                ci2panels[gemini_name].append((panel_id, ci_version))

        # unseen clinical indication
        else:
            ci2panels[gemini_name] = [(panel_id, ci_version)]

    return ci2panels


def validate_panel_form(metadata_df, gene_df):
    """ Validate panel form

    Args:
        metadata_df (Pandas dataframe): Dataframe containing metadata for the
        panel
        gene_df (Pandas dataframe): Dataframe with genes of the panel
    """

    assert all(
        [
            pd.notna(metadata_df.iat[2, 1]), pd.notna(metadata_df.iat[3, 1]),
            pd.notna(metadata_df.iat[9, 1])
        ]
    ), "Some essential fields in the metadata sheet are absent"

    assert set(gene_df.iloc[0:, 1]), "No genes are in the gene sheet"


def get_latest_panel_version(panel_versions):
    """ Get latest version of a panel

    Args:
        panel_versions (list): List of all versions of a particular panel

    Returns:
        list: Latest version as list to accomodate add on versions
    """

    panel_add_on_versions = []

    # parse panel versions that we can get the latest stored
    for panel_version in panel_versions:
        if "|" in panel_version:
            formatted_version = panel_version.split("|")
        else:
            formatted_version = [panel_version, ""]

        panel_add_on_versions.append(formatted_version)

    latest_version = None

    for panel_version, add_on_version in panel_add_on_versions:
        panel_version = version.parse(panel_version)
        add_on_version = version.parse(add_on_version)

        if latest_version:
            # panel version is higher
            if latest_version[0] < panel_version:
                latest_version = [panel_version, add_on_version]

            elif latest_version[0] == panel_version:
                # panel versions are equal but add on version
                # is higher
                if latest_version[1] < add_on_version:
                    latest_version = [panel_version, add_on_version]
        else:
            # looking at first version so store first value
            latest_version = [panel_version, add_on_version]

    latest_version_list = [str(latest_version[0]), str(latest_version[1])]

    return latest_version_list


def parse_panelapp_update_file(panelapp_file: str):
    """ Parse panelapp file

    Args:
        panelapp_file (str): File containing panelapp panel id to update and
        their new version

    Returns:
        list: List of dict for panelapp info for panels to update
    """

    data = []

    with open(panelapp_file) as f:
        for line in f:
            panelapp_id, version = line.strip().split()
            data.append({"panelapp_id": panelapp_id, "version": version})

    return data


def parse_json_file(json_file: str):
    """ Parse json file and return dict type object

    Args:
        json_file (str): Path to the json file

    Returns:
        dict: Dict containing the data in the json
    """

    with open(json_file) as f:
        return json.load(f)


def get_reference_id(session, meta, reference: str):
    """ Get the reference id for a given reference build name

    Args:
        session (SQLAlchemy session obj): SQLAlchemy session
        meta (SQLAlchemy meta obj): SQLAlchemy meta
        reference (str): Reference id

    Returns:
        int: Reference id corresponding to the reference build name
    """
    ref_tb = meta.tables["reference"]

    ref_query = session.query(
        ref_tb.c.id
    ).filter(
        ref_tb.c.name == reference
    ).one()

    assert len(ref_query) == 1, (
        "The reference query didn't return only one row:\n"
        f"{','.join([query for query in ref_query])}"
    )

    return ref_query[0]
