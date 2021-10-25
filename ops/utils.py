from collections import defaultdict, OrderedDict
import datetime
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

from .hardcoded_tests import tests as hd_tests
from .logger import setup_logging, output_to_loggers


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

            if not clinical_tx.startswith("not"):
                clinical_tx_status = True

            if not canonical.startswith("not"):
                canonical_status = True

            g2t[gene][transcript] = (clinical_tx_status, canonical_status)

    return g2t


def parse_hgnc_dump(hgnc_file: str):
    """ Parse the hgnc dump and return a dict of the data in the dump

    Args:
        hgnc_file (str): Path to the hgnc file

    Returns:
        dict: Dict of hgnc data
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
                sample2panels.setdefault(
                    line[headers["ExomeNumber"]], []
                ).append(
                    line[headers["PanelDescription"]]
                )

    return OrderedDict(sorted(sample2panels.items(), key=lambda t: t[0]))


def get_django_json(model: str, pk: str, fields: dict):
    """ Return understandable django json

    Args:
        model (str): Name of the model where to load data to
        pk (str): Id of the given data
        fields (dict): Dict to specify data to give for that id

    Returns:
        dict: Understandable django json
    """

    return {
        "model": f"panel_database.{model}",
        "pk": f"{pk}",
        "fields": fields
    }


def gather_ref_django_json(references: list, pk: int):
    """ Create the objects for the references

    Args:
        references (list): List of the hardcoded references
        pk (int): Primary key to start with for the references.

    Returns:
        list: List of the json objects to be imported
    """

    reference_json = []

    # Create the list of reference table
    for ref_id, ref in enumerate(references, pk+1):
        reference_json.append(
            get_django_json("Reference", ref_id, {"name": ref})
        )

    return reference_json


def gather_panel_types_django_json(panel_types: list, pk: int):
    """ Create the objects for the panel types

    Args:
        panel_types (list): List of the hardcoded panel types
        pk (int): Primary key to start with for the panel types.

    Returns:
        list: List of the json objects to be imported
    """

    paneltype_json = []

    for choice_id, choice in enumerate(panel_types, pk+1):
        paneltype_json.append(
            get_django_json("PanelType", choice_id, {"type": choice})
        )

    return paneltype_json


def gather_feature_types_django_json(feature_types: list, pk: int):
    """ Create the objects for the feature types

    Args:
        feature_types (list): List of the hardcoded feature types
        pk (int): Primary key to start with for the feature types.

    Returns:
        list: List of the json objects to be imported
    """

    featuretype_json = []

    for choice_id, choice in enumerate(feature_types, pk+1):
        featuretype_json.append(
            get_django_json("FeatureType", choice_id, {"type": choice})
        )

    return featuretype_json


def get_existing_object_pk(
    list_existing_objects: list, field_to_query: str, value: str
):
    """ Get the primary key of an object obtained using given field and value

    Args:
        list_existing_objects (list): List of json objects
        field_to_query (str): Field to query
        value (str): Value for given field

    Raises:
        Exception: If object cannot be found
        Exception: If query returns 2 or more objects
        Exception: Check if the code doesn't do anything weird

    Returns:
        str: Primary key of object
    """

    object_to_return = [
        obj["pk"]
        for obj in list_existing_objects
        if obj["fields"][field_to_query] == value
    ]

    if object_to_return != [] and len(object_to_return) == 1:
        return object_to_return[0]

    elif object_to_return == []:
        msg = (
            f"Couldn't find object using field {field_to_query} and "
            f"value {value}"
        )
        UTILS.error(msg)
        raise Exception(msg)

    elif len(object_to_return) >= 2:
        msg = (
            "Ambiguous search found more than 1 result using field "
            f"{field_to_query} and value {value}: {object_to_return}"
        )
        UTILS.error(msg)
        raise Exception(msg)

    else:
        msg = (
            f"Querying {field_to_query} using {value} returns "
            f"{object_to_return} which is not expected"
        )
        UTILS.error(msg)
        raise Exception(msg)


def get_links(list_existing_links: list, field_to_query: str, value: str):
    """ Return the link object between 2 tables

    Args:
        list_existing_links (list): List of json of the links
        field_to_query (str): Field to query in the json
        value (str): Value of the field to query

    Raises:
        Exception: If the query doesn't return any result
        Exception: Check if the code doesn't do anything weird

    Returns:
        list: List of json matching the query
    """

    objects_to_return = [
        obj
        for obj in list_existing_links
        if obj["fields"][field_to_query] == value
    ]

    if objects_to_return != []:
        return objects_to_return

    elif objects_to_return == []:
        msg = (
            f"Couldn't find object using field {field_to_query} and "
            f"value {value}"
        )
        UTILS.error(msg)
        raise Exception(msg)

    else:
        msg = (
            f"Querying {field_to_query} using {value} returns "
            f"{objects_to_return} which is not expected"
        )
        UTILS.error(msg)
        raise Exception(msg)


def gather_panel_data_django_json(
    panelapp_dict: dict, gene_dict: dict, featuretype_json: list,
    paneltype_json: list, pk_dict: dict
):
    """ Create the panel object in json

    Args:
        panelapp_dict (dict): Dict from panelapp data
        gene_dict (dict): Dict containing gene data
        featuretype_json (list): List of json with the feature type objects
        paneltype_json (list): List of json with the panel type objects
        pk_dict (dict): Dict of primary keys

    Returns:
        tuple: Tuple containing list of json for panels, features,
                panel2features, feature_types, genes and primary key dict
    """

    panel_json = []
    panelfeature_json = []
    feature_json = []

    gene_json = []

    # Create the list for panel, panel_gene, gene
    for panel_pk, panelapp_id in enumerate(panelapp_dict, pk_dict["panel"]+1):
        panel_dict = panelapp_dict[panelapp_id]
        # Get the primary key of the appropriate panel type
        panel_type_pk = get_existing_object_pk(
            paneltype_json, "type", panel_dict["type"]
        )

        if panelapp_id.endswith("_SG"):
            panelapp_id = ""

        panel_fields = {
                "panelapp_id": panelapp_id, "name": panel_dict["name"],
                "panel_type_id": panel_type_pk
        }

        panel_json.append(get_django_json("Panel", panel_pk, panel_fields))

        # go through the genes of the panel
        for hgnc_id in panel_dict["genes"]:
            gene_data = gene_dict[hgnc_id]
            # Get the primary key of the gene feature type
            gene_feature_pk = get_existing_object_pk(
                featuretype_json, "type", "gene"
            )

            # we haven't encountered this gene and added it to the json list
            # so we go ahead and create it
            if gene_data["check"] is False:
                # Add the gene to the gene table
                pk_dict["gene"] += 1
                # Store the gene pk in another variable
                gene_pk = pk_dict["gene"]

                gene_fields = {
                    "hgnc_id": hgnc_id
                }
                gene_json.append(get_django_json("Gene", gene_pk, gene_fields))

                # Mark the gene as seen
                gene_data["check"] = True

                # Create feature
                pk_dict["feature"] += 1
                feature_pk = pk_dict["feature"]
                feature_json.append(
                    add_feature(
                        feature_pk, gene_feature_pk,
                        gene_id=gene_pk
                    )
                )
            else:
                # we have seen the gene so we get references to the gene obj
                # and the feature obj
                gene_pk = get_existing_object_pk(
                    gene_json, "hgnc_id", hgnc_id
                )
                feature_pk = get_existing_object_pk(
                    feature_json, "gene_id", int(gene_pk)
                )

            # Create panel_feature link
            pk_dict["panel_feature"] += 1
            panelfeature_json.append(
                add_panel_feature(
                    pk_dict["panel_feature"], panel_pk, panel_dict["version"],
                    feature_pk
                )
            )

    # get pk of last panel created to get a starting point for creating
    # superpanel panels
    pk_dict["panel"] = panel_pk

    return (
        panel_json, feature_json, panelfeature_json, gene_json, pk_dict
    )


def gather_superpanel_data_django_json(
    superpanel_dict: dict, panel_json: list, paneltype_json: list,
    panelfeature_json: list, pk_dict: dict
):
    """ Add superpanels as panels in the panel_json list

    Args:
        superpanel_dict (dict): Superpanel dict from panelapp data
        panel_json (list): List of json object for panel
        paneltype_json (list): List of json object for panel types
        panelfeature_json (list): List of json object for panel features
        pk_dict (dict): Dict of primary keys

    Returns:
        list: List comprised of the modified panel json list +
                modified panel features list
    """

    # pk is the latest panel created + 1
    for superpanel_pk, superpanel_id in enumerate(
        superpanel_dict, pk_dict["panel"]+1
    ):
        superpanel_data = superpanel_dict[superpanel_id]

        # Get the primary key of the appropriate panel type
        panel_type_pk = get_existing_object_pk(
            paneltype_json, "type", superpanel_data["type"]
        )
        panel_fields = {
                "panelapp_id": superpanel_id, "name": superpanel_data["name"],
                "panel_type_id": panel_type_pk
        }
        panel_json.append(
            get_django_json("Panel", superpanel_pk, panel_fields)
        )

        # go through superpanel subpanels
        # the idea is to bypass subpanels when creating the panel2features
        # elements
        for subpanel in superpanel_data["subpanels"]:
            subpanel_id = superpanel_data["subpanels"][subpanel]["id"]

            # get the primary key of the subpanel
            subpanel_pk = get_existing_object_pk(
                panel_json, "panelapp_id", subpanel_id
            )

            # Use the already existing links from normal panels
            # to create the superpanel links to the features
            panel2features = get_links(
                panelfeature_json, "panel_id", int(subpanel_pk)
            )

            # go through the panel2features object of the specific subpanel
            for panel_feature in panel2features:
                # get the pk of the feature
                feature_pk = panel_feature["fields"]["feature_id"]

                # check if the link already exists i.e. superpanel has
                # subpanels that link to the same gene
                already_existing_link = [
                    obj
                    for obj in panelfeature_json
                    if int(obj["fields"]["panel_id"]) == int(superpanel_pk) and
                    int(obj["fields"]["feature_id"]) == int(feature_pk) and
                    str(obj["fields"]["panel_version"]) == str(
                        superpanel_data["version"]
                    )
                ]

                # if that link doesn't exist need to create it
                if already_existing_link == []:
                    pk_dict["panel_feature"] += 1
                    panelfeature_json.append(
                        add_panel_feature(
                            pk_dict["panel_feature"], superpanel_pk,
                            superpanel_data["version"], feature_pk
                        )
                    )

    return panel_json, panelfeature_json


def gather_clinical_indication_data_django_json(
    clin_ind2targets: dict, panel_json: list, pk_dict: dict
):
    """ Create the clinical indication and the needed link tables

    Args:
        clin_ind2targets (dict): Dict of data obtained from the test directory
        panel_json (list): List of json objects for panels
        pk_dict (dict): Dict of primary keys

    Raises:
        Exception: If hardcoded test is ambiguous (i.e. R266 points to R80 
                    for example). I'm using the R80 code to find the specific
                    test code i.e. R80.1 but this can cause problems if R80
                    has R80.1 and R80.2. This exception is for that because I
                    don't know how to handle that yet
        Exception: If couldn't find panels using the test_code data in the dict

    Returns:
        list: List of the clinical indication json + ci to panel json
    """

    clinical_indication_json = []
    clinical_indication2panels_json = []

    # go through the test codes
    for clin_ind_pk, test_code in enumerate(
        clin_ind2targets, pk_dict["clinind"]+1
    ):
        name = clin_ind2targets[test_code]["name"]
        gemini_name = clin_ind2targets[test_code]["gemini_name"]
        tests = clin_ind2targets[test_code]["tests"]
        version = clin_ind2targets[test_code]["version"]

        if gemini_name != "":
            clinind_fields = {
                "code": test_code,
                "name": name,
                "gemini_name": gemini_name,
            }
            clinical_indication_json.append(
                get_django_json(
                    "ClinicalIndication", clin_ind_pk, clinind_fields
                )
            )

        panels_gathered = []

        # gel retired panels associated with the clinical indication
        # try and gather the panels that gel decided on to assign them to the
        # original deprecated one
        if tests != []:
            for test in tests:
                # try and gather test code because of course
                # they don't specify which of the tests to use in panelapp
                hd_tests = [
                    r_code for r_code in clin_ind2targets if test in r_code
                ]

                # the clinical indication has multiple types of tests
                # (aka multiple panels or panel + single gene)
                if len(hd_tests) > 1:
                    # not sure how to handle this case
                    msg = (
                        f"Clinical indication {test_code} points to multiple "
                        f"other clinical indications: {tests}. "
                        "Those clinical indications point to multiple "
                        f"possible tests: {hd_tests}"
                    )
                    UTILS.error(msg)
                    raise Exception((
                        "Raising this error for when it is going to occur. "
                        "Check the utils log"
                    ))
                else:
                    # only one test code gathered for the clinical indication
                    # add genes/panels to the panels that are going to be
                    # associated with the test_code
                    if "genes" in clin_ind2targets[hd_tests[0]]:
                        panels_gathered += clin_ind2targets[hd_tests[0]]["genes"]

                    if "panels" in clin_ind2targets[hd_tests[0]]:
                        panels_gathered += clin_ind2targets[hd_tests[0]]["panels"]
        else:
            # normal test not hardcoded
            if "genes" in clin_ind2targets[test_code]:
                panels_gathered = clin_ind2targets[test_code]["genes"]

            if "panels" in clin_ind2targets[test_code]:
                panels_gathered = clin_ind2targets[test_code]["panels"]

        if panels_gathered == []:
            msg = f"Couldn't find panels for {test_code}"
            UTILS.warning(msg)
            raise Exception(msg)

        # Create test_panel json
        for panel in panels_gathered:
            if regex.match(r"[0-9*]", panel):
                # it's a panelapp id
                panel_pk = get_existing_object_pk(
                    panel_json, "panelapp_id", panel
                )
            else:
                # it's a gene panel name thingy (HGNC:[0-9]_SG)
                gene_panel_name = f"{panel}_SG_panel"
                panel_pk = get_existing_object_pk(
                    panel_json, "name", gene_panel_name
                )

            pk_dict["clinind_panels"] += 1
            clinind_panels_fields = {
                "clinical_indication_id": clin_ind_pk, "panel_id": panel_pk,
                "ci_version": version
            }
            clinical_indication2panels_json.append(
                get_django_json(
                    "ClinicalIndicationPanels", pk_dict["clinind_panels"],
                    clinind_panels_fields
                )
            )

    return clinical_indication_json, clinical_indication2panels_json


def gather_transcripts(
    gene_json: list, reference_json: list, g2t_data: dict, pk_dict: dict
):
    """ Create the transcripts and the needed link tables

    Args:
        gene_json (list): List of json objects for panels
        reference_json (list): List of json objects for panels
        g2t_data (dict): Dict of g2t data
        pk_dict (dict): Primary key dict

    Returns:
        tuple: List of json objects for transcripts and genes2transcripts
    """

    transcript_json = []
    genes2transcripts_json = []

    date = str(datetime.date.today())

    gene_objs = [obj for obj in gene_json]

    # loop through gene objs
    for gene_obj in gene_objs:
        hgnc_id = gene_obj["fields"]["hgnc_id"]
        # get all available transcripts from nirvana
        all_transcripts = g2t_data[hgnc_id]

        # loop through the transcripts
        for transcript, statuses in all_transcripts.items():
            pk_dict["transcript"] += 1
            refseq_base, refseq_version = transcript.split(".")
            clinical_tx, canonical_status = statuses

            # create the transcript obj
            transcript_json.append(
                get_django_json(
                    "Transcript", pk_dict["transcript"], {
                        "refseq_base": refseq_base,
                        "version": refseq_version,
                        "canonical": canonical_status
                    }
                )
            )

            gene_pk = gene_obj["pk"]
            ref_pk = [
                obj["pk"]
                for obj in reference_json
                if obj["fields"]["name"] == "GRCh37"
            ][0]

            pk_dict["g2t"] += 1
            # create the g2t obj
            genes2transcripts_json.append(
                get_django_json(
                    "Genes2transcripts", pk_dict["g2t"], {
                        "gene_id": gene_pk, "reference_id": ref_pk,
                        "transcript_id": pk_dict["transcript"],
                        "date": date, "clinical_transcript": clinical_tx
                    }
                )
            )

    return transcript_json, genes2transcripts_json


def add_feature(feature_pk: int, feature_type_pk: int, **links):
    """ Create feature object to add to the json list

    Args:
        feature_pk (int): Feature primary key to use for the object
        feature_type_pk (int): Feature type primary key
        links (kwargs): Fields to be added to the features

    Returns:
        dict: Dict describing the feature object
    """

    feature_fields = {
        "feature_type_id": feature_type_pk,
        "gene_id": None
    }

    for field, value in links.items():
        feature_fields[field] = value

    return get_django_json("Feature", feature_pk, feature_fields)


def add_panel_feature(
    pk: int, panel_pk: int, version: str, feature_pk: int,
    description: str = ""
):
    """ Return a panel feature object

    Args:
        pk (int): Primary key for the panel feature
        panel_pk (int): Primary key for the panel
        version (str): Panel version to be recorded
        feature_pk (int): Primary key for the feature
        description (str): Changes from old to new panel version

    Returns:
        dict: Dict describing the panel features object
    """

    return get_django_json(
        "PanelFeatures", pk,
        {
            "panel_version": version,
            "description": description,
            "panel_id": panel_pk,
            "feature_id": feature_pk
        }
    )


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

    for ci in clinical_indications:
        gemini_name, panel_id = ci

        # query to get all genes from a panel id
        data = session.query(
            panel_tb.c.name, panel2features_tb.c.feature_id,
            panel2features_tb.c.panel_version, gene_tb.c.hgnc_id
        ).join(panel2features_tb).join(feature_tb).join(gene_tb).filter(
            panel2features_tb.c.panel_id == panel_id
        ).all()

        # use the packaging package to parse the version and take the latest
        # version
        latest_version = max([version.parse(d[2]) for d in data])
        panel_genes = [(d[0], d[2], d[3]) for d in data]
        hgnc_ids = []

        for panel, panel_version, hgnc_id in panel_genes:
            # only get genes that are in the latest version of a given panel
            if version.parse(str(panel_version)) == latest_version:
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

        gemini2genes[gemini_name][f"{panel}_{latest_version}"].update(hgnc_ids)

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

    # get data from hardcoded locations in the metadata sheet
    clinical_indication = metadata_df.iat[2, 1]
    ci_version = metadata_df.iat[9, 1].strftime("%Y-%m-%d")
    panel = metadata_df.iat[3, 1]
    panel_version = re.sub("[^0-9^.]", "", metadata_df.iat[4, 1])
    add_on = metadata_df.iat[6, 1]

    if add_on:
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
