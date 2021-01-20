from collections import defaultdict
import regex

from sqlalchemy import distinct

from .logger import setup_logging, output_to_loggers
from .utils import get_date


CONSOLE, CHECK = setup_logging("check")


def check_db(
    folder: str, session, meta, panelapp_dict: dict, superpanel_dict: dict,
    ci2targets: dict
):
    """ Check that the data in the panelapp dump is the same as what's in the
    db

    Args:
        folder (str): Folder in which panels are stored
        session (SQLAlchemy session): Session object
        meta (SQLAlchemy MetaData): Metadata object
        panelapp_dict (dict): Dict of data from panelapp for panels
        superpanel_dict (dict): Dict of data from panelapp for superpanel
        ci2targets (dict): Dict of data from the test directory
    """

    msg = f"Checking database against {folder}"
    output_to_loggers(msg, CONSOLE, CHECK)

    # setup the gathering of data from the following tables
    ci_tb = meta.tables["clinical_indication"]
    ci_panels_tb = meta.tables["clinical_indication_panels"]
    feature_type_tb = meta.tables["feature_type"]
    panel_type_tb = meta.tables["panel_type"]
    panel_tb = meta.tables["panel"]
    feature_tb = meta.tables["feature"]
    panel_features_tb = meta.tables["panel_features"]
    gene_tb = meta.tables["gene"]

    error_tracker = False

    # check the clinical indications structure
    ci_errors = check_clinical_indications(
        session, ci2targets, panelapp_dict, superpanel_dict, ci_tb,
        ci_panels_tb, panel_tb, panel_features_tb
    )

    # check if errors in the clinical indications bit
    for error in ci_errors:
        error_tracker = True
        CHECK.error(error)

    global_panel_errors, panel_errors = check_panels(
        session, panelapp_dict, superpanel_dict, panel_type_tb,
        panel_features_tb, panel_tb, feature_tb, gene_tb, feature_type_tb
    )

    # check if errors in the total number of panels
    if global_panel_errors is not None:
        for error in global_panel_errors:
            error_tracker = True
            CHECK.error(error)

    # check if errors in the individual panels
    for panelapp_id, logged_data in panel_errors.items():
        for error_type, errors in logged_data.items():
            for error in errors:
                error_tracker = True
                CHECK.error(
                    f"Panelapp_id {panelapp_id}-{error_type}: {error}"
                )

    # check if there's an error to raise the exception
    if error_tracker is True:
        raise Exception(
            "Error(s) in the check has been found. Check the check_log for "
            "more details"
        )
    else:
        msg = (
            f"Checking against panelapp dump from {folder} against database "
            f"on {get_date()}: correct"
        )
        output_to_loggers(msg, CONSOLE, CHECK)

    return True


def check_clinical_indications(
    session, ci2targets: dict, panel_dict: dict, superpanel_dict: dict,
    ci_tb, ci_panels_tb, panel_tb, panel_feature_tb
):
    """ Check the structure of clinical indications in the database

    Args:
        session (SQLAlchemy.session): SQL Alchemy session
        ci2targets (dict): Dict with clinical indication data from test directory 
        panel_dict (dict): Dict with panel data from panelapp
        superpanel_dict (dict): Dict with superpanel data from panelapp
        ci_tb: SQL Alchemy queryable table for clinical indication
        ci_panels_tb: SQL Alchemy queryable table for clinical indication panels
        panel_tb: SQL Alchemy queryable table for panels
        panel_feature_tb: SQL Alchemy queryable table for panel to features

    Returns:
        list: List of error messages
    """

    log = []

    db_ci = session.query(ci_tb).all()

    # check if the number of tests is the same between the excel and the
    # database
    if len(ci2targets) != len(db_ci):
        msg = (
            "Number of tests in the test directory and the database different"
            f": {len(ci2targets)} (test) vs {len(db_ci)} (db)"
        )
        log.append(msg)

    # loop through the rows in the database
    for ci_row in db_ci:
        ci_pk, ci_id, name, version, gemini_name = ci_row

        # check if the test_code is in the test directory data
        if ci_id not in ci2targets:
            msg = (
                f"Clinical indication {ci_id} doesn't exist in the "
                "test directory"
            )
            log.append(msg)
            continue

        # assign data of the clinical indication
        data = ci2targets[ci_id]

        # check the attributes for the test
        if (
            data["name"] != name or data["version"] != version or
            data["gemini_name"] != gemini_name
        ):
            msg = (
                "Discrepancy between data gathered from test directory and "
                "stored detected please check the logs for more info"
            )
            log.append(msg)
            log.append(data)
            log.append(ci_row[1:])

        features = set()
        hd_test_data = []

        # handle stupid hardcoded clinical indications that now point to
        # other clinical indications
        if data["tests"] != []:
            hd_tests = []
            # gather panels for the now retired test using the panels for the
            # new tests
            hd_panels = set()

            # find the specific test codes using the clinical indication id
            for test in data["tests"]:
                for r_code in ci2targets:
                    if test in r_code:
                        hd_tests.append(r_code)

            # go through those test codes to find genes and panels linked to
            # the tests to finally link back to the original clinical
            # indication
            for hd_test in hd_tests:
                hd_test_data = ci2targets[hd_test]

                if "genes" in hd_test_data:
                    features.update(hd_test_data["genes"])

                if "panels" in hd_test_data:
                    hd_panels.update(hd_test_data["panels"])

                    for panel in hd_test_data["panels"]:
                        # assuming there's not going to be a superpanel for now
                        features.update(panel_dict[panel]["genes"])
        else:
            # normal test, check if it has single genes
            if "genes" in data:
                features.update(data["genes"])

            # normal test, check if it has panels
            if "panels" in data:
                for panel in data["panels"]:
                    # if the panel is a superpanel we want to get the genes
                    # from the subpanels associated to the superpanel and link
                    # it back to the superpanel
                    if panel in superpanel_dict:
                        for subpanel in superpanel_dict[panel]["subpanels"]:
                            features.update(panel_dict[subpanel]["genes"])

                    # normal panel, get all the genes for the panels associated
                    # with the test
                    else:
                        features.update(panel_dict[panel]["genes"])

        # get all the panel ids from the clinical indication primary key
        db_panels = session.query(panel_tb.c.id).join(ci_panels_tb).filter(
            ci_panels_tb.c.clinical_indication_id == ci_pk
        ).all()

        # get all the features using the previously acquired panel ids
        db_features = session.query(
            distinct(panel_feature_tb.c.feature_id)
        ).filter(
            panel_feature_tb.c.panel_id.in_(db_panels)
        ).all()

        # check if the nb of features gathered for the clinical indication
        # is equal to the nb of features associated to the clinical indication
        # in the db
        if len(features) != len(db_features):
            msg = (
                f"Clinical_indication {ci_pk}: Number of panels gathered "
                f"({len(features)}) is not equal to the amount stored "
                f"({len(db_features)})"
            )
            log.append(msg)

            if "panels" in data:
                log.append("Genes in panels:")
                log.append(features)

            if "genes" in data:
                log.append("Single genes")
                log.append(data["genes"])

            log.append("Links stored in the database")
            log.append(db_features)

        msg = (
            f"{name}: Panelapp id not present in data gathered from the "
            "test directory"
        )

        # get all the links from clinical indication to panels
        db_ci_link = session.query(ci_panels_tb).filter(
            ci_panels_tb.c.clinical_indication_id == ci_pk
        ).all()

        # go through the clinical indication to panels links
        for link in db_ci_link:
            link_pk, ci_pk, panel_pk = link

            # get the panelapp id of the link
            panelapp_id = session.query(panel_tb.c.panelapp_id).filter(
                panel_tb.c.id == panel_pk
            ).one()[0]

            # check whether it's a single gene panel or a normal panel
            if regex.match(r"[0-9+]", panelapp_id):
                # its a panelapp id
                # check if the clinical indication contains the panel
                if data["tests"] != []:
                    if panelapp_id not in hd_panels:
                        log.append(msg)
                        log.append(f"{panelapp_id} not in {data['panels']}")

                elif panelapp_id not in data["panels"]:
                    log.append(msg)
                    log.append(f"{panelapp_id} not in {data['panels']}")
            else:
                # its a gene panel
                # check if the clinical indication contains the single gene
                # panel
                if panelapp_id[:-3] not in data["genes"]:
                    log.append(msg)
                    log.append(f"{panelapp_id} not in {data['genes']}")

    return log


def check_panels(
    session, panelapp_dict, superpanel_dict, panel_type_tb,
    panel_features_tb, panel_tb, feature_tb, gene_tb, feature_type_tb
):
    """ Check if the panel structure in the database is correct

    Args:
        session (SQL Alchemy session): SQL Alchemy session
        panelapp_dict (dict): Dict with panel data from panelapp
        superpanel_dict (dict): Dict with superpanel data from panelapp
        panel_type_tb: SQL Alchemy queryable table for panel type
        panel_features_tb: SQL Alchemy queryable table for panel to features
        panel_tb: SQL Alchemy queryable table for panels
        feature_tb: SQL Alchemy queryable table for features
        gene_tb: SQL Alchemy queryable table for genes
        feature_type_tb: SQL Alchemy queryable table for feature types

    Raises:
        Exception: If the panelapp id is not in the panelapp data dump

    Returns:
        tuple: str, list for error at the total nb of panel, errors at the
                    panel level
    """

    nb_error = None
    panel_log = defaultdict(lambda: defaultdict(list))

    # get all panels stored in the db
    db_panels = session.query(panel_tb).all()
    # number of regular panels (single gene panel included) + superpanels
    total_nb_panels = len(panelapp_dict) + len(superpanel_dict)

    if total_nb_panels != len(db_panels):
        msg = (
            "Number of panels in the panelapp dump and the database different"
            f": {len(panelapp_dict)} (panel) + {len(superpanel_dict)} "
            f"(superpanel) vs {len(db_panels)} (db)"
        )
        nb_error = msg

    # loop through stored panels
    for panel_row in db_panels:
        (
            panel_pk, panelapp_id, name, version, panel_type_pk, reference_pk
        ) = panel_row

        # if not in panelapp dict or superpanel dict --> panel imported is not
        # in panelapp anymore
        if (
            panelapp_id not in panelapp_dict and
            panelapp_id not in superpanel_dict
        ):
            msg = (
                f"Panel {panelapp_id} doesn't exist in the panelapp data"
            )
            panel_log[panelapp_id]["errors"].append(msg)
            continue

        # check whether the db panel is a "regular" panel or a superpanel
        if panelapp_id in panelapp_dict:
            panel_data = panelapp_dict[panelapp_id]
            hgnc_ids = panel_data["genes"]
        elif panelapp_id in superpanel_dict:
            panel_data = superpanel_dict[panelapp_id]

            # need to gather genes in the superpanel using the subpanels
            hgnc_ids = set()

            for subpanel in superpanel_dict[panelapp_id]["subpanels"]:
                hgnc_ids.update(panelapp_dict[subpanel]["genes"])

        else:
            # should have been caught by the check just before
            raise Exception(
                f"Panel {panelapp_id} broke through the check. "
                "He's too powerful, let him through"
            )

        # check if the attributes stored are correct
        if name != panel_data["name"] or version != panel_data["version"]:
            msg = (
                f"Data associated with the panel {panelapp_id} is not "
                "correct. Check the logs for more info"
            )
            panel_log[panelapp_id]["errors"].append(msg)
            panel_log[panelapp_id]["errors"].append(
                f"{name} != {panel_data['name']}"
            )
            panel_log[panelapp_id]["errors"].append(
                f"{version} != {panel_data['version']}"
            )

        # get the panel type
        panel_type = session.query(panel_type_tb.c.type).filter(
            panel_type_tb.c.id == panel_type_pk
        ).one()[0]

        # check if it matches the one gathered in panelapp data
        if panel_type != panel_data["type"]:
            msg = (
                f"Panel type in the panelapp dump ({panel_data['type']}) and "
                f"stored in the db ({panel_type}) are different"
            )
            panel_log[panelapp_id]["errors"].append(msg)

        # get all the links to the feature table using the panel primary key
        db_panel2features = session.query(
            panel_features_tb.c.feature_id
        ).filter(
            panel_features_tb.c.panel_id == panel_pk
        ).all()

        # check the links
        feature_log = check_panel2features(
            session, db_panel2features, hgnc_ids, feature_tb, gene_tb,
            feature_type_tb
        )

        panel_log[panelapp_id]["feature_errors"] = feature_log

    return nb_error, panel_log


def check_panel2features(
    session, db_panel2features: list, hgnc_ids: list, feature_tb, gene_tb,
    feature_type_tb
):
    """ Check links from panels to features

    Args:
        session (SQL Alchemy session): SQL Alchemy session
        db_panel2features (list): List of panel2feature rows gathered for
                                    specific panel
        hgnc_ids (list): List of hgnc ids gathered for the panel
        feature_tb: SQL Alchemy queryable table for features
        gene_tb: SQL Alchemy queryable table for genes
        feature_type_tb: SQL Alchemy queryable table for feature types

    Returns:
        list: List of errors
    """

    error_log = []

    if len(db_panel2features) != len(hgnc_ids):
        msg = (
            "Discrepancy between the number of genes in the panelapp dump "
            f"({len(hgnc_ids)}) vs in the db "
            f"({len(db_panel2features)})"
        )
        error_log.append(msg)

    # I have the comma because db_panel2features is a list of tuples with one
    # element in there --> cleaner than having to assign feature_pk to
    # feature_pk[0]
    for feature_pk, in db_panel2features:
        # check if the feature is correct by querying the gene tb using the
        # feature pk
        feature_log_msg = check_feature(
            session, feature_pk, hgnc_ids, feature_tb, gene_tb,
        )

        if feature_log_msg is not None:
            error_log.append(feature_log_msg)

        # check if the feature type is correct by querying the feature type tb
        # using the feature pk
        feature_type_log_msg = check_feature_type(
            session, "gene", feature_pk, feature_tb, feature_type_tb
        )

        if feature_type_log_msg is not None:
            error_log.append(feature_type_log_msg)

    return error_log


def check_feature(
    session, feature_pk: int, hgnc_ids: list, feature_tb, gene_tb
):
    """ Check if feature is linked to the correct hgnc_id

    Args:
        session (SQL Alchemy session): SQL Alchemy session
        feature_pk (int): Primary key of the feature
        hgnc_ids (list): List of hgnc ids gathered for the panel
        feature_tb: SQL Alchemy queryable table for features
        gene_tb: SQL Alchemy queryable table for genes

    Returns:
        str: Error msg
    """

    msg = None

    # get the hgnc id from the db using the given feature primary key
    hgnc_id = session.query(gene_tb.c.hgnc_id).outerjoin(feature_tb).filter(
        feature_tb.c.id == feature_pk
    ).one()[0]

    if hgnc_id not in hgnc_ids:
        msg = (
            f"Gene {hgnc_id} is not in the genes gathered in the "
            f"panelapp dump: {hgnc_ids}"
        )

    return msg


def check_feature_type(
    session, expected_feature_type: str, feature_pk: int, feature_tb,
    feature_type_tb
):
    """ Check if feature type is correct for given feature primary key

    Args:
        session (SQL Alchemy session): SQL Alchemy session
        expected_feature_type (str): Expected feature type
        feature_pk (int): Primary key of the feature
        feature_tb: SQL Alchemy queryable table for features
        feature_type_tb: SQL Alchemy queryable table for feature types

    Returns:
        str: Error msg
    """

    msg = None

    # get the feature type for the given feature primary key
    feature_type = session.query(
        feature_type_tb.c.type
    ).outerjoin(feature_tb).filter(
        feature_tb.c.id == feature_pk
    ).one()[0]

    if feature_type != expected_feature_type:
        msg = (
            f"The feature type {feature_type} associated with "
            f"feature {feature_pk} is not the expected feature type "
            f"'{expected_feature_type}'"
        )

    return msg