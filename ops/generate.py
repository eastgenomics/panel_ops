import json

from .logger import setup_logging, output_to_loggers
from .utils import (
    get_date, write_new_output_folder, parse_gemini_dump, filter_out_gene,
    create_panelapp_dict, gather_ref_django_json,
    gather_panel_types_django_json, gather_feature_types_django_json,
    gather_panel_data_django_json, gather_superpanel_data_django_json,
    gather_transcripts, gather_clinical_indication_data_django_json,
    get_clinical_indication_through_genes
)


CONSOLE, GENERATION = setup_logging("generation")


def generate_panelapp_tsvs(all_panels: dict, type_panel: str):
    """ Generate tsv for every panelapp panel

    Args:
        all_panels (dict): Dict of all panels in panelapp
        type_panel (str): Type of panels between GMS and all panels

    Returns:
        str: Location where the panels were written
    """

    msg = f"Creating '{type_panel}' panelapp dump"
    output_to_loggers(msg, "info", CONSOLE, GENERATION)

    # name of the main folder
    output_dump = f"{type_panel}_panelapp_dump"
    output_folder = write_new_output_folder(output_dump)

    # loop through the panels
    for panel_id, panel in all_panels.items():
        # if the panel is superpanel we want to get the genes from the subpanels
        if panel.is_superpanel():
            subpanels = panel.get_subpanels()
            superpanel_output = (
                f"{panel.get_name()}_{panel.get_version()}_superpanel.tsv"
            )

            with open(f"{output_folder}/{superpanel_output}", "w") as f:
                for subpanel_id, subpanel, version in subpanels:
                    f.write((
                        f"{panel.get_id()}\t{panel.get_name()}\t"
                        f"{panel.get_version()}\t{panel.is_signedoff()}\t"
                        f"{subpanel_id}\t{subpanel}\t{version}\n"
                    ))
        # else just write the panel using the existing method
        else:
            panel.write(output_folder)

    msg = f"Created panelapp dump: {output_folder}"
    output_to_loggers(msg, "info", CONSOLE, GENERATION)

    return output_folder


def generate_genepanels(session, meta, hgnc_data: dict):
    """ Generate gene panels file

    Args:
        session (SQLAlchemy session): Session object
        meta (SQLAlchemy MetaData): Metadata object

    Returns:
        str: Output file
    """

    msg = "Creating genepanels file"
    output_to_loggers(msg, "info", CONSOLE, GENERATION)

    genes = {}

    ci_tb = meta.tables["clinical_indication"]
    ci2panels_tb = meta.tables["clinical_indication_panels"]

    # get the gemini names and associated panels ids
    cis = session.query(
        ci_tb.c.gemini_name, ci2panels_tb.c.panel_id
    ).join(
        ci2panels_tb, ci_tb.c.id == ci2panels_tb.c.clinical_indication_id
    ).all()

    gemini2genes = get_clinical_indication_through_genes(
        session, meta, cis, hgnc_data
    )

    # we want a pretty file so store the data in a nice way
    output_data = set()

    for ci, panel_data in gemini2genes.items():
        for panel, genes in panel_data.items():
            for gene in genes:
                output_data.add(
                    (ci, panel, gene)
                )

    # sort the data using panel names and genes
    sorted_output_data = sorted(output_data, key=lambda x: (x[0], x[1], x[2]))

    output_folder = write_new_output_folder("sql_dump", "genepanels")
    output_file = f"{output_folder}/{get_date()}_genepanels.tsv"

    with open(output_file, "w") as f:
        for row in sorted_output_data:
            data = "\t".join(row)
            f.write(f"{data}\n")

    msg = f"Created genepanels file: {output_file}"
    output_to_loggers(msg, "info", CONSOLE, GENERATION)

    return output_file


def generate_django_jsons(
    panel_dumps: list, clean_clinind_data: dict, g2t_data: str,
    single_genes: list, references: list, feature_types: list,
    panel_types: list, pk_dict: dict
):
    """ Write the jsons for every table in the panel_database + full json for
        importing in the database

    Args:
        panel_dumps (list): List of files to create panel dumps for
        clean_clinind_data (dict): Data gathered from the national test directory
        g2t_data (dict): Data gathered from g2t dump
        single_genes (list): List of single genes associated with clinical indications
        references (list): Hardcoded list of references to consider, defined in config_panel_db.py
        feature_types (list): Hardcoded list of feature types to consider, defined in config_panel_db.py
        panel_types (list): Hardcoded list of panel types to consider, defined in config_panel_db.py
        pk_dict (dict): Dict of primary keys

    Returns:
        str: Output file path
    """

    msg = "Gathering data from panel dumps"
    output_to_loggers(msg, "info", CONSOLE, GENERATION)

    (
        panelapp_dict, superpanel_dict, gene_dict
    ) = create_panelapp_dict(panel_dumps, panel_types, single_genes)

    # Create the list of reference table
    reference_json = gather_ref_django_json(
        references, pk_dict["reference"]
    )
    # Create the list of panel_type table
    paneltype_json = gather_panel_types_django_json(
        panel_types, pk_dict["panel_type"]
    )
    # Create the list of feature_type table
    featuretype_json = gather_feature_types_django_json(
        feature_types, pk_dict["feature_type"]
    )

    # Create the list for data associated with panels
    (
        panel_json, feature_json, panelfeature_json, gene_json, pk_dict
    ) = gather_panel_data_django_json(
        panelapp_dict, gene_dict, featuretype_json, paneltype_json, pk_dict
    )

    # Add the superpanels to the list of panel objects
    (
        panel_json, panelfeature_json
    ) = gather_superpanel_data_django_json(
        superpanel_dict, panel_json, paneltype_json, panelfeature_json, pk_dict
    )

    # Create the list for data associated with transcripts
    transcript_json, g2t_json = gather_transcripts(
        gene_json, reference_json, g2t_data, pk_dict
    )

    # Create the list of clinical indication
    (
        clinical_indication_json, clinical_indication2panels_json
    ) = gather_clinical_indication_data_django_json(
        clean_clinind_data, panel_json, pk_dict
    )

    json_lists = {
        "references": reference_json, "panel_types": paneltype_json,
        "feature_types": featuretype_json, "panels": panel_json,
        "feature": feature_json, "panel_features": panelfeature_json,
        "genes": gene_json, "transcripts": transcript_json,
        "genes2transcripts": g2t_json,
        "clinical_indications": clinical_indication_json,
        "clinical_indications2panels": clinical_indication2panels_json
    }

    msg = "Writing data in json files for django import"
    output_to_loggers(msg, "info", CONSOLE, GENERATION)

    today = get_date()
    all_elements = []

    # have all the elements in one list for the json dump
    for table, data in json_lists.items():
        for ele in data:
            all_elements.append(ele)

    output_folder = write_new_output_folder("django_fixtures")
    output_file = f"{output_folder}/{today}_json_dump.json"

    # write the single json containing all the elements in the database
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_elements, f, indent=4)

    # write single json for every table in the db
    # helps to debug sometimes
    for table, data in json_lists.items():
        table_output = f"{today}_{table}.json"
        output_file = f"{output_folder}/{table_output}"

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

    msg = f"Created json dump for django import: {output_folder}"
    output_to_loggers(msg, "info", CONSOLE, GENERATION)

    return output_folder


def generate_manifest(session, meta, gemini_dump: str, hgnc_data: dict):
    """ Generate new bioinformatic manifest for the new database

    Args:
        session (SQLAlchemy Session): Session to make queries
        meta (SQLAlchemy metadata): Metadata to get the tables from the
                                    existing db
        gemini_dump (str): Gemini dump file

    Returns:
        str: File path of the output file
    """

    msg = "Creating bioinformatic manifest file"
    output_to_loggers(msg, "info", CONSOLE, GENERATION)

    # get the content of the gemini dump
    sample2gemini_name = parse_gemini_dump(gemini_dump)

    # get the panels/genes from the db now
    ci_tb = meta.tables["clinical_indication"]
    ci2panels_tb = meta.tables["clinical_indication_panels"]

    uniq_used_panels = set([
        panel
        for ele in sample2gemini_name.values()
        for panel in ele
    ])

    # get the gemini names and associated genes and panels ids
    ci_in_manifest = session.query(
        ci_tb.c.gemini_name, ci2panels_tb.c.panel_id
    ).join(
        ci2panels_tb, ci_tb.c.id == ci2panels_tb.c.clinical_indication_id
    ).filter(
        ci_tb.c.gemini_name.in_(uniq_used_panels)
    ).all()

    gemini2genes = get_clinical_indication_through_genes(
        session, meta, ci_in_manifest, hgnc_data
    )

    # we want a pretty file so store the data that we want to output in a nice
    # way
    output_data = set()

    for sample, clinical_indications in sample2gemini_name.items():
        for clinical_indication in clinical_indications:
            # match gemini names from the dump to the genes in the db
            if clinical_indication in gemini2genes:
                for panel in gemini2genes[clinical_indication]:
                    genes = gemini2genes[clinical_indication][panel]

                    for gene in genes:
                        # match format of the bioinformatic manifest
                        output_data.add(
                            (sample, clinical_indication, panel, gene)
                        )
            else:
                # check if it is a single gene panel
                if clinical_indication.startswith("_"):
                    gene = clinical_indication.strip("_")
                    # match format of the bioinformatic manifest
                    output_data.add((sample, f"_{gene}", f"_{gene}", gene))

    # and sort it using sample id and the gene symbol
    sorted_output_data = sorted(output_data, key=lambda x: (x[0], x[3]))

    output_folder = write_new_output_folder("sql_dump", "bio_manifest")
    output_file = f"{output_folder}/{get_date()}_bio_manifest.tsv"

    with open(output_file, "w") as f:
        for row in sorted_output_data:
            data = "\t".join(row)
            f.write(f"{data}\n")

    msg = f"Created sample2panels file: {output_file}"
    output_to_loggers(msg, "info", CONSOLE, GENERATION)

    return output_file
