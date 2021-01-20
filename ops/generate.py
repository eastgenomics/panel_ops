from collections import defaultdict
import json
from pathlib import Path

from .logger import setup_logging, output_to_loggers
from .utils import (
    get_date, parse_gemini_dump
)


CONSOLE, GENERATION = setup_logging("generation")


def generate_panelapp_dump(all_panels: dict, type_panel: str):
    """ Generate tsv for every panelapp panel

    Args:
        all_panels (dict): Dict of all panels in panelapp
        type_panel (str): Type of panels between GMS and all panels

    Returns:
        str: Location where the panels will be written
    """

    msg = f"Creating '{type_panel}' panelapp dump"
    output_to_loggers(msg, CONSOLE, GENERATION)

    output_dump = f"{type_panel}_panelapp_dump"
    output_date = f"{get_date()}"
    output_index = 1
    output_folder = f"{output_dump}/{output_date}-{output_index}"

    while Path(output_folder).is_dir():
        output_index += 1
        output_folder = f"{output_dump}/{output_date}-{output_index}"

    Path(output_folder).mkdir(parents=True)

    for panel_id, panel in all_panels.items():
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
        else:
            panel.write(output_folder)

    msg = f"Created panelapp dump: {output_folder}"
    output_to_loggers(msg, CONSOLE, GENERATION)

    return output_folder


def generate_genepanels(session, meta):
    """ Generate gene panels file

    Args:
        session (SQLAlchemy session): Session object
        meta (SQLAlchemy MetaData): Metadata object

    Returns:
        str: Output file
    """

    msg = "Creating genepanels file"
    output_to_loggers(msg, CONSOLE, GENERATION)

    panels = {}
    gene_panels = {}
    genes = {}

    panel_tb = meta.tables["panel"]
    panel2features_tb = meta.tables["panel_features"]
    feature_tb = meta.tables["feature"]
    gene_tb = meta.tables["gene"]

    # query database to get all panels
    for panel_row in session.query(panel_tb):
        (
            panel_id, panelapp_id, name, version, panel_type, reference
        ) = panel_row

        panels[panel_id] = (name, version)

    # query database to get all genes
    for feature_id, symbol in session.query(
        feature_tb.c.id, gene_tb.c.symbol
    ).join(gene_tb):
        genes[feature_id] = symbol

    # query database to get all panel2genes links
    for feature_id, panel_id in session.query(
        panel2features_tb.c.feature_id, panel2features_tb.c.panel_id
    ):
        gene_panels.setdefault(panel_id, []).append(feature_id)

    # we want a pretty file so store the data in a nice way
    output_data = set()

    for panel_id, panel_data in panels.items():
        panel_name, version = panel_data

        for feature_id in gene_panels[panel_id]:
            output_data.add(
                (panel_name, str(version), genes[feature_id])
            )

    # sort the data using panel names and genes
    sorted_output_data = sorted(output_data, key=lambda x: (x[0], x[2]))

    output_index = 1
    output_folder = f"sql_dump/{get_date()}-{output_index}_genepanels"

    while Path(output_folder).is_dir():
        output_index += 1
        output_folder = f"sql_dump/{get_date()}-{output_index}_genepanels"

    Path(output_folder).mkdir(parents=True)

    output_file = f"{output_folder}/{get_date()}_genepanels.tsv"

    with open(output_file, "w") as f:
        for row in sorted_output_data:
            data = "\t".join(row)
            f.write(f"{data}\n")

    msg = f"Created genepanels file: {output_file}"
    output_to_loggers(msg, CONSOLE, GENERATION)

    return output_file


def generate_gms_panels(gms_panels, confidence_level: int = 3):
    """ Generate gene files for GMS panels

    Args:
        gms_panels (dict): Dict of gms panels
        confidence_level (int, optional): Confidence level of genes to get. Defaults to 3.

    Returns:
        str: Output folder path
    """

    msg = "Creating gms panels"
    output_to_loggers(msg, CONSOLE, GENERATION)

    output_index = 1
    output_folder = f"sql_dump/{get_date()}-{output_index}_gms_panels"

    while Path(output_folder).is_dir():
        output_index += 1
        output_folder = f"sql_dump/{get_date()}-{output_index}_gms_panels"

    Path(output_folder).mkdir(parents=True)

    for panel_id, panel in gms_panels.items():
        panel_file = f"{panel.get_name()}_{panel.get_version()}"

        output_file = f"{output_folder}/{panel_file}"

        with open(output_file, "w") as f:
            for gene, hgnc_id in panel.get_genes(confidence_level):
                f.write(f"{gene}\t{hgnc_id}\n")

    msg = f"Created gms panels: {output_folder}"
    output_to_loggers(msg, CONSOLE, GENERATION)

    return output_folder


def generate_django_jsons(**json_lists: list):
    """ Write the jsons for every table in the panel_database + full json for
        importing in the database

    Args:
        json_lists (list): List of dicts for every table

    Returns:
        str: Output folder
    """

    msg = "Creating json dump for django import"
    output_to_loggers(msg, CONSOLE, GENERATION)

    today = get_date()
    all_elements = []

    # have all the elements in one list for the json dump
    for table, data in json_lists.items():
        for ele in data:
            all_elements.append(ele)

    output_index = 1
    output_folder = f"django_fixtures/{get_date()}-{output_index}"

    while Path(output_folder).is_dir():
        output_index += 1
        output_folder = f"django_fixtures/{get_date()}-{output_index}"

    Path(output_folder).mkdir(parents=True)

    output = f"{output_folder}/{today}_json_dump.json"

    # write the single json containing all the elements in the database
    with open(output, "w", encoding="utf-8") as f:
        json.dump(all_elements, f, indent=4)

    # write single json for every table in the db
    # helps to debug sometimes
    for table, data in json_lists.items():
        table_output = f"{today}_{table}.json"

        with open(
            f"{output_folder}/{table_output}", "w", encoding="utf-8"
        ) as f:
            json.dump(data, f, indent=4)

    msg = f"Created json dump for django import: {output_folder}"
    output_to_loggers(msg, CONSOLE, GENERATION)

    return output_folder


def generate_manifest(session, meta, gemini_dump):
    """ Generate new bioinformatic manifest for the new database

    Args:
        session (SQLAlchemy Session): Session to make queries
        meta (SQLAlchemy metadata): Metadata to get the tables from the
                                    existing db
        gemini_dump (file_path): Gemini dump file

    Returns:
        str: File path of the output file
    """

    msg = "Creating bioinformatic manifest file"
    output_to_loggers(msg, CONSOLE, GENERATION)

    # get the content of the gemini dump
    sample2gm_panels = parse_gemini_dump(gemini_dump)

    # get the panels/genes from the db now
    ci_tb = meta.tables["clinical_indication"]
    ci2panels_tb = meta.tables["clinical_indication_panels"]
    panel_tb = meta.tables["panel"]
    panel2features_tb = meta.tables["panel_features"]
    feature_tb = meta.tables["feature"]
    gene_tb = meta.tables["gene"]

    uniq_used_panels = set([
        panel
        for ele in sample2gm_panels.values()
        for panel in ele
    ])

    # get the gemini names and associated genes and panels ids
    ci_in_manifest = session.query(
        ci_tb.c.gemini_name, ci2panels_tb.c.panel_id
    ).join(ci2panels_tb).filter(
        ci_tb.c.gemini_name.in_(uniq_used_panels)
    ).all()

    gemini2genes = defaultdict(lambda: set())

    for ci in ci_in_manifest:
        gemini_name, panel_id = ci

        gene_for_panel = session.query(
            panel_tb.c.name, panel2features_tb.c.feature_id, gene_tb.c.symbol
        ).join(panel2features_tb).join(feature_tb).join(gene_tb).filter(
            panel2features_tb.c.panel_id == panel_id
        ).all()

        genes = [data[2] for data in gene_for_panel]
        gemini2genes[gemini_name].update(genes)

    # we want a pretty file so store the data that we want to output in a nice
    # way
    output_data = set()

    for sample, panels in sample2gm_panels.items():
        for panel in panels:
            # match gemini names from the dump to the genes in the db
            if panel in gemini2genes:
                for gene in gemini2genes[panel]:
                    output_data.add((sample, panel, "NA", gene))
            else:
                if panel.startswith("_"):
                    gene = panel.strip("_")
                    output_data.add((sample, f"_{gene}", "NA", gene))

    # and sort it using sample id and the gene symbol
    sorted_output_data = sorted(output_data, key=lambda x: (x[0], x[3]))

    output_index = 1
    output_folder = f"sql_dump/{get_date()}-{output_index}_bio_manifest"

    while Path(output_folder).is_dir():
        output_index += 1
        output_folder = f"sql_dump/{get_date()}-{output_index}_bio_manifest"

    Path(output_folder).mkdir(parents=True)

    output_file = f"{output_folder}/{get_date()}_bio_manifest.tsv"

    with open(output_file, "w") as f:
        for row in sorted_output_data:
            data = "\t".join(row)
            f.write(f"{data}\n")

    msg = f"Created sample2panels file: {output_file}"
    output_to_loggers(msg, CONSOLE, GENERATION)

    return output_file
