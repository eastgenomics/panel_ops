from collections import defaultdict
import json
from pathlib import Path

from .logger import setup_logging, output_to_loggers
from .utils import (
    get_date, parse_gemini_dump, create_panelapp_dict, gather_ref_django_json,
    gather_panel_types_django_json, gather_feature_types_django_json,
    gather_panel_data_django_json, gather_superpanel_data_django_json,
    gather_transcripts, gather_clinical_indication_data_django_json
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

    # name of the main folder
    output_dump = f"{type_panel}_panelapp_dump"
    output_date = f"{get_date()}"
    output_index = 1
    # full path to the folders to be created
    output_folder = f"{output_dump}/{output_date}-{output_index}"

    # don't want to overwrite files so create folders using the output index
    while Path(output_folder).is_dir():
        output_index += 1
        output_folder = f"{output_dump}/{output_date}-{output_index}"

    # create the folders
    Path(output_folder).mkdir(parents=True)

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
    gene_panels = defaultdict(lambda: defaultdict(list))
    genes = {}

    panel_tb = meta.tables["panel"]
    panel2features_tb = meta.tables["panel_features"]
    feature_tb = meta.tables["feature"]
    gene_tb = meta.tables["gene"]

    # query database to get all panels
    for panel_row in session.query(panel_tb):
        panel_id, panelapp_id, name, panel_type = panel_row

        panels[panel_id] = name

    # query database to get all genes
    for feature_id, hgnc_id in session.query(
        feature_tb.c.id, gene_tb.c.hgnc_id
    ).join(gene_tb):
        genes[feature_id] = hgnc_id

    # query database to get all panel2genes links
    for pk, panel_version, feature_id, panel_id in session.query(
        panel2features_tb
    ):
        gene_panels[panel_id][panel_version].append(feature_id)

    # we want a pretty file so store the data in a nice way
    output_data = set()

    for panel_id, panel_data in panels.items():
        panel_name = panel_data

        # get the latest version of a panel. i don't really have a clean way
        # to specify specific panel versions to get
        panel_versions = [float(version) for version in gene_panels[panel_id]]
        latest_version = str(max(panel_versions))

        for feature_id in gene_panels[panel_id][latest_version]:
            output_data.add(
                (panel_name, str(latest_version), genes[feature_id])
            )

    # sort the data using panel names and genes
    sorted_output_data = sorted(output_data, key=lambda x: (x[0], x[2]))

    output_index = 1
    output_folder = f"sql_dump/{get_date()}-{output_index}_genepanels"

    # don't want to overwrite files so create folders using the output index
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


def generate_gms_panels(gms_panels: dict, confidence_level: int = 3):
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

    # don't want to overwrite files so create folders using the output index
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


def generate_django_jsons(
    panel_dumps: list, clean_clinind_data: dict, hgnc_data: dict,
    nirvana_data: dict, single_genes: list, references: list,
    feature_types: list, panel_types: list, pk_dict: dict
):
    """ Write the jsons for every table in the panel_database + full json for
        importing in the database

    Args:
        panel_dumps (list): List of files to create panel dumps for
        clean_clinind_data (dict): Data gathered from the national test directory
        hgnc_data (dict): Data gathered from hgnc dump
        nirvana_data (dict): Data gathered from nirvana gff
        single_genes (list): List of single genes associated with clinical indications
        references (list): Hardcoded list of references to consider, defined in config_panel_db.py
        feature_types (list): Hardcoded list of feature types to consider, defined in config_panel_db.py
        panel_types (list): Hardcoded list of panel types to consider, defined in config_panel_db.py
        pk_dict (dict): Dict of primary keys

    Returns:
        str: Output file path
    """

    msg = "Gathering data from panelapp dump and co"
    output_to_loggers(msg, CONSOLE, GENERATION)

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

    transcript_json, g2t_json = gather_transcripts(
        gene_json, reference_json, hgnc_data, nirvana_data, pk_dict
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
    output_to_loggers(msg, CONSOLE, GENERATION)

    today = get_date()
    all_elements = []

    # have all the elements in one list for the json dump
    for table, data in json_lists.items():
        for ele in data:
            all_elements.append(ele)

    output_index = 1
    output_folder = f"django_fixtures/{get_date()}-{output_index}"

    # don't want to overwrite files so create folders using the output index
    while Path(output_folder).is_dir():
        output_index += 1
        output_folder = f"django_fixtures/{get_date()}-{output_index}"

    Path(output_folder).mkdir(parents=True)

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
    output_to_loggers(msg, CONSOLE, GENERATION)

    return output_folder


def generate_manifest(session, meta, gemini_dump: str):
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

        # query to get all genes from a panel id
        gene_for_panel = session.query(
            panel_tb.c.name, panel2features_tb.c.feature_id, gene_tb.c.hgnc_id
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
                    # match format of the bioinformatic manifest
                    output_data.add((sample, panel, "NA", gene))
            else:
                if panel.startswith("_"):
                    gene = panel.strip("_")
                    # match format of the bioinformatic manifest
                    output_data.add((sample, f"_{gene}", "NA", gene))

    # and sort it using sample id and the gene symbol
    sorted_output_data = sorted(output_data, key=lambda x: (x[0], x[3]))

    output_index = 1
    output_folder = f"sql_dump/{get_date()}-{output_index}_bio_manifest"

    # don't want to overwrite files so create folders using the output index
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


def generate_g2t(session, meta):
    """ Generate genes2transcripts

    Args:
        session (SQLAlchemy session): SQLAlchemy session obj
        meta (SQLAlchemy meta): SQLAlchemy meta obj
    """

    msg = f"Creating genes2transcripts"
    output_to_loggers(msg, CONSOLE, GENERATION)

    gene_tb = meta.tables["gene"]
    g2t_tb = meta.tables["genes2transcripts"]
    transcript_tb = meta.tables["transcript"]

    data = []

    # loop through all genes in database
    for gene_pk, hgnc_id in session.query(gene_tb):
        # get the clinical transcript of the current gene
        transcript_data = session.query(
            transcript_tb.c.refseq_base, transcript_tb.c.version
        ).outerjoin(g2t_tb).filter(
            g2t_tb.c.clinical_transcript == 1
        ).filter(g2t_tb.c.gene_id == gene_pk).one_or_none()

        if transcript_data is not None:
            refseq_base, version = transcript_data
            data.append((hgnc_id, f"{refseq_base}.{version}"))

    sorted_data = sorted(data, key=lambda x: (x[0]))

    output_dump = "sql_dump"
    output_date = get_date()
    output_index = 1
    output_folder = (
        f"{output_dump}/{output_date}-{output_index}_genes2transcripts"
    )

    # don't want to overwrite files so create folders using the output index
    while Path(output_folder).is_dir():
        output_index += 1
        output_folder = f"{output_dump}/{output_date}-{output_index}"

    Path(output_folder).mkdir(parents=True)

    output_file = f"{output_folder}/{output_date}_genes2transcripts.tsv"

    with open(output_file, "w") as f:
        for hgnc_id, clinical_transcript in sorted_data:
            f.write(f"{hgnc_id}\t{clinical_transcript}\n")

    msg = f"Created sample2panels file: {output_file}"
    output_to_loggers(msg, CONSOLE, GENERATION)
