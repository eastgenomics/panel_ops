from ops.logger import setup_logging, output_to_loggers
from ops.utils import (
    get_date, write_new_output_folder, parse_gemini_dump,
    get_clinical_indication_through_genes, get_latest_clinical_indication_data
)


CONSOLE, GENERATION = setup_logging("generation")


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
        ci_tb.c.gemini_name, ci2panels_tb.c.panel_id, ci2panels_tb.c.ci_version
    ).join(
        ci2panels_tb, ci_tb.c.id == ci2panels_tb.c.clinical_indication_id
    ).all()

    ci2panels = get_latest_clinical_indication_data(cis)

    gemini2genes = get_clinical_indication_through_genes(
        session, meta, ci2panels, hgnc_data
    )

    # we want a pretty file so store the data in a nice way
    output_data = set()

    for ci, panel_data in gemini2genes.items():
        ci_name, panelapp_id = ci.split(":")
        for panel, genes in panel_data.items():
            for gene in genes:
                output_data.add(
                    (ci_name, panel, gene, panelapp_id)
                )

    # sort the data using panel names and genes
    sorted_output_data = sorted(
        output_data, key=lambda x: (x[0], x[1], x[2], x[3])
    )

    output_folder = write_new_output_folder("sql_dump", "genepanels")
    output_file = f"{output_folder}/{get_date()}_genepanels.tsv"

    with open(output_file, "w") as f:
        for row in sorted_output_data:
            data = "\t".join(row)
            f.write(f"{data}\n")

    msg = f"Created genepanels file: {output_file}"
    output_to_loggers(msg, "info", CONSOLE, GENERATION)

    return output_file


def generate_g2t(session, meta, reference_id: int):
    """ Generate genes2transcripts file

    Args:
        session (SQLAlchemy session): Session object
        meta (SQLAlchemy MetaData): Metadata object
        reference_id (int): Reference id to use for extracting genes and
        transcripts

    Returns:
        str: Output file
    """

    msg = "Creating g2t file"
    output_to_loggers(msg, "info", CONSOLE, GENERATION)

    gene_tb = meta.tables["gene"]
    g2t_tb = meta.tables["genes2transcripts"]
    tx_tb = meta.tables["transcript"]

    # get the all the data that g2t requires:
    # hgnc id, transcript, clinical status, canonical status
    g2t_data = session.query(
        gene_tb.c.hgnc_id, tx_tb.c.refseq_base, tx_tb.c.version,
        g2t_tb.c.clinical_transcript, tx_tb.c.canonical
    ).join(
        gene_tb, gene_tb.c.id == g2t_tb.c.gene_id
    ).join(
        tx_tb, tx_tb.c.id == g2t_tb.c.transcript_id
    ).filter(
        g2t_tb.c.reference_id == reference_id
    ).all()

    data = []

    # loop through the data to format it in a way to output it easily
    for hgnc_id, refseq_base, version, clinical, canonical in g2t_data:
        transcript = ".".join([refseq_base, str(version)])

        if clinical == 0:
            clinical_status = "not_clinical_transcript"
        else:
            clinical_status = "clinical_transcript"

        if canonical == 0:
            canonical_status = "not_canonical"
        else:
            canonical_status = "canonical"

        data.append([hgnc_id, transcript, clinical_status, canonical_status])

    # sort the data according to hgnc id and transcript
    sorted_data = sorted(data, key=lambda x: (x[0], x[1]))

    output_folder = write_new_output_folder("sql_dump", "g2t")
    output_file = f"{output_folder}/{get_date()}_g2t.tsv"

    with open(output_file, "w") as f:
        for row in sorted_data:
            data = "\t".join(row)
            f.write(f"{data}\n")

    msg = f"Created g2t file: {output_file}"
    output_to_loggers(msg, "info", CONSOLE, GENERATION)

    return output_file


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
        ci_tb.c.gemini_name, ci2panels_tb.c.panel_id, ci2panels_tb.c.ci_version
    ).join(
        ci2panels_tb, ci_tb.c.id == ci2panels_tb.c.clinical_indication_id
    ).filter(
        ci_tb.c.gemini_name.in_(uniq_used_panels)
    ).all()

    ci2panels = get_latest_clinical_indication_data(ci_in_manifest)

    gemini2genes = get_clinical_indication_through_genes(
        session, meta, ci2panels, hgnc_data
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
