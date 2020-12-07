from .logger import setup_logging
from .utils import assign_transcript, get_date


LOGGER = setup_logging("check")


def check_gene(gene: str, hgmd_dict: dict, nirvana_dict: dict):
    """Returns the transcripts and exons for given gene according to HGMD and
    nirvana

    Args:
        gene (str): Gene symbol
        hgmd_dict (dict): Dict of HGMD data
        nirvana_dict (dict): Dict of Nirvana data
    """

    transcript_dict, clinical_transcript = assign_transcript(
        gene, hgmd_dict, nirvana_dict
    )

    if transcript_dict:
        print(
            f"Clinical transcript for {gene} is: {clinical_transcript}"
        )
        for tx in transcript_dict:
            print(tx)

            for exon_nb in transcript_dict[tx]["exons"]:
                chrom = transcript_dict[tx]["exons"][exon_nb]["chrom"]
                start = transcript_dict[tx]["exons"][exon_nb]["start"]
                end = transcript_dict[tx]["exons"][exon_nb]["end"]
                print(f"Exon_nb: {exon_nb}\t{chrom}\t{start}-{end}")
    else:
        print(f"No transcript for {gene}")


def check_panelapp_dump_against_db(folder, session, meta, data_dicts: tuple):
    """ Check that the data in the panelapp dump is the same as what's in the
    db

    Args:
        folder (str): Folder in which panels are stored
        session (SQLAlchemy session): Session object
        meta (SQLAlchemy MetaData): Metadata object
        data_dicts (tuple): Tuple of dicts
    """

    LOGGER.info(f"Checking database against {folder}")

    logging_dict = {}

    (
        panelapp_dict, superpanel_dict, gene_dict, str_dict,
        cnv_dict, region_dict
    ) = data_dicts

    ref_tb = meta.tables["reference"]
    panel_tb = meta.tables["panel"]
    superpanel_tb = meta.tables["superpanel"]
    gene_tb = meta.tables["gene"]
    panel_gene_tb = meta.tables["panel_gene"]
    transcript_tb = meta.tables["transcript"]
    exon_tb = meta.tables["exon"]
    region_tb = meta.tables["region"]
    str_tb = meta.tables["str"]
    region_str_tb = meta.tables["region_str"]
    cnv_tb = meta.tables["cnv"]
    region_cnv_tb = meta.tables["region_cnv"]

    # Get the ids for the references for use with strs and cnvs
    grch37_id = session.query(ref_tb.c.id).filter(
        ref_tb.c.name == "GRCh37").one()[0]

    grch38_id = session.query(ref_tb.c.id).filter(
        ref_tb.c.name == "GRCh38").one()[0]

    # Check superpanels
    # Query db for superpanels
    superpanel_values = [int(id_) for id_ in superpanel_dict.keys()]
    db_superpanels = session.query(panel_tb).filter(
        panel_tb.c.panelapp_id.in_(superpanel_values)
    ).all()

    db_superpanels_id = set([row[1] for row in db_superpanels])
    db_superpanel_diff = db_superpanels_id - set(superpanel_values)
    panelapp_superpanel_diff = set(superpanel_values) - db_superpanels_id

    if len(db_superpanels) == len(superpanel_dict):
        if db_superpanel_diff or panelapp_superpanel_diff:
            logging_dict.setdefault("superpanel", []).append(
                f"{db_superpanel_diff} is missing from the panelapp dump"
            )
            logging_dict["superpanel"].append(
                f"{panelapp_superpanel_diff} is missing from the database"
            )

    else:
        logging_dict.setdefault("superpanel", []).append(
            f"{db_superpanel_diff} is missing from the panelapp dump"
        )
        logging_dict["superpanel"].append(
            f"{panelapp_superpanel_diff} is missing from the database"
        )

    # Check subpanels associated to superpanels
    for superpanel_row in db_superpanels:
        (
            superpanel_id, panelapp_id, panel_name,
            version, signedoff, superpanel
        ) = superpanel_row

        superpanel_query = session.query(superpanel_tb.c.panel_id).filter(
            superpanel_tb.c.superpanel_id == superpanel_id
        ).all()

        subpanels = [ele[0] for ele in superpanel_query]

        subpanel_query = session.query(panel_tb).filter(
            panel_tb.c.id.in_(subpanels)
        ).all()

        subpanel_dict = superpanel_dict[str(panelapp_id)]["subpanels"]

        for subpanel_row in subpanel_query:
            (
                subpanel_id, sub_panelapp_id, sub_name,
                version, signedoff, superpanel
            ) = subpanel_row

            if sub_name != subpanel_dict[str(sub_panelapp_id)]["name"]:
                logging_dict.setdefault("subpanel", []).append(
                    f"{panel_name}: Subpanel name do not match: db "
                    f"'{sub_name}' vs panelapp '{sub_panelapp_id}'"
                )

    # Get all panels using panelapp ids in panelapp dict
    panel_values = [int(id_) for id_ in panelapp_dict.keys()]
    db_panels = session.query(panel_tb).filter(
        panel_tb.c.panelapp_id.in_(panel_values)
    ).all()

    db_panels_id = set([row[1] for row in db_panels])
    db_panel_diff = db_panels_id - set(panel_values)
    panelapp_panel_diff = set(panel_values) - db_panels_id

    if len(db_panels) == len(panelapp_dict):
        if db_panel_diff or panelapp_panel_diff:
            logging_dict.setdefault("panel", []).append(
                f"{db_panel_diff} is missing from the panelapp dump"
            )
            logging_dict["panel"].append(
                f"{panelapp_panel_diff} is missing from the panelapp dump"
            )
    else:
        logging_dict.setdefault("panel", []).append(
            f"{db_panel_diff} is missing from the panelapp dump"
        )
        logging_dict["panel"].append(
            f"{panelapp_panel_diff} is missing from the panelapp dump"
        )

    for panel_row in db_panels:
        (
            panel_id, panelapp_id, panel_name, version, signedoff, superpanel
        ) = panel_row

        # Get all genes for that panel
        gene_values = list(panelapp_dict[str(panelapp_id)]["genes"])
        db_genes = session.query(gene_tb).filter(
            gene_tb.c.symbol.in_(gene_values)
        ).all()

        # Get all panel2gene links for that panel
        db_panel_genes = session.query(panel_gene_tb).filter(
            panel_gene_tb.c.panel_id == panel_id
        ).all()

        db_genes_symbols = set([row[1] for row in db_genes])
        db_gene_diff = db_genes_symbols - set(gene_values)
        panelapp_gene_diff = set(gene_values) - db_genes_symbols

        if len(gene_values) == len(db_panel_genes):
            if db_gene_diff or panelapp_gene_diff:
                logging_dict.setdefault("gene", []).append(
                    f"{panel_name}: {db_gene_diff} is missing from the "
                    "panelapp dump"
                )
                logging_dict.setdefault("gene", []).append(
                    f"{panel_name}: {panelapp_gene_diff} is missing from the "
                    "db"
                )
        else:
            logging_dict.setdefault("gene", []).append(
                f"{panel_name}: {db_gene_diff} is missing from the "
                "panelapp dump"
            )
            logging_dict.setdefault("gene", []).append(
                f"{panel_name}: {panelapp_gene_diff} is missing from the "
                "db"
            )

        for gene_row in db_genes:
            gene_id, symbol, clin_tx_id = gene_row
            transcript_data = gene_dict[symbol]["transcripts"]

            if transcript_data is not None:
                # Get all transcripts for gene
                refseq_values = [
                    tx.split(".")[0]
                    for tx in transcript_data.keys()
                ]
                db_transcripts = session.query(transcript_tb).filter(
                    transcript_tb.c.refseq.in_(refseq_values)
                ).all()

                db_transcripts_refseq = set(
                    [row[1] for row in db_transcripts]
                )
                db_tx_diff = db_transcripts_refseq - set(refseq_values)
                panelapp_tx_diff = set(refseq_values) - db_transcripts_refseq

                if len(transcript_data) == len(db_transcripts):
                    if db_tx_diff or panelapp_tx_diff:
                        logging_dict.setdefault("transcript", []).append((
                            f"{panel_name} - {symbol}: {db_tx_diff} is "
                            "missing from the panelapp dump"
                        ))
                        logging_dict.setdefault("transcript", []).append((
                            f"{panel_name} - {symbol}: {panelapp_tx_diff} is "
                            "missing from the panelapp dump"
                        ))
                else:
                    logging_dict.setdefault("transcript", []).append((
                        f"{panel_name} - {symbol}: {db_tx_diff} is "
                        "missing from the panelapp dump"
                    ))
                    logging_dict.setdefault("transcript", []).append((
                        f"{panel_name} - {symbol}: {panelapp_tx_diff} is "
                        "missing from the panelapp dump"
                    ))

                for tx in db_transcripts:
                    tx_id, refseq, tx_version, tx_gene_id = tx
                    tx = f"{refseq}.{tx_version}"
                    exon_data = gene_dict[symbol]["transcripts"][tx]["exons"]

                    # Get exons for the transcripts
                    exon_values = [int(nb) for nb in exon_data.keys()]
                    db_exons = session.query(exon_tb).filter(
                        exon_tb.c.number.in_(exon_values),
                        exon_tb.c.transcript_id == tx_id
                    ).all()

                    if len(db_exons) != len(exon_data):
                        logging_dict.setdefault("exon", []).append((
                            f"{panel_name} - {symbol} - Nb of links from "
                            f"'{refseq}' to the exons is not what is expected"
                        ))
                        logging_dict.setdefault("exon", []).append((
                            f"{panel_name} - {symbol} - {refseq} - "
                            f"{len(db_exons)} in the db against "
                            f"{len(exon_data)} in the panelapp dump"
                        ))

                    # Get all regions for the transcripts
                    exon_query = session.query(exon_tb.c.region_id).filter(
                        exon_tb.c.transcript_id == tx_id
                    )
                    db_regions = session.query(region_tb).filter(
                        region_tb.c.id.in_(exon_query)
                    ).all()

                    if len(db_regions) != len(exon_data):
                        logging_dict.setdefault("region", []).append((
                            f"{panel_name} - {symbol} - {tx}: Nb of regions "
                            f"for {refseq} is not what is expected"
                        ))

                    exons = set()

                    for exon in db_exons:
                        exon_id, number, region_id, transcript_id = exon
                        chrom = exon_data[str(number)]["chrom"]
                        start = exon_data[str(number)]["start"]
                        end = exon_data[str(number)]["end"]
                        exons.add((chrom, int(start), int(end)))

                    for region in db_regions:
                        region_id, db_chrom, db_start, db_end, ref = region

                        if (db_chrom, db_start, db_end) not in exons:
                            logging_dict.setdefault("region", []).append((
                                f"{panel_name} - {symbol} - {tx}: "
                                f"Region {db_chrom}:{db_start}-{db_end} (db) "
                                f"is not present in the exons of {refseq} "
                                "panelapp dump"
                            ))

        str_values = list(panelapp_dict[str(panelapp_id)]["strs"])

        # If the panel has strs
        if str_values:
            db_strs = session.query(str_tb).filter(
                str_tb.c.id.in_(str_values)
            ).all()

            for str_row in db_strs:
                (
                    str_id, name, repeated_seq, nb_repeats,
                    nb_patho_repeats, gene_id
                ) = str_row

                symbol = session.query(gene_tb.c.symbol).filter(
                    gene_tb.c.id == gene_id).one()[0]

                # Gene associated with the str is not correct
                if symbol != str_dict[name]["gene"]:
                    logging_dict.setdefault("str", []).append(
                        f"{panel_name} - {name}: '{symbol}' (db) is not equal "
                        f"to {str_dict[name]['gene']} (panelapp)"
                    )

                # data stored for the str is not correct
                if (
                    repeated_seq != str_dict[name]["seq"] or
                    str(nb_repeats) != str_dict[name]["nb_normal_repeats"] or
                    str(nb_patho_repeats) != str_dict[name]["nb_pathogenic_repeats"]
                ):
                    logging_dict.setdefault("str", []).append(
                        f"{panel_name} - {name}: Data for the str is not "
                        "correct"
                    )
                    logging_dict.setdefault("str", []).append(
                        f"{repeated_seq} != {str_dict[name]['seq']}"
                    )
                    logging_dict.setdefault("str", []).append(
                        f"{nb_repeats} != "
                        f"{str_dict[name]['nb_normal_repeats']}"
                    )
                    logging_dict.setdefault("str", []).append(
                        f"{nb_patho_repeats} != "
                        f"{str_dict[name]['nb_pathogenic_repeats']}"
                    )

                reg_str_query = session.query(
                    region_str_tb.c.region_id
                ).filter(
                    region_str_tb.c.str_id == str_id
                )
                associated_regions = session.query(region_tb).filter(
                    region_tb.c.id.in_(reg_str_query)
                ).all()

                for region in associated_regions:
                    region_id, db_chrom, db_start, db_end, ref = region

                    if ref == grch37_id:
                        if (str(db_start), str(db_end)) not in region_dict[db_chrom]["GRCh37"]:
                            logging_dict.setdefault("str", []).append((
                                f"{panel_name} - {name}: "
                                f"{db_chrom}:{db_start}-{db_end} not in GRCh37"
                            ))

                    elif ref == grch38_id:
                        if (str(db_start), str(db_end)) not in region_dict[db_chrom]["GRCh38"]:
                            logging_dict.setdefault("str", []).append((
                                f"{panel_name} - {name}: "
                                f"{db_chrom}:{db_start}-{db_end} not in GRCh38"
                            ))

        cnv_values = list(panelapp_dict[str(panelapp_id)]["cnvs"])

        # panel has cnvs
        if cnv_values:
            db_cnvs = session.query(cnv_tb).filter(
                cnv_tb.c.name.in_(cnv_values)
            ).all()

            for cnv_row in db_cnvs:
                (cnv_id, name, variant_type) = cnv_row

                # check the type of the cnv
                if (variant_type != cnv_dict[name]["type"]):
                    logging_dict.setdefault("cnv", []).append(
                        f"{panel_name} - {name}: Type of cnv is incorrect "
                        f"{variant_type} (db) vs {cnv_dict[name]['type']} "
                        "(panelapp)"
                    )

                reg_cnv_query = session.query(
                    region_cnv_tb.c.region_id
                ).filter(
                    region_cnv_tb.c.cnv_id == cnv_id
                )
                associated_regions = session.query(region_tb).filter(
                    region_tb.c.id.in_(reg_cnv_query)
                ).all()

                for region in associated_regions:
                    region_id, db_chrom, db_start, db_end, ref = region

                    if ref == grch37_id:
                        if (str(db_start), str(db_end)) not in region_dict[db_chrom]["GRCh37"]:
                            logging_dict.setdefault("cnv", []).append((
                                f"{panel_name} - {name}: "
                                f"{db_chrom}:{db_start}-{db_end} not in GRCh37"
                            ))

                    elif ref == grch38_id:
                        if (str(db_start), str(db_end)) not in region_dict[db_chrom]["GRCh38"]:
                            logging_dict.setdefault("cnv", []).append((
                                f"{panel_name} - {name}: "
                                f"{db_chrom}:{db_start}-{db_end} not in GRCh38"
                            ))

    if logging_dict.values():
        for log_type, msgs in logging_dict.items():
            for error_msg in msgs:
                LOGGER.error(error_msg)
    else:
        msg = (
            f"Checking against panelapp dump from {folder} against database "
            f"on {get_date()}: correct"
        )
        LOGGER.info(msg)

    return True


def check_test_against_db(session, meta, test2target: dict):
    """Check if the tests have been imported correctly

    Args:
        session (SQLAlchemy session): Session object
        meta (SQLAlchemy MetaData): Metadata object
        test2target (dict): Dict of test2target
    """

    LOGGER.info("Checking tests")

    test_tb = meta.tables["test"]
    test_panel_tb = meta.tables["test_panel"]
    panel_tb = meta.tables["panel"]
    test_gene_tb = meta.tables["test_gene"]
    gene_tb = meta.tables["gene"]

    test_ids = [test for test in test2target]
    db_tests = session.query(test_tb).filter(
        test_tb.c.test_id.in_(test_ids)
    ).all()

    # Check if the number of imported tests is the same as the number of tests
    # in the xls
    if len(test2target) != len(db_tests):
        msg = (
            "Nb of tests in the xls is not the same has in the database: "
            f"xls {len(test2target)} vs db {len(db_tests)}"
        )
        LOGGER.error(msg)
        return

    for test in db_tests:
        db_test_id, test_id, clin_ind, method, date, gem_name = test

        panel_targets = test2target[test_id]["panels"]

        # If the test has panels associated to it
        if panel_targets != []:
            # Get the panels associated to the test in the db
            db_panel_targets = session.query(test_panel_tb).filter(
                test_panel_tb.c.test_id == db_test_id
            ).all()

            # From the associated panels, check if the panelapp id imported is
            # the same as the one in the xls
            for panel_target in db_panel_targets:
                test_panel_id, panel_id, db_test_id = panel_target
                panelapp_id = session.query(panel_tb.c.panelapp_id).filter(
                    panel_tb.c.id == panel_id
                ).one()[0]

                if str(panelapp_id) not in test2target[test_id]["panels"]:
                    LOGGER.info(
                        f"Panel {panelapp_id} is missing from {test_id}"
                    )
                    LOGGER.debug(test2target[test_id]["panels"])
                    return

        gene_targets = test2target[test_id]["genes"]

        # If the test has genes associated to it
        if gene_targets != []:
            # Get the genes associated to the test in the db
            db_gene_targets = session.query(test_gene_tb).filter(
                test_gene_tb.c.test_id == db_test_id
            ).all()

            # From the associated genes, check if the gene imported is the same
            # as the one(s) in the xls
            for gene_target in db_gene_targets:
                test_panel_id, gene_id, db_test_id = gene_target
                symbol = session.query(gene_tb.c.symbol).filter(
                    gene_tb.c.id == gene_id
                ).one()[0]

                if symbol not in test2target[test_id]["genes"]:
                    LOGGER.info(
                        f"Gene {symbol} is missing from {test_id}"
                    )
                    LOGGER.debug(test2target[test_id]["panels"])
                    return

    return True
