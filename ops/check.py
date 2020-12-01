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
        session (SQLAlchemysession): Session object 
        meta (SQLAlchemy MetaData): Metadata object
        data_dicts (tuple): Tuple of dicts
    """

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

    if len(db_superpanels) == len(superpanel_dict):
        db_superpanels_id = set([row[1] for row in db_superpanels])
        superpanel_diff = db_superpanels_id.symmetric_difference(
            set(superpanel_values)
        )

        if superpanel_diff:
            msg = f"{superpanel_diff} is not present in both sets"
            LOGGER.error(msg)
            return

    else:
        LOGGER.error("Expected nb of superpanels is incorrect")

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
                msg = (
                    f"{panel_name}: Subpanel '{sub_name}' is not equal to "
                    f"what is expected for '{sub_panelapp_id}'"
                )
                LOGGER.error(msg)
                return

    # Get all panels using panelapp ids in panelapp dict
    panel_values = [int(id_) for id_ in panelapp_dict.keys()]
    db_panels = session.query(panel_tb).filter(
        panel_tb.c.panelapp_id.in_(panel_values)
    ).all()

    if len(db_panels) == len(panelapp_dict):
        db_panels_id = set([row[1] for row in db_panels])
        panel_diff = db_panels_id.symmetric_difference(set(panel_values))

        if panel_diff:
            msg = f"{panel_diff} is not present in both sets"
            LOGGER.error(msg)
            return
    else:
        LOGGER.error("Expected nb of panels is incorrect")
        return

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

        if len(gene_values) != len(db_genes):
            msg = (
                f"{panel_name}: Number of genes is incorrect"
            )
            LOGGER.error(msg)
            return
        else:
            db_genes_symbols = set([row[1] for row in db_genes])
            gene_diff = db_genes_symbols.symmetric_difference(
                set(gene_values)
            )

            if gene_diff:
                LOGGER.error(
                    f"{panel_name}: {gene_diff} is not in both list of genes"
                )
                return

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

                if len(transcript_data) != len(db_transcripts):
                    msg = (
                        f"{panel_name} - {symbol}: Nb of transcripts is not "
                        "what is excepted"
                    )
                    LOGGER.error(msg)
                    return
                else:
                    db_transcripts_refseq = set(
                        [row[1] for row in db_transcripts]
                    )
                    tx_diff = db_transcripts_refseq.symmetric_difference(
                        set(refseq_values)
                    )

                    if tx_diff:
                        msg = (
                            f"{panel_name} - {symbol}: {tx_diff} is not in "
                            "both list of tx"
                        )
                        LOGGER.error(msg)
                        return

                for tx in db_transcripts:
                    tx_id, refseq, tx_version, tx_gene_id = tx
                    tx = f"{refseq}.{tx_version}"
                    exon_data = gene_dict[symbol]["transcripts"][tx]["exons"]

                    # Get exons for the transcritps
                    exon_values = [int(nb) for nb in exon_data.keys()]
                    db_exons = session.query(exon_tb).filter(
                        exon_tb.c.number.in_(exon_values),
                        exon_tb.c.transcript_id == tx_id
                    ).all()

                    if len(db_exons) != len(exon_data):
                        msg = (
                            f"{panel_name} - {symbol} - Nb of exons for "
                            f"'{refseq}' is not what is expected"
                        )
                        LOGGER.error(msg)
                        return

                    # Get all regions for the transcripts
                    exon_query = session.query(exon_tb.c.region_id).filter(
                        exon_tb.c.transcript_id == tx_id
                    )
                    db_regions = session.query(region_tb).filter(
                        region_tb.c.id.in_(exon_query)
                    ).all()

                    if len(db_regions) != len(exon_data):
                        msg = (
                            f"{panel_name} - {symbol} - {tx}: Nb of regions "
                            f"for {refseq} is not what is expected"
                        )
                        LOGGER.error(msg)
                        return

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
                            msg = (
                                f"{panel_name} - {symbol} - {tx}: "
                                f"Region {db_chrom}:{db_start}-{db_end} is "
                                f"not present in the exons of {refseq}"
                            )
                            LOGGER.error(msg)
                            return

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
                    msg = (
                        f"{panel_name} - {name}: '{symbol}' is not correct"
                    )
                    LOGGER.error(msg)
                    return

                # data stored for the str is not correct
                if (
                    repeated_seq != str_dict[name]["seq"] or
                    str(nb_repeats) != str_dict[name]["nb_normal_repeats"] or
                    str(nb_patho_repeats) != str_dict[name]["nb_pathogenic_repeats"]
                ):
                    msg = (
                        f"{panel_name} - {name}: Data for the str is not correct"
                    )
                    LOGGER.error(msg)
                    msg = f"{repeated_seq} != {str_dict[name]['seq']}"
                    LOGGER.debug(msg)
                    msg = f"{nb_repeats} != {str_dict[name]['nb_normal_repeats']}"
                    LOGGER.debug(msg)
                    msg = f"{nb_patho_repeats} != {str_dict[name]['nb_pathogenic_repeats']}"
                    LOGGER.debug(msg)
                    return

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
                            msg = (
                                f"{panel_name} - {name}: "
                                f"{db_chrom}:{db_start}-{db_end} not in GRCh37"
                            )
                            LOGGER.error(msg)

                    elif ref == grch38_id:
                        if (str(db_start), str(db_end)) not in region_dict[db_chrom]["GRCh38"]:
                            msg = (
                                f"{panel_name} - {name}: "
                                f"{db_chrom}:{db_start}-{db_end} not in GRCh38"
                            )
                            LOGGER.error(msg)

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
                    msg = f"{panel_name} - {name}: Type of cnv is incorrect"
                    LOGGER.error(msg)
                    return

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
                            msg = (
                                f"{panel_name} - {name}: "
                                f"{db_chrom}:{db_start}-{db_end} not in GRCh37"
                            )
                            LOGGER.error(msg)
                    elif ref == grch38_id:
                        if (str(db_start), str(db_end)) not in region_dict[db_chrom]["GRCh38"]:
                            msg = (
                                f"{panel_name} - {name}: "
                                f"{db_chrom}:{db_start}-{db_end} not in GRCh38"
                            )
                            LOGGER.error(msg)

    msg = (
        f"Checking against panelapp dump from {folder} correct on {get_date()}"
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
