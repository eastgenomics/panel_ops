from .utils import assign_transcript, create_panelapp_dict


def check_gene(gene: str, hgmd_dict: dict, nirvana_dict: dict):
    """Returns the transcripts and exons for given gene according to HGMD and nirvana

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


def check_panelapp_dump_against_db(
    folder: str, session, meta, hgmd_dict: dict, nirvana_dict: dict
):
    """ Check that the data in the panelapp dump is the same as what's in the db

    Args:
        folder (str): Folder in which panels are stored
        session (SQLAlchemy session): Session object
        meta (SQLAlchemy MetaData): Metadata object
        hgmd_dict (dict): Dict of data stored in HGMD
        nirvana_dict (dict): Dict of parsed data from Nirvana GFF
    """

    (
        panelapp_dict, gene_dict, str_dict, cnv_dict, region_dict
    ) = create_panelapp_dict(folder, hgmd_dict, nirvana_dict)

    ref_tb = meta.tables["reference"]
    panel_tb = meta.tables["panel"]
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

    # Get all panels using panelapp ids in panelapp dict
    panel_values = [int(id_) for id_ in panelapp_dict.keys()]
    db_panels = session.query(panel_tb).filter(
        panel_tb.c.panelapp_id.in_(panel_values)
    ).all()

    if len(db_panels) == len(panelapp_dict):
        print("Panels are good")
    else:
        print("Panels are bad")
    for panel_row in db_panels:
        panel_id, panelapp_id, name, version, signedoff = panel_row
        print(name)

        # Get all genes for that panel
        gene_values = list(panelapp_dict[str(panelapp_id)]["genes"])
        db_genes = session.query(gene_tb).filter(
            gene_tb.c.symbol.in_(gene_values)
        ).all()

        # Get all panel2gene links for that panel
        db_panel_genes = session.query(panel_gene_tb).filter(
            panel_gene_tb.c.panel_id == panel_id
        ).all()

        if len(db_panel_genes) != len(db_genes):
            print("Nb of genes")
            print(len(db_genes))
            print(len(db_panel_genes))
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
                    print("Nb transcripts")
                    print(symbol)
                    print(db_transcripts)
                    print(transcript_data.keys())
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
                        print("Nb exons")
                        print(tx)
                        print(db_exons)
                        print(exon_data)
                        return

                    # Get all regions for the transcripts
                    exon_query = session.query(exon_tb.c.region_id).filter(
                        exon_tb.c.transcript_id == tx_id
                    )
                    db_regions = session.query(region_tb).filter(
                        region_tb.c.id.in_(exon_query)
                    ).all()

                    if len(db_regions) != len(exon_data):
                        print("Nb of regions")
                        print(tx)
                        print(len(exon_data))
                        print(exon_data)
                        print(len(db_regions))
                        print(db_regions)
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
                            print("Region")
                            print(exons)
                            print((db_chrom, db_start, db_end))
                            return

        str_values = list(panelapp_dict[str(panelapp_id)]["strs"])

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

                if symbol != str_dict[name]["gene"]:
                    print(f"Gene associated with {name} is not correct:")
                    print(symbol)
                    print(str_dict[name]["gene"])
                    return

                if (
                    repeated_seq != str_dict[name]["seq"] or
                    str(nb_repeats) != str_dict[name]["nb_normal_repeats"] or
                    str(nb_patho_repeats) != str_dict[name]["nb_pathogenic_repeats"]
                ):
                    print("STR")
                    print(str_dict[name])
                    print(str_row)
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
                            print("STR GRCh37")
                            print(region_dict[db_chrom]["GRCh37"])
                    elif ref == grch38_id:
                        if (str(db_start), str(db_end)) not in region_dict[db_chrom]["GRCh38"]:
                            print("STR GRCh38")
                            print(region_dict[db_chrom]["GRCh38"])

        cnv_values = list(panelapp_dict[str(panelapp_id)]["cnvs"])

        if cnv_values:
            db_cnvs = session.query(cnv_tb).filter(
                cnv_tb.c.name.in_(cnv_values)
            ).all()

            for cnv_row in db_cnvs:
                (cnv_id, name, variant_type) = cnv_row

                if (variant_type != cnv_dict[name]["type"]):
                    print("CNV")
                    print(cnv_dict[name])
                    print(variant_type)
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
                            print("CNV GRCh37")
                            print(region_dict[db_chrom]["GRCh37"])
                    elif ref == grch38_id:
                        if (str(db_start), str(db_end)) not in region_dict[db_chrom]["GRCh38"]:
                            print("CNV GRCh38")
                            print(region_dict[db_chrom]["GRCh38"])

    return True


def check_test_against_db(session, meta, test2target: dict):
    """Check if the tests have been imported correctly

    Args:
        session (SQLAlchemy session): Session object
        meta (SQLAlchemy MetaData): Metadata object
        test2target (dict): Dict of test2target
    """

    test_tb = meta.tables["test"]
    test_panel_tb = meta.tables["test_panel"]
    panel_tb = meta.tables["panel"]
    test_gene_tb = meta.tables["test_gene"]
    gene_tb = meta.tables["gene"]

    test_ids = [test for test in test2target]
    db_tests = session.query(test_tb).filter(
        test_tb.c.test_id.in_(test_ids)
    ).all()

    # Check if the number of imported tests is the same as the number of tests in the xls
    if len(test2target) != len(db_tests):
        print("Tests are bad")
        return

    for test in db_tests:
        db_test_id, test_id, clin_ind, method, date, gem_name = test
        print(test_id)

        panel_targets = test2target[test_id]["panels"]

        # If the test has panels associated to it
        if panel_targets != []:
            # Get the panels associated to the test in the db
            db_panel_targets = session.query(test_panel_tb).filter(
                test_panel_tb.c.test_id == db_test_id
            ).all()

            # From the associated panels, check if the panelapp id imported is the same as the one in the xls
            for panel_target in db_panel_targets:
                test_panel_id, panel_id, db_test_id = panel_target
                panelapp_id = session.query(panel_tb.c.panelapp_id).filter(
                    panel_tb.c.id == panel_id
                ).one()[0]

                if str(panelapp_id) not in test2target[test_id]["panels"]:
                    print("Panel is bad")
                    print(panel_id)
                    print(panelapp_id)
                    print(test2target[test_id]["panels"])
                    return

        gene_targets = test2target[test_id]["genes"]

        # If the test has genes associated to it
        if gene_targets != []:
            # Get the genes associated to the test in the db
            db_gene_targets = session.query(test_gene_tb).filter(
                test_gene_tb.c.test_id == db_test_id
            ).all()

            # From the associated genes, check if the gene imported is the same as the one(s) in the xls
            for gene_target in db_gene_targets:
                test_panel_id, gene_id, db_test_id = gene_target
                symbol = session.query(gene_tb.c.symbol).filter(
                    gene_tb.c.id == gene_id
                ).one()[0]

                if symbol not in test2target[test_id]["genes"]:
                    print("Gene is bad")
                    return

    return True
