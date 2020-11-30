def get_gemini_name(session, meta, part_of_gemini_name: str):
    """ Return the gemini name using part of this name

    Args:
        session (SQLAlchemy session): Session object
        meta (SQLAlchemy meta): Meta data used for retrieving data in the db
        part_of_gemini_name (str): Part of the gemini name

    Returns:
        str: Full gemini name
    """

    test_tb = meta.tables["test"]

    test_name = session.query(test_tb.c.gemini_name).filter(
        test_tb.c.gemini_name.like(f"%{part_of_gemini_name}%")
    ).one()[0]

    print(test_name)

    return test_name


def get_genes_from_gemini_name(session, meta, gemini_name: str):
    """ Return list of genes associated with gemini name

    Args:
        session (SQLAlchemy session): Session object
        meta (SQLAlchemy meta): Meta data used for retrieving data in the db
        gemini_name (str): Part of the gemini name

    Returns:
        str: Csv string of genes ready for reanalysis
    """

    test_tb = meta.tables["test"]
    test_panel_tb = meta.tables["test_panel"]
    test_gene_tb = meta.tables["test_gene"]
    panel_gene_tb = meta.tables["panel_gene"]
    gene_tb = meta.tables["gene"]

    test_id = session.query(test_tb.c.id).filter(
        test_tb.c.gemini_name.like(f"%{gemini_name}%")
    ).one()[0]

    test2genes = session.query(test_gene_tb.c.gene_id).filter(
        test_gene_tb.c.test_id == test_id
    ).all()

    test2panels = session.query(test_panel_tb.c.panel_id).filter(
        test_panel_tb.c.test_id == test_id
    ).all()

    panel2genes = session.query(panel_gene_tb.c.gene_id).filter(
        panel_gene_tb.c.panel_id.in_(test2panels)
    ).all()

    genes_from_tests = session.query(gene_tb.c.symbol).filter(
        gene_tb.c.id.in_(test2genes)
    ).all()

    genes_from_panels = session.query(gene_tb.c.symbol).filter(
        gene_tb.c.id.in_(panel2genes)
    ).all()

    genes = genes_from_tests + genes_from_panels

    print(",".join([f"_{gene[0]}" for gene in genes]))

    return ",".join([f"_{gene[0]}" for gene in genes])
