from collections import defaultdict
import json
import os

from .logger import setup_logging
from .utils import (
    get_date, assign_transcript, parse_gemini_dump
)


LOGGER = setup_logging("generation")


def generate_panelapp_dump(all_panels: dict, type_panel: str):
    """ Generate tsv for every panelapp panel

    Args:
        all_panels (dict): Dict of all panels in panelapp
        type_panel (str): Type of panels between GMS and all panels

    Returns:
        str: Location where the panels will be written
    """

    LOGGER.info(f"Creating '{type_panel}' panelapp dump")

    output_dump = f"{type_panel}_panelapp_dump"
    output_date = f"{get_date()}"
    output_folder = f"{output_dump}/{output_date}"

    if not os.path.exists(output_dump) and not os.path.isdir(output_dump):
        os.mkdir(output_dump)

        if (
            not os.path.exists(output_folder) and
            not os.path.isdir(output_folder)
        ):
            os.mkdir(output_folder)

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

    LOGGER.info(f"Created panelapp dump: {output_folder}")

    return output_folder


def generate_genepanels(session, meta):
    """ Generate gene panels file

    Args:
        session (SQLAlchemy session): Session object
        meta (SQLAlchemy MetaData): Metadata object

    Returns:
        str: Output file
    """

    LOGGER.info("Creating genepanels file")

    panels = {}
    gene_panels = {}
    genes = {}

    panel_table = meta.tables["panel"]
    gene_table = meta.tables["gene"]
    panel_gene_table = meta.tables["panel_gene"]

    for panel_id, name in session.query(panel_table.c.id, panel_table.c.name):
        panels[panel_id] = name

    for gene_id, symbol in session.query(gene_table.c.id, gene_table.c.symbol):
        genes[gene_id] = symbol

    for gene_id, panel_id in session.query(
        panel_gene_table.c.gene_id, panel_gene_table.c.panel_id
    ):
        gene_panels.setdefault(panel_id, []).append(gene_id)

    if not os.path.exists("sql_dump"):
        os.mkdir("sql_dump")

    output_file = f"sql_dump/{get_date()}_genepanels.tsv"

    with open(output_file, "w") as f:
        # Go through the panel_genes links
        for panel_id, gene_ids in gene_panels.items():
            for gene_id in gene_ids:
                # Use the panel and gene ids to get panel name and gene symbol
                f.write(f"{panels[panel_id]}\t{genes[gene_id]}\n")

    LOGGER.info(f"Created genepanels file: {output_file}")

    return output_file


def generate_gms_panels(gms_panels, confidence_level: int = 3):
    """ Generate gene files for GMS panels

    Args:
        confidence_level (int, optional): Confidence level of genes to get. Defaults to 3.
    """

    LOGGER.info("Creating gms panels")

    out_folder = f"{get_date()}_gms_panels"

    if not os.path.exists(out_folder) and not os.path.isdir(out_folder):
        os.mkdir(out_folder)

    for panel_id, panel in gms_panels.items():
        panel_file = f"{panel.get_name()}_{panel.get_version()}"

        with open(f"{out_folder}/{panel_file}", "w") as f:
            for gene in panel.get_genes(confidence_level):
                f.write(f"{gene}\n")

    LOGGER.info(f"Created gms panels: {out_folder}")


def get_all_transcripts(g2t: str, hgmd_dict: dict, nirvana_dict: dict):
    """ Generate g2t file and genes that have no transcripts file

    Args:
        g2t (str): g2t file (gene\ttranscript)
        hgmd_dict (dict): HGMD dict
        nirvana_dict (dict): nirvana dict
    """

    LOGGER.info("Creating g2t file and no transcript file")

    genes = []

    with open(g2t) as f:
        for line in f:
            gene, transcript = line.strip().split()
            genes.append(gene)

    no_transcript_file = open(f"{get_date()}_no_transcript_gene", "w")
    transcript_file = open(f"{get_date()}_g2t", "w")

    for gene in genes:
        transcript_dict, clinical_transcript = assign_transcript(
            gene, hgmd_dict, nirvana_dict
        )

        if transcript_dict:
            transcript_file.write(f"{gene}\t{clinical_transcript}\n")
        else:
            no_transcript_file.write(f"{gene}\n")

    no_transcript_file.close()
    transcript_file.close()

    msg = (
        f"Created g2t file '{get_date()}_g2t' and no transcript file "
        f"'{get_date()}_no_transcript_gene'"
    )
    LOGGER.info(msg)


def write_django_jsons(json_lists: list):
    """ Write the jsons for every table in the panel_database + full json for importing in the database

    Args:
        json_lists (list): List of dicts for every table

    Returns:
        str: Output folder
    """

    LOGGER.info("Creating json dump for django import")

    today = get_date()
    all_elements = []

    for table, data in json_lists.items():
        for ele in data:
            all_elements.append(ele)

    output_django = "django_fixtures"

    if not os.path.exists(output_django):
        os.mkdir(output_django)

    if not os.path.exists(f"{output_django}/{today}"):
        os.mkdir(f"{output_django}/{today}")

    output = f"{today}_json_dump"

    with open(
        f"{output_django}/{today}/{output}.json", "w", encoding="utf-8"
    ) as f:
        json.dump(all_elements, f, ensure_ascii=False, indent=4)

    for table, data in json_lists.items():
        table_output = f"{today}_{table}.json"

        with open(
            f"{output_django}/{today}/{table_output}", "w", encoding="utf-8"
        ) as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    LOGGER.info(
        f"Created json dump for django import: {output_django}/{today}"
    )

    return output_django


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


def create_django_json(
    ci_dict: dict, test2targets: dict, panelapp_dict: dict,
    superpanel_dict: dict, gene_dict: dict,
    str_dict: dict, cnv_dict: dict, region_dict: dict, pk_dict
):
    """ Create dicts of data for django importing

    Args:
        ci_dict (dict): Dict for clinical indication
        test2targets (dict): Dict for test2targetts
        panelapp_dict (dict): Dict for panelapp data
        superpanel_dict (dict): Dict of superpanels
        gene_dict (dict): Dict for checking if gene's already in the output dict
        str_dict (dict): Dict for checking if str's already in the output dict
        cnv_dict (dict): Dict for checking if cnv's already in the output dict
        region_dict (dict): Dict for checking if region's already in the output dict

    Returns:
        dict: Dict for every table in the database
    """

    test_json = []
    testgene_json = []
    testpanel_json = []

    panel_json = []
    superpanel_json = []
    panelgene_json = []
    gene_json = []

    cnv_json = []
    panelcnv_json = []
    regioncnv_json = []

    str_json = []
    panelstr_json = []
    regionstr_json = []

    transcript_json = []
    exon_json = []
    region_json = []

    reference_json = []

    # Create the list of reference table
    for ref_id, ref in enumerate(["GRCh37", "GRCh38"], pk_dict["reference"]):
        reference_json.append(
            get_django_json("Reference", ref_id, {"name": ref})
        )

    # Create the list for panel, panel_gene, gene, transcript, exon, region for exons
    for panel_id, panelapp_id in enumerate(panelapp_dict, pk_dict["panel"]):
        panel_dict = panelapp_dict[panelapp_id]
        panel_fields = {
                "panelapp_id": panelapp_id, "name": panel_dict["name"],
                "version": panel_dict["version"],
                "signedoff": panel_dict["signedoff"],
                "is_superpanel": False
        }
        panel_json.append(get_django_json("Panel", panel_id, panel_fields))

        # Go through all the genes in the panel
        for gene in panel_dict["genes"]:
            clinical_transcript = gene_dict[gene]["clinical"]
            transcript_data = gene_dict[gene]["transcripts"]

            if gene_dict[gene]["check"] is False:
                # If gene not seen before, create gene json
                pk_dict["gene"] += 1
                gene_fields = {
                    "symbol": gene,
                    "clinical_transcript": clinical_transcript,
                }
                sub_gene_dict = get_django_json(
                    "Gene", pk_dict["gene"], gene_fields
                )

                if transcript_data:
                    # Go through the transcripts
                    for transcript in transcript_data:
                        pk_dict["transcript"] += 1
                        transcript_fields = {
                            "refseq": transcript.split(".")[0],
                            "version": transcript.split(".")[1],
                            "gene": pk_dict["gene"]
                        }
                        transcript_json.append(
                            get_django_json(
                                "Transcript", pk_dict["transcript"],
                                transcript_fields
                            )
                        )

                        if transcript == clinical_transcript:
                            sub_gene_dict["fields"]["clinical_transcript"] = pk_dict["transcript"]

                        # Go through the exons
                        for exon_nb in transcript_data[transcript]["exons"]:
                            exon_data = transcript_data[transcript]["exons"][exon_nb]
                            exon_chrom = exon_data["chrom"]
                            exon_start = exon_data["start"]
                            exon_end = exon_data["end"]
                            exon_ref_id, exon_ref_name = [
                                (ref["pk"], ref["fields"]["name"])
                                for ref in reference_json
                                if ref["fields"]["name"] == "GRCh37"
                            ][0]

                            # Check if seen the region before
                            if region_dict[exon_chrom][exon_ref_name][(
                                exon_start, exon_end
                            )] is False:
                                pk_dict["region"] += 1
                                region_fields = {
                                    "chrom": exon_chrom, "start": exon_start,
                                    "end": exon_end,
                                    "reference": exon_ref_id
                                }
                                region_to_add = get_django_json(
                                    "Region", pk_dict["region"], region_fields
                                )
                                region_json.append(region_to_add)
                                region_dict[exon_chrom][exon_ref_name][(
                                    exon_start, exon_end
                                )] = region_to_add
                                region_id = pk_dict["region"]
                            else:
                                existing_region = region_dict[exon_chrom][exon_ref_name][(
                                    exon_start, exon_end
                                )]
                                region_id = existing_region["pk"]

                            pk_dict["exon"] += 1
                            exon_fields = {
                                "number": exon_nb,
                                "transcript": pk_dict["transcript"],
                                "region": region_id
                            }
                            exon_json.append(
                                get_django_json(
                                    "Exon", pk_dict["exon"], exon_fields
                                )
                            )

                gene_dict[gene]["check"] = sub_gene_dict
                gene_id = sub_gene_dict["pk"]
                gene_json.append(sub_gene_dict)
            else:
                # If seen the gene before get the primary pk for that gene
                gene_id = gene_dict[gene]["check"]["pk"]
                sub_gene_dict = gene_dict[gene]["check"]

            # Create link from Gene to Panel
            pk_dict["panelgene"] += 1
            panelgene_fields = {
                "panel": panel_id,
                "gene": gene_id
            }
            panelgene_json.append(
                get_django_json(
                    "PanelGene", pk_dict["panelgene"], panelgene_fields
                )
            )

    # Create the list for superpanels
    for superpanel_id, superpanel in enumerate(superpanel_dict, panel_id+1):
        subpanel_pks = []
        superpanel_data = superpanel_dict[superpanel]
        print(superpanel_data["name"])

        for subpanel in superpanel_data["subpanels"]:
            subpanel_id = superpanel_data["subpanels"][subpanel]["id"]
            subpanel_pks.append([
                panel_django["pk"]
                for panel_django in panel_json
                if panel_django["fields"]["panelapp_id"] == subpanel_id
            ][0])

        panel_fields = {
            "panelapp_id": superpanel, "name": superpanel_data["name"],
            "version": superpanel_data["version"],
            "signedoff": superpanel_data["signedoff"],
            "is_superpanel": True
        }
        panel_json.append(get_django_json(
            "Panel", superpanel_id, panel_fields
        ))

        # for each subpanel of a superpanel, create a link
        for subpanel_pk in subpanel_pks:
            pk_dict["superpanel"] += 1
            superpanel_fields = {
                "superpanel": superpanel_id, "panel": subpanel_pk
            }
            superpanel_json.append(
                get_django_json(
                    "Superpanel", pk_dict["superpanel"], superpanel_fields
                )
            )

    # Second pass because str have gene and if genes don't exist yet...
    for panelapp_id in panelapp_dict:
        panel_pk = [
            panel_django["pk"]
            for panel_django in panel_json
            if panel_django["fields"]["panelapp_id"] == panelapp_id
        ][0]

        # Go through the strs
        for str_name in panelapp_dict[panelapp_id]["strs"]:
            str_gene = str_dict[str_name]["gene"]
            str_gene_pk = [
                gene_data["pk"]
                for gene_data in gene_json
                if gene_data["fields"]["symbol"] == str_gene
            ][0]

            if str_dict[str_name]["check"] is False:
                pk_dict["str"] += 1
                str_fields = {
                    "gene": str_gene_pk, "name": str_name,
                    "repeated_sequence": str_dict[str_name]["seq"],
                    "nb_repeats": str_dict[str_name]["nb_normal_repeats"],
                    "nb_pathogenic_repeats": str_dict[str_name]["nb_pathogenic_repeats"],
                }
                str_to_add = get_django_json("Str", pk_dict["str"], str_fields)
                str_json.append(str_to_add)

                # Handle region of str for grch37
                str_chrom, str_start, str_end = str_dict[str_name]["grch37"]
                str_ref_id, str_ref_name = [
                    (ref["pk"], ref["fields"]["name"])
                    for ref in reference_json
                    if ref["fields"]["name"] == "GRCh37"
                ][0]

                if str_start is not None and str_end is not None:
                    if region_dict[str_chrom][str_ref_name][(
                        str_start, str_end
                    )] is False:
                        pk_dict["region"] += 1
                        region_fields = {
                            "chrom": str_chrom, "start": str_start,
                            "end": str_end, "reference": str_ref_id
                        }
                        region_to_add = get_django_json(
                            "Region", pk_dict["region"], region_fields
                        )
                        region_json.append(region_to_add)
                        region_dict[str_chrom][str_ref_name][(
                            str_start, str_end
                        )] = region_to_add
                        region_id = pk_dict["region"]
                    else:
                        existing_region = region_dict[str_chrom][str_ref_name][(
                            str_start, str_end
                        )]
                        region_id = existing_region["pk"]

                    pk_dict["regionstr"] += 1
                    regionstr_fields = {
                        "region": region_id, "str": pk_dict["str"]
                    }
                    regionstr_json.append(
                        get_django_json(
                            "RegionStr", pk_dict["regionstr"], regionstr_fields
                        )
                    )

                # Handle region of str for grch38
                str_chrom, str_start, str_end = str_dict[str_name]["grch38"]
                str_ref_id, str_ref_name = [
                    (ref["pk"], ref["fields"]["name"])
                    for ref in reference_json
                    if ref["fields"]["name"] == "GRCh38"
                ][0]

                if str_start is not None and str_end is not None:
                    if region_dict[str_chrom][str_ref_name][(
                        str_start, str_end
                    )] is False:
                        pk_dict["region"] += 1
                        region_fields = {
                            "chrom": str_chrom, "start": str_start,
                            "end": str_end, "reference": str_ref_id
                        }
                        region_to_add = get_django_json(
                            "Region", pk_dict["region"], region_fields
                        )
                        region_json.append(region_to_add)
                        region_dict[str_chrom][str_ref_name][(
                            str_start, str_end
                        )] = region_to_add
                        region_id = pk_dict["region"]
                    else:
                        existing_region = region_dict[str_chrom][str_ref_name][(
                            str_start, str_end
                        )]
                        region_id = existing_region["pk"]

                    pk_dict["regionstr"] += 1
                    regionstr_fields = {
                        "region": region_id, "str": pk_dict["str"]
                    }
                    regionstr_json.append(
                        get_django_json(
                            "RegionStr", pk_dict["regionstr"], regionstr_fields
                        )
                    )

                str_dict[str_name]["check"] = str_to_add
                str_id = str_to_add["pk"]
            else:
                str_id = str_dict[str_name]["check"]["pk"]

            pk_dict["panelstr"] += 1
            panelstr_fields = {"panel": panel_pk, "str": str_id}
            panelstr_json.append(
                get_django_json(
                    "PanelStr", pk_dict["panelstr"], panelstr_fields
                )
            )

        # Go through cnvs
        for cnv in panelapp_dict[panelapp_id]["cnvs"]:
            if cnv_dict[cnv]["check"] is False:
                pk_dict["cnv"] += 1
                cnv_fields = {
                    "name": cnv, "variant_type": cnv_dict[cnv]["type"],
                }
                cnv_to_add = get_django_json("Cnv", pk_dict["cnv"], cnv_fields)
                cnv_json.append(cnv_to_add)

                # Handle region of str for grch37
                cnv_chrom, cnv_start, cnv_end = cnv_dict[cnv]["grch37"]
                cnv_ref_id, cnv_ref_name = [
                    (ref["pk"], ref["fields"]["name"])
                    for ref in reference_json
                    if ref["fields"]["name"] == "GRCh37"
                ][0]

                if cnv_start is not None and cnv_end is not None:
                    if region_dict[cnv_chrom][cnv_ref_name][(
                        cnv_start, cnv_end
                    )] is False:
                        pk_dict["region"] += 1
                        region_fields = {
                            "chrom": cnv_chrom, "start": cnv_start,
                            "end": cnv_end, "reference": cnv_ref_id
                        }
                        region_to_add = get_django_json(
                            "Region", pk_dict["region"], region_fields
                        )
                        region_json.append(region_to_add)
                        region_dict[cnv_chrom][cnv_ref_name][(
                            cnv_start, cnv_end
                        )] = region_to_add
                        region_id = pk_dict["region"]
                    else:
                        existing_region = region_dict[cnv_chrom][cnv_ref_name][(
                            cnv_start, cnv_end
                        )]
                        region_id = existing_region["pk"]

                    pk_dict["regioncnv"] += 1
                    regioncnv_fields = {
                        "region": region_id, "cnv": pk_dict["cnv"]
                    }
                    regioncnv_json.append(
                        get_django_json(
                            "RegionCnv", pk_dict["regioncnv"], regioncnv_fields
                        )
                    )

                # Handle region of str for grch38
                cnv_chrom, cnv_start, cnv_end = cnv_dict[cnv]["grch38"]
                cnv_ref_id, cnv_ref_name = [
                    (ref["pk"], ref["fields"]["name"])
                    for ref in reference_json
                    if ref["fields"]["name"] == "GRCh38"
                ][0]

                if cnv_start is not None and cnv_end is not None:
                    if region_dict[cnv_chrom][cnv_ref_name][(
                        cnv_start, cnv_end
                    )] is False:
                        pk_dict["region"] += 1
                        region_fields = {
                            "chrom": cnv_chrom, "start": cnv_start,
                            "end": cnv_end, "reference": cnv_ref_id
                        }
                        region_to_add = get_django_json(
                            "Region", pk_dict["region"], region_fields
                        )
                        region_json.append(region_to_add)
                        region_dict[cnv_chrom][cnv_ref_name][(
                            cnv_start, cnv_end
                        )] = region_to_add
                        region_id = pk_dict["region"]
                    else:
                        existing_region = region_dict[cnv_chrom][cnv_ref_name][(
                            cnv_start, cnv_end
                        )]
                        region_id = existing_region["pk"]

                    pk_dict["regioncnv"] += 1
                    regioncnv_fields = {
                        "region": region_id, "cnv": pk_dict["cnv"]
                    }
                    regioncnv_json.append(
                        get_django_json(
                            "RegionCnv", pk_dict["regioncnv"], regioncnv_fields
                        )
                    )

                cnv_dict[cnv]["check"] = cnv_to_add
                cnv_id = cnv_to_add["pk"]
            else:
                cnv_id = cnv_dict[cnv]["check"]["pk"]

            pk_dict["panelcnv"] += 1
            panelcnv_fields = {"panel": panel_pk, "cnv": cnv_id}
            panelcnv_json.append(
                get_django_json(
                    "PanelCnv", pk_dict["panelcnv"], panelcnv_fields
                )
            )

    # Go through tests to assign panels and genes
    for test_pk, test in enumerate(test2targets, pk_dict["test"]):
        clinind_id, test_id = test.split(".")
        name = ci_dict[clinind_id]
        method = test2targets[test]["method"]
        gemini_name = test2targets[test]["gemini_name"]

        test_fields = {
            "test_id": test,
            "name": name,
            "method": method,
            "version": get_date(),
            "gemini_name": gemini_name,
        }
        test_json.append(get_django_json("Test", test_pk, test_fields))

        # Create test_panel json
        for panel in test2targets[test]["panels"]:
            panel_pk = [
                panel_django["pk"]
                for panel_django in panel_json
                if panel_django["fields"]["panelapp_id"] == panel
            ]

            if panel_pk:
                panel_pk = panel_pk[0]
                pk_dict["testpanel"] += 1
                testpanel_fields = {"test": test_pk, "panel": panel_pk}
                testpanel_json.append(
                    get_django_json(
                        "TestPanel", pk_dict["testpanel"], testpanel_fields
                    )
                )

        # Create test_genes json
        for gene in test2targets[test]["genes"]:
            pk_dict["gene"] = [
                gene_django["pk"]
                for gene_django in gene_json
                if gene_django["fields"]["symbol"] == gene
            ]

            if pk_dict["gene"]:
                pk_dict["gene"] = pk_dict["gene"][0]
                pk_dict["testgene"] += 1
                testgene_fields = {"test": test_pk, "gene": pk_dict["gene"]}
                testgene_json.append(
                    get_django_json(
                        "TestGene", pk_dict["testgene"], testgene_fields
                    )
                )

    return {
        "test": test_json, "testpanel": testpanel_json,
        "testgene": testgene_json, "panel": panel_json,
        "superpanel": superpanel_json, "panelgene": panelgene_json,
        "gene": gene_json, "panelstr": panelstr_json, "cnv": cnv_json,
        "regioncnv": regioncnv_json, "panelcnv": panelcnv_json,
        "str": str_json, "regionstr": regionstr_json,
        "transcript": transcript_json, "exon": exon_json,
        "region": region_json, "reference": reference_json
    }


def generate_gemini_names(session, meta, test2targets: dict):
    """ Generate gemini names file

    Args:
        session (SQLAlchemy session): Session object
        meta (SQLAlchemy MetaData): Metadata object
        test2targets (dict): Dict from the xls
    """

    LOGGER.info("Creating gemini names file")

    test_table = meta.tables["test"]

    db_tests = [row for row in session.query(test_table.c.gemini_name)]

    if len(db_tests) == len(test2targets):
        print("Nb of tests in db as expected")
    else:
        print(len(test2targets))
        print(db_tests)
        return

    if not os.path.exists("sql_dump"):
        os.mkdir("sql_dump")

    output_file = f"sql_dump/{get_date()}_gemini_names.txt"

    with open(output_file, "w") as f:
        for test in db_tests:
            gemini_name = test[0]
            f.write(f"{gemini_name}\n")

    LOGGER.info(f"Created gemini names file: {output_file}")


def generate_sample2panels(session, meta, gemini_dump):
    """ Generate new bioinformatic manifest for the new database

    Args:
        session (SQLAlchemy Session): Session to make queries
        meta (SQLAlchemy metadata): Metadata to get the tables from the existing db
        gemini_dump (file_path): Gemini dump file

    Returns:
        str: File path of the output file
    """

    LOGGER.info("Creating sample2panels file")

    # get the content of the gemini dump
    sample2gm_panels = parse_gemini_dump(gemini_dump)

    # get the panels/genes from the db now
    test_table = meta.tables["test"]
    test_panel_table = meta.tables["test_panel"]
    test_gene_table = meta.tables["test_gene"]
    panel_gene_table = meta.tables["panel_gene"]
    gene_table = meta.tables["gene"]

    uniq_used_panels = set(sample2gm_panels.values())

    # get the gemini names and associated genes and panels ids
    test_queries = session.query(
        test_table.c.gemini_name,
        test_gene_table.c.gene_id,
        test_panel_table.c.panel_id
    ).outerjoin(test_panel_table).outerjoin(test_gene_table).filter(
        test_table.c.gemini_name.in_(uniq_used_panels)
    ).all()

    gemini2genes = defaultdict(lambda: set())

    for test in test_queries:
        gemini_name, gene_id, panel_id = test

        # query genes from test_gene output
        if gene_id:
            gene = session.query(gene_table.c.symbol).filter(
                gene_table.c.id == gene_id
            ).one()[0]

            gemini2genes[gemini_name].update([gene])

        # query genes from test_panel output
        elif panel_id:
            panels_genes = session.query(
                gene_table.c.symbol
            ).outerjoin(panel_gene_table).filter(
                panel_gene_table.c.panel_id == panel_id
            ).all()

            panels_genes = [gene[0] for gene in panels_genes]

            gemini2genes[gemini_name].update(panels_genes)

    if not os.path.exists("sql_dump"):
        os.mkdir("sql_dump")

    output_file = f"sql_dump/{get_date()}_sample2panels.tsv"

    with open(output_file, "w") as f:
        for sample, panel in sample2gm_panels.items():
            # match gemini names from the dump to the genes in the db
            if panel in gemini2genes:
                for gene in gemini2genes[panel]:
                    f.write(f"{sample}\t{panel}\tNA\t{gene}\n")

    LOGGER.info(f"Created sample2panels file: {output_file}")

    return output_file


def generate_panel_names(session, meta, gms):
    """ Generate txt file with all the panel names

    Args:
        session (): [description]
        meta ([type]): [description]
        gms ([type]): [description]
    """

    LOGGER.info("Creating panel names file")

    panel_table = meta.tables["panel"]

    if gms is True:
        test_queries = sorted(session.query(
            panel_table.c.name, panel_table.c.version
        ).filter(
            panel_table.c.signedoff != "False"
        ).all(), key=lambda t: t[0])
        output_file = f"sql_dump/{get_date()}_gms_panels.txt"
    elif gms is False:
        test_queries = sorted(session.query(
            panel_table.c.name, panel_table.c.version
        ).all(), key=lambda t: t[0])
        output_file = f"sql_dump/{get_date()}_all_panels.txt"
    else:
        test_queries = None
        output_file = None

    if not os.path.exists("sql_dump"):
        os.mkdir("sql_dump")

    with open(output_file, "w") as f:
        for row in test_queries:
            name, version = row
            f.write(f"{name}_{version}\n")

    LOGGER.info(f"Created panel names file: {output_file}")
