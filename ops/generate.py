import json
import os

import MySQLdb
from panelapp import queries

from .utils import get_date, check_if_seen_before, assign_transcript


def generate_panelapp_dump(all_panels: dict, type_panel: str):
    """ Generate tsv for every panelapp panel

    Args:
        all_panels (dict): Dict of all panels in panelapp

    Returns:
        str: Location where the panels will be written
    """

    output_folder = f"{get_date()}_panelapp_{type_panel}_dump"

    for panel_id, panel in all_panels.items():
        print(panel)
        panel.write(output_folder)

    return output_folder


def generate_genepanels(c):
    """ Generate gene panels file

    Args:
        c (MySQLdd cursor): Cursor connected to panel_database

    Returns:
        str: Output file
    """

    panels = {}
    gene_panels = {}
    genes = {}

    # Get the panel ids and store it
    c.execute(
        """SELECT id, name from panel;"""
    )

    for panel in c.fetchall():
        panel_id, name = panel
        panels[panel_id] = name

    # Get the gene ids and store it
    c.execute(
        """SELECT id, symbol from gene;"""
    )

    for gene in c.fetchall():
        gene_id, symbol = gene
        genes[gene_id] = symbol

    # Get the panel_gene ids and store it
    c.execute(
        """SELECT gene_id, panel_id from panel_gene;"""
    )

    for row in c.fetchall():
        gene_id, panel_id = row
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

    return output_file


def generate_gms_panels(confidence_level: int = 3):
    """ Generate gene files for GMS panels

    Args:
        confidence_level (int, optional): Confidence level of genes to get. Defaults to 3.
    """

    out_folder = f"{get_date()}_gms_panels"

    if not os.path.exists(out_folder) and not os.path.isdir(out_folder):
        os.mkdir(out_folder)

    signedoff = queries.get_all_signedoff_panels()

    for panel_id, panel in signedoff.items():
        panel_file = f"{panel.get_name()}_{panel.get_version()}"

        with open(f"{out_folder}/{panel_file}", "w") as f:
            for gene in panel.get_genes(confidence_level):
                f.write(f"{gene}\n")


def get_all_transcripts(g2t: str, hgmd_dict: dict, nirvana_dict: dict):
    """ Generate g2t file and genes that have no transcripts file

    Args:
        g2t (str): g2t file (gene\ttranscript)
        hgmd_dict (dict): HGMD dict
        nirvana_dict (dict): nirvana dict
    """

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


def write_django_jsons(json_lists: list):
    """ Write the jsons for every table in the panel_database + full json for importing in the database

    Args:
        json_lists (list): List of dicts for every table

    Returns:
        str: Output folder
    """

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
    ci_dict: dict, test2targets: dict, panelapp_dict: dict, gene_dict: dict,
    str_dict: dict, cnv_dict: dict, region_dict: dict
):
    """ Create dicts of data for django importing

    Args:
        ci_dict (dict): Dict for clinical indication
        test2targets (dict): Dict for test2targetts
        panelapp_dict (dict): Dict for panelapp data
        gene_dict (dict): Dict for checking if gene's already in the output dict
        str_dict (dict): Dict for checking if str's already in the output dict
        cnv_dict (dict): Dict for checking if cnv's already in the output dict
        region_dict (dict): Dict for checking if region's already in the output dict

    Returns:
        list: List of dict for every table in the database
    """

    test_json = []
    testgene_json = []
    testpanel_json = []

    panel_json = []
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

    # Loads of variables for primary keys
    testpanel_pk = testgene_pk = 0
    panelgene_pk = panelstr_pk = panelcnv_pk = 0
    gene_pk = transcript_pk = exon_pk = 0
    str_pk = regionstr_pk = 0
    cnv_pk = regioncnv_pk = 0
    region_pk = regioncnv_pk = 0

    # Create the list of reference table
    for ref_id, ref in enumerate(["GRCh37", "GRCh38"], 1):
        reference_json.append(
            get_django_json("Reference", ref_id, {"name": ref})
        )

    # Create the list for panel, panel_gene, gene, transcript, exon, region for exons
    for panel_id, panelapp_id in enumerate(panelapp_dict, 1):
        panel_dict = panelapp_dict[panelapp_id]
        print(panel_dict["name"])
        panel_fields = {
                "panelapp_id": panelapp_id, "name": panel_dict["name"],
                "version": panel_dict["version"],
                "signedoff": panel_dict["signedoff"]
        }
        panel_json.append(get_django_json("Panel", panel_id, panel_fields))

        # Go through all the genes in the panel
        for gene in panelapp_dict[panelapp_id]["genes"]:
            clinical_transcript = gene_dict[gene]["clinical"]
            transcript_data = gene_dict[gene]["transcripts"]

            if check_if_seen_before(
                gene_dict[gene]["check"]
            ) is False:
                # If gene not seen before, create gene json
                gene_pk += 1
                gene_fields = {
                    "symbol": gene,
                    "clinical_transcript_id": clinical_transcript,
                }
                sub_gene_dict = get_django_json("Gene", gene_pk, gene_fields)

                if transcript_data:
                    # Go through the transcripts
                    for transcript in transcript_data:
                        transcript_pk += 1
                        transcript_fields = {
                            "refseq": transcript.split(".")[0],
                            "version": transcript.split(".")[1],
                            "gene_id": gene_pk
                        }
                        transcript_json.append(
                            get_django_json(
                                "Transcript", transcript_pk, transcript_fields
                            )
                        )

                        if transcript == clinical_transcript:
                            sub_gene_dict["fields"]["clinical_transcript_id"] = transcript_pk

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
                            if check_if_seen_before(
                                region_dict[exon_chrom][exon_ref_name][(
                                    exon_start, exon_end
                                )]
                            ) is False:
                                region_pk += 1
                                region_fields = {
                                    "chrom": exon_chrom, "start": exon_start,
                                    "end": exon_end,
                                    "reference_id": exon_ref_id
                                }
                                region_to_add = get_django_json(
                                    "Region", region_pk, region_fields
                                )
                                region_json.append(region_to_add)
                                region_dict[exon_chrom][exon_ref_name][(
                                    exon_start, exon_end
                                )] = region_to_add
                                region_id = region_pk
                            else:
                                existing_region = region_dict[exon_chrom][exon_ref_name][(
                                    exon_start, exon_end
                                )]
                                region_id = existing_region["pk"]

                            exon_pk += 1
                            exon_fields = {
                                "number": exon_nb,
                                "transcript_id": transcript_pk,
                                "region_id": region_id
                            }
                            exon_json.append(
                                get_django_json("Exon", exon_pk, exon_fields)
                            )

                gene_dict[gene]["check"] = sub_gene_dict
                gene_id = sub_gene_dict["pk"]
                gene_json.append(sub_gene_dict)
            else:
                # If seen the gene before get the primary pk for that gene
                gene_id = gene_dict[gene]["check"]["pk"]
                sub_gene_dict = gene_dict[gene]["check"]

            # Create link from Gene to Panel
            panelgene_pk += 1
            panelgene_fields = {
                "panel_id": panel_id,
                "gene_id": gene_id
            }
            panelgene_json.append(
                get_django_json("PanelGene", panelgene_pk, panelgene_fields)
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

            if check_if_seen_before(
                str_dict[str_name]["check"]
            ) is False:
                str_pk += 1
                str_fields = {
                    "gene_id": str_gene_pk, "name": str_name,
                    "repeated_sequence": str_dict[str_name]["seq"],
                    "nb_repeats": str_dict[str_name]["nb_normal_repeats"],
                    "nb_pathogenic_repeats": str_dict[str_name]["nb_pathogenic_repeats"],
                }
                str_to_add = get_django_json("Str", str_pk, str_fields)
                str_json.append(str_to_add)

                # Handle region of str for grch37
                str_chrom, str_start, str_end = str_dict[str_name]["grch37"]
                str_ref_id, str_ref_name = [
                    (ref["pk"], ref["fields"]["name"])
                    for ref in reference_json
                    if ref["fields"]["name"] == "GRCh37"
                ][0]

                if str_start is not None and str_end is not None:
                    if check_if_seen_before(
                        region_dict[str_chrom][str_ref_name][(
                            str_start, str_end
                        )]
                    ) is False:
                        region_pk += 1
                        region_fields = {
                            "chrom": str_chrom, "start": str_start,
                            "end": str_end, "reference_id": str_ref_id
                        }
                        region_to_add = get_django_json(
                            "Region", region_pk, region_fields
                        )
                        region_json.append(region_to_add)
                        region_dict[str_chrom][str_ref_name][(
                            str_start, str_end
                        )] = region_to_add
                        region_id = region_pk
                    else:
                        existing_region = region_dict[str_chrom][str_ref_name][(
                            str_start, str_end
                        )]
                        region_id = existing_region["pk"]

                    regionstr_pk += 1
                    regionstr_fields = {
                        "region_id": region_id,
                        "str_id": str_pk
                    }
                    regionstr_json.append(
                        get_django_json(
                            "RegionStr", regionstr_pk, regionstr_fields
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
                    if check_if_seen_before(
                        region_dict[str_chrom][str_ref_name][(
                            str_start, str_end
                        )]
                    ) is False:
                        region_pk += 1
                        region_fields = {
                            "chrom": str_chrom, "start": str_start,
                            "end": str_end, "reference_id": str_ref_id
                        }
                        region_to_add = get_django_json(
                            "Region", region_pk, region_fields
                        )
                        region_json.append(region_to_add)
                        region_dict[str_chrom][str_ref_name][(
                            str_start, str_end
                        )] = region_to_add
                        region_id = region_pk
                    else:
                        existing_region = region_dict[str_chrom][str_ref_name][(
                            str_start, str_end
                        )]
                        region_id = existing_region["pk"]

                    regionstr_pk += 1
                    regionstr_fields = {
                        "region_id": region_id,
                        "str_id": str_pk
                    }
                    regionstr_json.append(
                        get_django_json(
                            "RegionStr", regionstr_pk, regionstr_fields
                        )
                    )

                str_dict[str_name]["check"] = str_to_add
                str_id = str_to_add["pk"]
            else:
                str_id = str_dict[str_name]["check"]["pk"]

            panelstr_pk += 1
            panelstr_fields = {"panel_id": panel_pk, "str_id": str_id}
            panelstr_json.append(
                get_django_json("PanelStr", panelstr_pk, panelstr_fields)
            )

        # Go through cnvs
        for cnv in panelapp_dict[panelapp_id]["cnvs"]:
            if check_if_seen_before(cnv_dict[cnv]["check"]) is False:
                cnv_pk += 1
                cnv_fields = {
                    "name": cnv, "variant_type": cnv_dict[cnv]["type"],
                }
                cnv_to_add = get_django_json("Cnv", cnv_pk, cnv_fields)
                cnv_json.append(cnv_to_add)

                # Handle region of str for grch37
                cnv_chrom, cnv_start, cnv_end = cnv_dict[cnv]["grch37"]
                cnv_ref_id, cnv_ref_name = [
                    (ref["pk"], ref["fields"]["name"])
                    for ref in reference_json
                    if ref["fields"]["name"] == "GRCh37"
                ][0]

                if cnv_start is not None and cnv_end is not None:
                    if check_if_seen_before(
                        region_dict[cnv_chrom][cnv_ref_name][(
                            cnv_start, cnv_end
                        )]
                    ) is False:
                        region_pk += 1
                        region_fields = {
                            "chrom": cnv_chrom, "start": cnv_start,
                            "end": cnv_end, "reference_id": cnv_ref_id
                        }
                        region_to_add = get_django_json(
                            "Region", region_pk, region_fields
                        )
                        region_json.append(region_to_add)
                        region_dict[cnv_chrom][cnv_ref_name][(
                            cnv_start, cnv_end
                        )] = region_to_add
                        region_id = region_pk
                    else:
                        existing_region = region_dict[cnv_chrom][cnv_ref_name][(
                            cnv_start, cnv_end
                        )]
                        region_id = existing_region["pk"]

                    regioncnv_pk += 1
                    regioncnv_fields = {
                        "region_id": region_id, "cnv_id": cnv_pk
                    }
                    regioncnv_json.append(
                        get_django_json(
                            "RegionCnv", regioncnv_pk, regioncnv_fields
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
                    if check_if_seen_before(
                        region_dict[cnv_chrom][cnv_ref_name][(
                            cnv_start, cnv_end
                        )]
                    ) is False:
                        region_pk += 1
                        region_fields = {
                            "chrom": cnv_chrom, "start": cnv_start,
                            "end": cnv_end, "reference_id": cnv_ref_id
                        }
                        region_to_add = get_django_json(
                            "Region", region_pk, region_fields
                        )
                        region_json.append(region_to_add)
                        region_dict[cnv_chrom][cnv_ref_name][(
                            cnv_start, cnv_end
                        )] = region_to_add
                        region_id = region_pk
                    else:
                        existing_region = region_dict[cnv_chrom][cnv_ref_name][(
                            cnv_start, cnv_end
                        )]
                        region_id = existing_region["pk"]

                    regioncnv_pk += 1
                    regioncnv_fields = {
                        "region_id": region_id, "cnv_id": cnv_pk
                    }
                    regioncnv_json.append(
                        get_django_json(
                            "RegionCnv", regioncnv_pk, regioncnv_fields
                        )
                    )

                cnv_dict[cnv]["check"] = cnv_to_add
                cnv_id = cnv_to_add["pk"]
            else:
                cnv_id = cnv_dict[cnv]["check"]["pk"]

            panelcnv_pk += 1
            panelcnv_fields = {"panel_id": panel_pk, "cnv_id": cnv_id}
            panelcnv_json.append(
                get_django_json("PanelCnv", panelcnv_pk, panelcnv_fields)
            )

    # Go through tests to assign panels and genes
    for test_pk, test in enumerate(test2targets, 1):
        clinind_id, test_id = test.split(".")
        name = ci_dict[clinind_id]
        method = test2targets[test]["method"]
        test_fields = {
            "test_id": test,
            "name": name,
            "method": method,
            "version": get_date(),
            "gemini_name": f"{test}_{name}_{method}",
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
                testpanel_pk += 1
                testpanel_fields = {"test_id": test_pk, "panel_id": panel_pk}
                testpanel_json.append(
                    get_django_json("TestPanel", testpanel_pk, testpanel_fields)
                )

        # Create test_genes json
        for gene in test2targets[test]["genes"]:
            gene_pk = [
                gene_django["pk"]
                for gene_django in gene_json
                if gene_django["fields"]["symbol"] == gene
            ]

            if gene_pk:
                gene_pk = gene_pk[0]
                testgene_pk += 1
                testgene_fields = {"test_id": test_pk, "gene_id": gene_pk}
                testgene_json.append(
                    get_django_json("TestGene", testgene_pk, testgene_fields)
                )

    return {
        "test": test_json, "testpanel": testpanel_json, "testgene": testgene_json,
        "panel": panel_json, "panelgene": panelgene_json, "gene": gene_json,
        "panelstr": panelstr_json, "cnv": cnv_json,
        "regioncnv": regioncnv_json, "panelcnv": panelcnv_json,
        "str": str_json, "regionstr": regionstr_json,
        "transcript": transcript_json, "exon": exon_json,
        "region": region_json, "reference": reference_json
    }


def generate_gemini_names(c, test2targets: dict):
    """ Generate gemini names file

    Args:
        c (MySQLdb cursor): Cursor connected to the panel_database
        test2targets (dict): Dict from the xls
    """

    c.execute("SELECT gemini_name FROM test")
    db_tests = c.fetchall()

    if len(db_tests) == len(test2targets):
        print("Nb of tests in db as expected")
    else:
        print(len(test2targets))
        print(db_tests)
        return

    if not os.path.exists("sql_dump"):
        os.mkdir("sql_dump")

    with open(f"sql_dump/{get_date()}_gemini_names.txt", "w") as f:
        for test in db_tests:
            gemini_name = test[0]
            f.write(f"{gemini_name}\n")
