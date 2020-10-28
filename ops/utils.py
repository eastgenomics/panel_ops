from collections import defaultdict
import datetime
import gzip
import os

import dxpy
import MySQLdb
import regex
import vcf
import xlrd

from panelapp import queries


def assign_transcript(gene: str, hgmd_dict: dict, nirvana_dict: dict):
    """ Return transcript data and clinical transcript from hgmd/nirvana

    Args:
        gene (str): Gene symbol
        hgmd_dict (dict): Dict of parsed data from hgmd
        nirvana_dict (dict): Dict of parsed data from gff nirvana

    Returns:
        tuple: Dict of transcript data and clinical transcript refseq
    """

    transcript_data = None
    clinical_transcript = None
    uppered_gene = gene.upper()

    if uppered_gene in hgmd_dict:
        if uppered_gene in nirvana_dict:
            transcript_data = nirvana_dict[uppered_gene]
            hgmd_tx = hgmd_dict[uppered_gene]

            if hgmd_tx in transcript_data:
                clinical_transcript = hgmd_tx
            else:
                for transcript in transcript_data:
                    if transcript_data[transcript]["canonical"]:
                        clinical_transcript = transcript
    else:
        if uppered_gene in nirvana_dict:
            transcript_data = nirvana_dict[uppered_gene]

            for transcript in transcript_data:
                if transcript_data[transcript]["canonical"]:
                    clinical_transcript = transcript

    return transcript_data, clinical_transcript


def get_date():
    """ Return today's date in YYMMDD format

    Returns:
        str: Date
    """

    return str(datetime.date.today())[2:].replace("-", "")


def connect_to_db():
    """ Return cursor of panel_database

    Returns:
        MySQLdb cursor: Panel_database cursor
    """

    db = MySQLdb.connect(
        host="localhost", user="kimy",
        passwd="panel_database", db="panel_database"
    )
    c = db.cursor()
    return c


def get_all_panels():
    """ Return dict of panelapp_id to panel object

    Returns:
        dict: Dict of all panels in panelapp
    """

    signedoff_panels = queries.get_all_signedoff_panels()
    all_panels = queries.get_all_panels()

    for panel_id, panel in signedoff_panels.items():
        all_panels[panel_id] = panel

    return all_panels


def get_GMS_panels():
    """ Return dict of panelapp_id to panel object

    Returns:
        dict: Dict of GMS panels in panelapp
    """

    return queries.get_all_signedoff_panels()


def parse_HGMD():
    """ Return dict of parsed HGMD data

    Returns:
        dict: Dict of gene2transcripts
    """

    data = {}

    f = vcf.Reader(dxpy.DXFile(
        dxid="file-Fv1X0jQ40qBjj4GPKj196kjJ",
        project="project-Fv1Vzkj40qBfq0v38p7q50v8"
    ))

    for line in f:
        gene = line.INFO["GENE"]
        transcript = line.INFO["DNA"].split("%")[0]
        data[gene.upper()] = transcript

    return data


def get_nirvana_data_dict(nirvana_refseq: str):
    """ Return dict of parsed data for Nirvana

    Args:
        nirvana_refseq (str): GFF file for nirvana

    Returns:
        dict: Dict of gene2transcripts2exons
    """

    nirvana_tx_dict = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(None)))
    )

    with gzip.open(nirvana_refseq) as nir_fh:
        for line in nir_fh:
            fields = line.decode("utf-8").strip().split("\t")
            record_type = fields[2]

            info_field = fields[8]
            info_fields = info_field.split("; ")
            info_dict = {}

            if record_type in ["gene", "UTR", "CDS"]:
                continue

            for field in info_fields:
                key, value = field.split(" ")
                value = value.strip("\"")
                value = value.strip("\'")
                info_dict[key] = value

            gff_gene_name = info_dict["gene_name"]
            gff_transcript = info_dict["transcript_id"]

            if record_type == "transcript":
                if "tag" in info_dict:
                    gff_tag = True
                else:
                    gff_tag = False

                if gff_gene_name and gff_transcript:
                    chrom = fields[0].replace("chr", "")
                    start, end = fields[3:5]
                    canonical = gff_tag
                    nirvana_tx_dict[gff_gene_name][gff_transcript]["canonical"] = canonical

            elif record_type == "exon":
                exon_number = info_dict["exon_number"]

                if gff_gene_name and gff_transcript:
                    chrom = fields[0].replace("chr", "")
                    start, end = fields[3:5]
                    nirvana_tx_dict[gff_gene_name][gff_transcript]["exons"][exon_number] = {
                        "chrom": chrom,
                        "start": start,
                        "end": end,
                    }

    return nirvana_tx_dict


def create_panelapp_dict(
    dump_folder: str, hgmd_dict: dict, nirvana_dict: defaultdict
):
    """ Return list of dicts for the data stored in the panelapp dump folder

    Args:
        dump_folder (str): Folder containing the panelapp dump
        hgmd_dict (dict): Dict of HGMD parsed data
        nirvana_dict (defaultdict): Dict of parsed Nirvana GFF data

    Returns:
        list: List of dicts for the data stored in the panelapp dump folder
    """

    panelapp_dict = defaultdict(lambda: defaultdict(set))
    # The following dicts will contain a key called "check" for knowing whether the entity has been seen before
    gene_dict = defaultdict(lambda: defaultdict(None))
    str_dict = defaultdict(lambda: defaultdict(None))
    cnv_dict = defaultdict(lambda: defaultdict(None))
    region_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(None)))

    if os.path.exists(dump_folder) and os.path.isdir(dump_folder):
        for file in os.listdir(dump_folder):
            with open("{}/{}".format(dump_folder, file)) as f:
                for line in f:
                    line = line.strip().split("\t")
                    panel_name, panel_id, version, signedoff, entity_type = line[0:5]
                    panel_dict = panelapp_dict[panel_id]

                    panel_dict["name"] = panel_name
                    panel_dict["version"] = version
                    panel_dict["signedoff"] = signedoff

                    if entity_type == "gene":
                        gene = line[5]

                        transcript2exon, clinical_transcript = assign_transcript(
                            gene, hgmd_dict, nirvana_dict
                        )

                        panel_dict["genes"].add(gene)
                        gene_dict[gene]["check"] = False

                        if transcript2exon and clinical_transcript:
                            gene_dict[gene]["transcripts"] = transcript2exon
                            gene_dict[gene]["clinical"] = clinical_transcript

                            for transcript in transcript2exon:
                                for exon_nb in transcript2exon[transcript]["exons"]:
                                    chrom = transcript2exon[transcript]["exons"][exon_nb]["chrom"]
                                    start = transcript2exon[transcript]["exons"][exon_nb]["start"]
                                    end = transcript2exon[transcript]["exons"][exon_nb]["end"]
                                    region_dict[chrom]["GRCh37"][(start, end)] = False
                        else:
                            gene_dict[gene]["transcripts"] = None
                            gene_dict[gene]["clinical"] = None

                    elif entity_type == "str":
                        (
                            name, gene, seq, nb_normal_repeats,
                            nb_pathogenic_repeats, chrom,
                            grch37_coor, grch38_coor
                        ) = line[5:]

                        panel_dict["strs"].add(name)

                        str_dict[name]["check"] = False
                        str_dict[name]["gene"] = gene
                        str_dict[name]["seq"] = seq
                        str_dict[name]["nb_normal_repeats"] = nb_normal_repeats
                        str_dict[name]["nb_pathogenic_repeats"] = nb_pathogenic_repeats

                        if grch37_coor != "None":
                            start_grch37, end_grch37 = grch37_coor.strip("[]").split(",")
                            start_grch37 = start_grch37.strip()
                            end_grch37 = end_grch37.strip()
                            str_dict[name]["grch37"] = (
                                chrom, start_grch37, end_grch37
                            )
                            region_dict[chrom]["GRCh37"][(start_grch37, end_grch37)] = False
                        else:
                            str_dict[name]["grch37"] = (
                                chrom, None, None
                            )

                        if grch38_coor != "None":
                            start_grch38, end_grch38 = grch38_coor.strip("[]").split(",")
                            start_grch38 = start_grch38.strip()
                            end_grch38 = end_grch38.strip()
                            str_dict[name]["grch38"] = (
                                chrom, start_grch38, end_grch38
                            )
                            region_dict[chrom]["GRCh38"][(start_grch38, end_grch38)] = False
                        else:
                            str_dict[name]["grch38"] = (
                                chrom, None, None
                            )

                    elif entity_type == "cnv":
                        (
                            name, type_variant, chrom,
                            grch37_coor, grch38_coor
                        ) = line[5:]

                        panel_dict["cnvs"].add(name)

                        cnv_dict[name]["check"] = False
                        cnv_dict[name]["type"] = type_variant

                        if grch37_coor != "None":
                            start_grch37, end_grch37 = grch37_coor.strip("[]").split(",")
                            start_grch37 = start_grch37.strip()
                            end_grch37 = end_grch37.strip()
                            cnv_dict[name]["grch37"] = (
                                chrom, start_grch37, end_grch37
                            )
                            region_dict[chrom]["GRCh37"][(start_grch37, end_grch37)] = False
                        else:
                            cnv_dict[name]["grch37"] = (
                                chrom, None, None
                            )

                        if grch38_coor != "None":
                            start_grch38, end_grch38 = grch38_coor.strip("[]").split(",")
                            start_grch38 = start_grch38.strip()
                            end_grch38 = end_grch38.strip()
                            cnv_dict[name]["grch38"] = (
                                chrom, start_grch38, end_grch38
                            )
                            region_dict[chrom]["GRCh38"][(start_grch38, end_grch38)] = False
                        else:
                            cnv_dict[name]["grch38"] = (
                                chrom, None, None
                            )

    return panelapp_dict, gene_dict, str_dict, cnv_dict, region_dict


def check_if_seen_before(value_to_check):
    """ Return if the entity has been seen before

    Args:
        value_to_check: dict["check"]

    Returns:
        bool
    """

    if value_to_check is False:
        return False
    else:
        return True


def parse_tests_xls(file: str):
    """ Parse the data in the National test directory

    Args:
        file (str): XLS of the National test directory

    Returns:
        tuple: Dict of clin_ind_id2clin_ind and dict of test_id2targets
    """

    ci_id2ci = {}
    test_id2targets = defaultdict(lambda: defaultdict(str))

    xls = xlrd.open_workbook(file)
    sheet_with_tests = xls.sheet_by_name("R&ID indications")

    for row in range(sheet_with_tests.nrows):
        if row >= 2:
            (
                ci_id, ci, criteria, test_id,
                targets, method, clinical_group, comment
            ) = sheet_with_tests.row_values(row)

            if ci_id:
                ci_id2ci[ci_id.strip()] = ci.strip()

            if "panel" in method or "WES" in method or "Single gene" in method:
                test_id2targets[test_id.strip()]["targets"] = targets.strip()
                test_id2targets[test_id.strip()]["method"] = method.strip()

    return ci_id2ci, test_id2targets


def clean_targets(test2targets: dict):
    """ Replace the methods from the XLS to abbreviation:
    WES and co -> P
    Panel -> P
    Single Gene -> G

    Args:
        test2targets (dict): Dict of test_id2targets

    Returns:
        dict: Dict of dict for test2targets
    """

    clean_test2targets = defaultdict(lambda: defaultdict(list))

    for test in test2targets:
        targets = test2targets[test]["targets"]
        method = test2targets[test]["method"]

        if "WES" in method:
            clean_test2targets[test]["method"] = "P"

        elif "panel" in method:
            match = regex.search(r"(.*)[pP]anel", method)
            type_panel = match.groups()[0][0]
            clean_test2targets[test]["method"] = f"{type_panel}P"

        elif "gene" in method:
            clean_test2targets[test]["method"] = "G"

        for indiv_target in targets.split(";"):
            indiv_target = indiv_target.strip()

            if "Relevant" not in indiv_target:
                match = regex.search(r"(?P<panel_id>\(\d+\))", indiv_target)

                if match:
                    target_to_add = match.group("panel_id").strip("()")
                    clean_test2targets[test]["panels"].append(target_to_add)
                else:
                    target_to_add = indiv_target.strip()
                    clean_test2targets[test]["genes"].append(target_to_add)

    return clean_test2targets
