import sys
from django.db import models

# sys.path.append("/home/egg-user/panels/panel_config")

import panel_config.config_panel_db as config_panel_db


class ClinicalIndication(models.Model):
    code = models.CharField(max_length=100)
    name = models.CharField(max_length=200)
    gemini_name = models.CharField(max_length=200)

    class Meta:
        db_table = "clinical_indication"
        indexes = [
            models.Index(fields=["name"]),
            models.Index(fields=["gemini_name"]),
        ]


class ClinicalIndicationPanels(models.Model):
    clinical_indication = models.ForeignKey(
        ClinicalIndication, on_delete=models.DO_NOTHING
    )
    panel = models.ForeignKey("Panel", on_delete=models.DO_NOTHING)
    ci_version = models.CharField(max_length=100, default="")

    class Meta:
        db_table = "clinical_indication_panels"
        indexes = [
            models.Index(fields=["clinical_indication", "panel", "ci_version"])
        ]


class Panel(models.Model):
    panelapp_id = models.CharField(max_length=100, blank=True, default="")
    name = models.CharField(max_length=100)
    panel_type = models.ForeignKey("PanelType", on_delete=models.DO_NOTHING)

    class Meta:
        db_table = "panel"
        indexes = [
            models.Index(fields=["name"]),
            models.Index(fields=["panelapp_id"])
        ]


class PanelType(models.Model):
    panel_types = []

    for panel_type in config_panel_db.panel_types:
        panel_types.append((panel_type, panel_type))

    type = models.CharField(max_length=50, choices=panel_types)

    class Meta:
        db_table = "panel_type"


class PanelFeatures(models.Model):
    panel_version = models.CharField(max_length=50)
    description = models.CharField(max_length=1000, blank=True, default="")
    feature = models.ForeignKey("Feature", on_delete=models.DO_NOTHING)
    panel = models.ForeignKey(Panel, on_delete=models.DO_NOTHING)

    class Meta:
        db_table = "panel_features"
        indexes = [
            models.Index(fields=["panel_version", "feature", "panel"])
        ]


class Reference(models.Model):
    name = models.CharField(max_length=100)

    class Meta:
        db_table = "reference"


class Feature(models.Model):
    gene = models.ForeignKey(
        "Gene", on_delete=models.DO_NOTHING, null=True
    )
    feature_type = models.ForeignKey(
        "FeatureType", on_delete=models.DO_NOTHING
    )

    class Meta:
        db_table = "feature"
        indexes = [
            models.Index(fields=["gene"])
        ]


class FeatureType(models.Model):
    feature_choices = []

    for feature_type in config_panel_db.feature_types:
        feature_choices.append((feature_type, feature_type))

    type = models.CharField(max_length=50, choices=feature_choices)

    class Meta:
        db_table = "feature_type"


class Genes2transcripts(models.Model):
    clinical_transcript = models.BooleanField()
    date = models.DateField()
    transcript = models.ForeignKey("Transcript", on_delete=models.DO_NOTHING)
    gene = models.ForeignKey("Gene", on_delete=models.DO_NOTHING)
    reference = models.ForeignKey("Reference", on_delete=models.DO_NOTHING)

    class Meta:
        db_table = "genes2transcripts"
        indexes = [
            models.Index(fields=["transcript", "gene", "reference"])
        ]


class Gene(models.Model):
    hgnc_id = models.CharField(max_length=25)

    class Meta:
        db_table = "gene"


class Transcript(models.Model):
    refseq_base = models.CharField(max_length=50)
    version = models.IntegerField()
    canonical = models.BooleanField()

    class Meta:
        db_table = "transcript"


class hgnc_current(models.Model):
    hgnc_id = models.CharField(max_length=100, unique=True, primary_key=True)
    approved_symbol = models.CharField(max_length=100)
    approved_name = models.TextField()
    status = models.TextField()
    previous_symbols = models.TextField()
    alias_symbols = models.TextField()
    chromosome = models.TextField()
    accession_numbers = models.TextField()
    locus_type = models.TextField()
    locus_group = models.TextField()
    previous_name = models.TextField()
    alias_names = models.TextField()
    date_approved = models.TextField()
    date_modified = models.TextField()
    date_symbol_changed = models.TextField()
    date_name_changed = models.TextField()
    enzyme_ids = models.TextField()
    specialist_database_links = models.TextField()
    specialist_database_ids = models.TextField()
    pubmed_ids = models.TextField()
    gene_group_id = models.TextField()
    gene_group_name = models.TextField()
    ccds_ids = models.TextField()
    locus_specific_databases = models.TextField()
    ext_ncbi_gene_id = models.TextField()
    ext_omim_id = models.TextField()
    ext_refseq = models.TextField()
    ext_uniprot_id = models.TextField()
    ext_ensembl_id = models.TextField()
    ext_vega_id = models.TextField()
    ext_ucsc_id = models.TextField()
    ext_mouse_genome_database_id = models.TextField()
    ext_rat_genome_database_id = models.TextField()
    ext_lncipedia_id = models.TextField()
    ext_gtrnadb_id = models.TextField()
    ext_agr_hgnc_id = models.TextField()

    class Meta:
        db_table = "hgnc_current"
        indexes = [
            models.Index(fields=["hgnc_id"]),
        ]


class hgnc_210129(models.Model):
    hgnc_id = models.CharField(max_length=100, unique=True, primary_key=True)
    approved_symbol = models.CharField(max_length=100)
    approved_name = models.TextField()
    status = models.TextField()
    previous_symbols = models.TextField()
    alias_symbols = models.TextField()
    chromosome = models.TextField()
    accession_numbers = models.TextField()
    locus_type = models.TextField()
    locus_group = models.TextField()
    previous_name = models.TextField()
    alias_names = models.TextField()
    date_approved = models.TextField()
    date_modified = models.TextField()
    date_symbol_changed = models.TextField()
    date_name_changed = models.TextField()
    enzyme_ids = models.TextField()
    specialist_database_links = models.TextField()
    specialist_database_ids = models.TextField()
    pubmed_ids = models.TextField()
    gene_group_id = models.TextField()
    gene_group_name = models.TextField()
    ccds_ids = models.TextField()
    locus_specific_databases = models.TextField()
    ext_ncbi_gene_id = models.TextField()
    ext_omim_id = models.TextField()
    ext_refseq = models.TextField()
    ext_uniprot_id = models.TextField()
    ext_ensembl_id = models.TextField()
    ext_vega_id = models.TextField()
    ext_ucsc_id = models.TextField()
    ext_mouse_genome_database_id = models.TextField()
    ext_rat_genome_database_id = models.TextField()
    ext_lncipedia_id = models.TextField()
    ext_gtrnadb_id = models.TextField()
    ext_agr_hgnc_id = models.TextField()

    class Meta:
        db_table = "hgnc_210129"
        indexes = [
            models.Index(fields=["hgnc_id"]),
        ]
