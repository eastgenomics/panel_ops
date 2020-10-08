import os
import sys

import django

sys.path.append('/home/kimy/NHS/Panelapp/panelapp_database/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "panelapp_database.settings")
django.setup()

from panel_database.models import (
    Test, Panel, Cnv, Str, Gene, Transcript, Exon, Reference, Region,
    TestPanel, TestGene, PanelStr, PanelGene, PanelCnv, RegionStr, RegionCnv
)


def update_django_tables(panelapp_dict):
    for panel_name in panelapp_dict:
        gene_dict = panelapp_dict[panel_name]["genes"]
        cnv_dict = panelapp_dict[panel_name]["cnvs"]
        str_dict = panelapp_dict[panel_name]["strs"]

        for gene in gene_dict:
            print(gene)
            gene_object = Gene.objects.filter(symbol=gene)
            print(gene_object)
        break
