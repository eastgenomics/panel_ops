import os
import sys

import django

from .logger import setup_logging
from .utils import get_date

sys.path.append('/home/egg-user/panels/panel_palace/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "panel_palace.settings")
django.setup()

from django.core import management
from django.core.management.commands import loaddata
from panel_database.models import (
    Test, Panel, Cnv, Str, Gene, Transcript, Exon, Reference, Region,
    TestPanel, TestGene, PanelStr, PanelGene, PanelCnv, RegionStr, RegionCnv
)


LOGGER = setup_logging("mod_db")


def import_django_fixture(path_to_json):
    LOGGER.info(f"Importing data using json: '{path_to_json}'")

    try:
        management.call_command(loaddata.Command(), path_to_json)
    except Exception as e:
        LOGGER.error("Importing gone wrong")
        LOGGER.debug(f"{e}")
    else:
        return True


def check_attributes_for_obj(obj, **attributes):
    obj = obj.filter(**attributes)

    if obj:
        return True
    else:
        return False


def create_orphans():
    pass


def create_links(panel_data):
    pass


def update_django_tables(data_dicts):
    (
        panelapp_dict, superpanel_dict, gene_dict,
        str_dict, cnv_dict, region_dict
    ) = data_dicts

    for panel_id in panelapp_dict:
        panel_data = panelapp_dict[panel_id]
        panel_genes = panel_data["genes"]
        panel_cnvs = panel_data["cnvs"]
        panel_strs = panel_data["strs"]

        try:
            panel_obj = Panel.objects.filter(name=panel_data["name"])
        except panel_database.models.DoesNotExist as e:
            msg = f"{panel_data['name']} does not exist in the database"
            LOGGER.info(msg)
        else:
            attributes = {
                "version": panel_data["version"],
                "signedoff": panel_data["signedoff"], "panelapp_id": panel_id
            }
            update_needed = check_attributes_for_obj(panel_obj, **attributes)

            if update_needed is True:
                create_orphans()
                create_links()

            for gene in panel_genes:
                try:
                    gene_obj = Gene.objects.filter(symbol=gene)
                except panel_database.models.DoesNotExist as e:
                    LOGGER.info(f"{gene} does not exist in the database")
                else:
                    pass
                    # update_needed = check_attributes_for_obj(
                    #     gene_obj, **attributes
                    # )

            for str_name in panel_strs:
                str_data = str_dict[str_name]

                try:
                    str_obj = Str.objects.filter(name=str_name)
                except panel_database.models.DoesNotExist as e:
                    LOGGER.info(f"{str_name} does not exist in the database")
                else:
                    attributes = {
                        "gene": str_data["gene"],
                        "repeated_sequence": str_data["seq"],
                        "nb_repeats": str_data["nb_normal_repeats"],
                        "nb_pathogenic_repeats": str_data["nb_pathogenic_repeats"]
                    }
                    update_needed = check_attributes_for_obj(
                        str_obj, **attributes
                    )

            for cnv_name in panel_cnvs:
                try:
                    cnv_obj = Cnv.objects.filter(name=cnv_name)
                except panel_database.models.DoesNotExist as e:
                    LOGGER.info(f"{cnv_name} does not exist in the database")
                else:
                    update_needed = check_attributes_for_obj(
                        cnv_obj, **attributes
                    )
