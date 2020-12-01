import os
import sys

import django

from .logger import setup_logging

sys.path.append('/home/kimy/NHS/Panelapp/panel_palace/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "panel_palace.settings")
django.setup()

from django.core import management
from django.core.management.commands import loaddata
from django.forms.models import model_to_dict
from panel_database.models import (
    Test, Panel, Cnv, Str, Gene, Transcript, Exon, Reference, Region,
    TestPanel, TestGene, PanelStr, PanelGene, PanelCnv, RegionStr, RegionCnv
)


LOGGER = setup_logging("mod_db")


def import_django_fixture(path_to_json):
    """ Import data to django database using a django fixture (json with specific format)

    Args:
        path_to_json (str): Path to django fixture

    Returns:
        bool: True if import works
    """

    LOGGER.info(f"Importing data using json: '{path_to_json}'")

    try:
        management.call_command(loaddata.Command(), path_to_json)
    except Exception as e:
        LOGGER.error("Importing gone wrong")
        LOGGER.debug(f"{e}")
        return False
    else:
        return True


def check_attributes_for_obj(obj, **attributes):
    model_dict = model_to_dict(obj, fields=attributes.keys())

    if model_dict != attributes:
        return True
    else:
        return False


def update_panel_attributes(panel_obj, panelapp_id, panel_data):
    msg = [f"{panelapp_id}"]

    if panel_obj.version != panel_data["version"]:
        msg.append((
            f"Version changed: db = '{panel_obj.version}'"
            f"!= dump = '{panel_data['version']}'"
        ))
        panel_obj.version = panel_data["version"]

    if panel_obj.name != panel_data["name"]:
        msg.append((
            f"Name changed: db = '{panel_obj.name}'"
            f"!= dump = '{panel_data['name']}'"
        ))
        panel_obj.name = panel_data["name"]

    if panel_obj.signedoff != panel_data["signedoff"]:
        msg.append((
            f"signedoff changed: db = '{panel_obj.signedoff}'"
            f"!= dump = '{panel_data['signedoff']}'"
        ))
        panel_obj.signedoff = panel_data["signedoff"]

    return msg, panel_obj


def update_str_attributes(str_obj, str_name, str_data):
    msg = [f"{str_name}"]

    if str_obj.gene != str_data["gene"]:
        msg.append((
            f"gene changed: db = '{str_obj.gene}'"
            f"!= dump = '{str_data['gene']}'"
        ))
        str_obj.version = str_data["gene"]

    if str_obj.repeated_sequence != str_data["seq"]:
        msg.append((
            f"repeated_sequence changed: db = '{str_obj.repeated_sequence}'"
            f"!= dump = '{str_data['seq']}'"
        ))
        str_obj.repeated_sequence = str_data["seq"]

    if str_obj.nb_repeats != str_data["nb_normal_repeats"]:
        msg.append((
            f"nb_repeats changed: db = '{str_obj.nb_repeats}'"
            f"!= dump = '{str_data['nb_normal_repeats']}'"
        ))
        str_obj.nb_repeats = str_data["nb_normal_repeats"]

    if str_obj.nb_pathogenic_repeats != str_data["nb_pathogenic_repeats"]:
        msg.append((
            f"nb_pathogenic_repeats changed: db = '{str_obj.nb_pathogenic_repeats}'"
            f"!= dump = '{str_data['nb_pathogenic_repeats']}'"
        ))
        str_obj.nb_pathogenic_repeats = str_data["nb_pathogenic_repeats"]

    return msg, str_obj


def update_cnv_attributes(cnv_obj, cnv_name, cnv_data):
    msg = [f"{cnv_name}"]

    if cnv_obj.variant_type != cnv_data["variant_type"]:
        msg.append((
            f"variant_type changed: db = '{cnv_obj.variant_type}'"
            f"!= dump = '{cnv_data['type']}'"
        ))
        cnv_obj.variant_type = cnv_data["variant_type"]

    return msg, cnv_obj


def update_django_tables(data_dicts):
    (
        panelapp_dict, superpanel_dict, gene_dict,
        str_dict, cnv_dict, region_dict
    ) = data_dicts

    pk_dict = {}

    LOGGER.info("Updating panels")

    for superpanel in superpanel_dict:
        pass

    for panel_id in panelapp_dict:
        panel_data = panelapp_dict[panel_id]
        panel_genes = panel_data["genes"]
        panel_cnvs = panel_data["cnvs"]
        panel_strs = panel_data["strs"]

        try:
            panel_obj = Panel.objects.get(panelapp_id=panel_id)
        except panel_database.models.DoesNotExist as e:
            # Panel to be created
            msg = f"Panelapp id '{panel_id}' does not exist in the database"
            LOGGER.info(msg)
            latest_panel_pk = Panel.objects.latest("id").values_list(
                "id", flat=True
            )
            pk_dict["panel"] = latest_panel_pk
        else:
            # check if the attributes from the panelapp dump are
            # the same as the ones stored in the database
            panel_attributes = {
                "version": panel_data["version"], "name": panel_data["name"],
                "signedoff": panel_data["signedoff"]
            }
            update_needed = check_attributes_for_obj(
                panel_obj, **panel_attributes
            )

            if update_needed is True:
                # Panel to be updated
                msg = f"Panelapp panel {panel_id} needs modification"
                LOGGER.info(msg)
                msg, panel_obj = update_panel_attributes(
                    panel_obj, panel_id, panel_data
                )
                LOGGER.info(" | ".join(msg))
                # panel_obj.save()

            # check if the panel is linked to the correct gene, str, cnv
            panelgene_queryset = PanelGene.objects.select_related(
                "gene"
            ).filter(panel_id=panel_obj.pk)

            gene_symbols = [
                queryset.gene.symbol for queryset in panelgene_queryset
            ]

            diff = set(gene_symbols).symmetric_difference(panel_genes)
            if diff:
                LOGGER.info(f"{panel_data['name']}: Genes linked are not identical")
                LOGGER.debug(f"{gene_symbols}")
                LOGGER.debug(f"{panel_genes}")

            return
            for gene in panel_genes:
                gene_data = gene_dict[gene]

                try:
                    gene_obj = Gene.objects.get(symbol=gene)
                except panel_database.models.DoesNotExist as e:
                    # Gene to be created
                    LOGGER.info(f"{gene} does not exist in the database")
                    latest_gene_pk = Gene.objects.latest("id").values_list(
                        "id", flat=True
                    )
                else:
                    # Check transcripts
                    transcript, version = gene_data["clinical"].split(".")

                    try:
                        tx_obj = Transcript.objects.get(
                            refseq=transcript, version=version,
                            gene_id=gene_obj.id
                        )
                    except panel_database.models.DoesNotExist as e:
                        # Transcript to be created
                        msg = (
                            f"{transcript}.{version} doesn't exist "
                            "in the database"
                        )
                        LOGGER.info(msg)
                        latest_tx_pk = Transcript.objects.latest(
                            "id"
                        ).values_list(
                            "id", flat=True
                        )

                    else:
                        # Check the link between gene and transcript
                        gene_pk = gene_obj
                        tx_pk = tx_obj


            for str_name in panel_strs:
                str_data = str_dict[str_name]

                try:
                    str_obj = Str.objects.filter(name=str_name)
                except panel_database.models.DoesNotExist as e:
                    LOGGER.info(f"{str_name} does not exist in the database")
                    latest_str_pk = Str.objects.latest("id").values_list(
                        "id", flat=True
                    )
                else:
                    str_attributes = {
                        "gene": str_data["gene"],
                        "repeated_sequence": str_data["seq"],
                        "nb_repeats": str_data["nb_normal_repeats"],
                        "nb_pathogenic_repeats": str_data["nb_pathogenic_repeats"]
                    }
                    update_needed = check_attributes_for_obj(
                        str_obj, **str_attributes
                    )

                    if update_needed is True:
                        # Str to be updated
                        msg = f"Str {str_name} needs modification"
                        LOGGER.info(msg)
                        msg, str_obj = update_str_attributes(
                            str_obj, str_name, str_data
                        )
                        LOGGER.info(" | ".join(msg))
                        # str_obj.save()

            for cnv_name in panel_cnvs:
                cnv_data = cnv_dict[cnv_name]

                try:
                    cnv_obj = Cnv.objects.filter(name=cnv_name)
                except panel_database.models.DoesNotExist as e:
                    LOGGER.info(f"{cnv_name} does not exist in the database")
                    latest_cnv_pk = Cnv.objects.latest("id").values_list(
                        "id", flat=True
                    )
                else:
                    cnv_attributes = {
                        "variant_type": cnv_data["type"],
                    }

                    update_needed = check_attributes_for_obj(
                        cnv_obj, **cnv_attributes
                    )

                    if update_needed is True:
                        # Cnv to be updated
                        msg = f"Cnv {cnv_name} needs modification"
                        LOGGER.info(msg)
                        msg, cnv_obj = update_cnv_attributes(
                            cnv_obj, cnv_name, cnv_data
                        )
                        LOGGER.info(" | ".join(msg))
                        # cnv_obj.save()
