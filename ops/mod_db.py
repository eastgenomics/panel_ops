import datetime
import os
import sys

import django
from packaging import version

from .config import path_to_panel_palace
from .logger import setup_logging, output_to_loggers
from .utils import parse_hgnc_dump, parse_g2t, parse_bespoke_panel_form

sys.path.append(path_to_panel_palace)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "panel_palace.settings")
django.setup()

from django.core import management
from django.core.management.commands import loaddata
from django.apps import apps
from panel_database.models import (
    Genes2transcripts, Gene, Transcript, Feature, ClinicalIndication,
    ClinicalIndicationPanels, Panel, PanelFeatures, PanelType, FeatureType
)

CONSOLE, MOD_DB = setup_logging("mod_db")


def import_django_fixture(path_to_json: str):
    """ Import data to django database using a django fixture (json with
    specific format)

    Args:
        path_to_json (str): Path to django fixture

    Raise:
        Exception: If there's any issue with the import
    """

    msg = f"Importing data using json: '{path_to_json}'"
    output_to_loggers(msg, "info", CONSOLE, MOD_DB)

    try:
        # Call the loaddata cmd --> python manage.py loaddata path_to_json
        management.call_command(loaddata.Command(), path_to_json)
    except Exception as e:
        MOD_DB.error(f"{e}")
        raise e
    else:
        msg = f"Import of data using '{path_to_json}' successful"
        output_to_loggers(msg, "info", CONSOLE, MOD_DB)


def import_hgnc_dump(path_to_hgnc_dump: str, date: str):
    """ Import hgnc data in the current hgnc table and the new hgnc table

    Args:
        path_to_hgnc_dump (str): Path to the hgnc dump
        date (str): Date to look for the appropriate hgnc table
    """

    msg = (
        f"Importing data using: '{path_to_hgnc_dump}' and looking for "
        f"table using {date} --> hgnc_{date}"
    )
    output_to_loggers(msg, "info", CONSOLE, MOD_DB)

    # Parse the hgnc data dump
    hgnc_data = parse_hgnc_dump(path_to_hgnc_dump)

    # Get the hgnc model table using the date
    hgnc_new = apps.get_model(
        app_label="panel_database", model_name=f"hgnc_{date}"
    )
    # Get the hgnc current table
    hgnc_current = apps.get_model(
        app_label="panel_database", model_name="hgnc_current"
    )

    all_current_entries = hgnc_current.objects.all()

    # Check if there's data in the hgnc current table
    if all_current_entries:
        # Delete everything
        hgnc_current.objects.all().delete()

    # Loop through the 2 tables, need to import the same data twice
    for model in [hgnc_current, hgnc_new]:
        # Loop through the data in the hgnc dump
        for hgnc_id in hgnc_data:
            # Add the hgnc_id in the hgnc data
            data = dict({"hgnc_id": hgnc_id}, **hgnc_data[hgnc_id])
            # Create the object with all the data from the dump
            obj = model(**data)
            # So there's this method called bulk_create() and I tried using
            # that but I kept getting this following error
            # django.db.utils.OperationalError: (2006, 'MySQL server has gone away')
            # So yeah, looping and saving, might be slower but works
            obj.save()

    msg = (
        f"Finished importing data using: '{path_to_hgnc_dump}' in "
        f"hgnc_current and  hgnc_{date}"
    )
    output_to_loggers(msg, "info", CONSOLE, MOD_DB)


def import_new_g2t(path_to_g2t_file: str):
    """ Import new genes2transcripts file to update tables in the database
    It changes the clinical status and the date attribute of the g2t table

    Args:
        path_to_g2t_file (str): Path to the genes2transcripts file to import
    """

    msg = f"Importing new g2t using: '{path_to_g2t_file}'"
    output_to_loggers(msg, "info", CONSOLE, MOD_DB)

    date = datetime.date.today()

    g2t_data = parse_g2t(path_to_g2t_file)
    g2t_rows = Genes2transcripts.objects.all()

    for gene in g2t_data:
        new_gene, gene_created = Gene.objects.get_or_create(
            hgnc_id=gene
        )
        new_feature, feature_created = Feature.objects.get_or_create(
            gene_id=new_gene.id, feature_type_id=1
        )

        if gene_created:
            msg = (
                f"Created gene and feature for {gene}: {new_gene}, "
                f"{new_feature}"
            )
            output_to_loggers(msg, "info", CONSOLE, MOD_DB)

        for transcript, statuses in g2t_data[gene].items():
            refseq, version = transcript.split(".")
            clinical, canonical = statuses

            filter_dict = {
                "gene__hgnc_id": gene,
                "transcript__refseq_base": refseq,
                "transcript__version": version,
                "transcript__canonical": canonical
            }

            row = g2t_rows.filter(**filter_dict)

            if row:
                clinical_transcript = row.values_list(
                    "clinical_transcript", flat=True
                ).get()

                if clinical_transcript != clinical:
                    msg = (
                        "Updating genes2transcripts row "
                        f"'{row.values('id').get()}' - Clinical status "
                        f"{clinical_transcript} --> {clinical}, updating "
                        "date as well"
                    )
                    output_to_loggers(msg, "info", CONSOLE, MOD_DB)
                    row.update(clinical_transcript=clinical, date=date)
            else:
                new_tx, tx_created = Transcript.objects.get_or_create(
                    refseq_base=refseq, version=version, canonical=canonical
                )
                new_g2t, g2t_created = Genes2transcripts.objects.get_or_create(
                    gene_id=new_gene.id, reference_id=1, date=date,
                    transcript_id=new_tx.id, clinical_transcript=clinical
                )

                if (tx_created and not g2t_created) or (not tx_created and g2t_created):
                    msg = (
                        "One of the following row already existed: "
                        f"{new_tx} {tx_created} | "
                        f"{new_g2t} {g2t_created}."
                        "Please check that there is no underlying issues."
                    )
                    output_to_loggers(msg, "warning", CONSOLE, MOD_DB)
                elif tx_created and g2t_created:
                    msg = (
                        f"The following objects have been created: {new_tx}, "
                        f"{new_g2t}"
                    )
                    output_to_loggers(msg, "info", CONSOLE, MOD_DB)

    msg = (
        f"Finished importing new g2t data using: '{path_to_g2t_file}'"
    )
    output_to_loggers(msg, "info", CONSOLE, MOD_DB)


def import_bespoke_panel(panel_form: str):
    """ Import bespoke panel in the database

    Args:
        panel_form (str): Excel file containing data for the new panel form

    Raises:
        Exception: When the data to be imported is already present in the
        database
    """

    # parse bespoke panel form
    data = parse_bespoke_panel_form(panel_form)

    msg = f"Checking {panel_form} data before importing it"
    output_to_loggers(msg, "info", CONSOLE, MOD_DB)

    # check if the clinical indication data doesn't already exist in the
    # database
    data_in_database = check_if_ci_data_in_database(data)

    if data_in_database:
        msg = (
            "Please check that the data to be imported is not already in the "
            "database"
        )
        raise Exception(msg)

    msg = f"Importing {panel_form} into panel palace"
    output_to_loggers(msg, "info", CONSOLE, MOD_DB)

    for ci in data:
        ci_data = data[ci]

        # assign "C code" to bespoke clinical indication
        ci_id = assign_CUH_code(ci)
        # assemble the gemini name from CUH code and the ci name
        gemini_name = f"{ci_id}_{ci}"

        for panel in ci_data["panels"]:
            panel_data = ci_data["panels"][panel]

            # get the panel type matching the in-house type
            bespoke_panel_type_id = PanelType.objects.get(type="in-house").id
            # create panel
            new_panel, panel_created = Panel.objects.get_or_create(
                name=panel, panel_type_id=bespoke_panel_type_id
            )

            if panel_created:
                msg = f"Panel {new_panel.name} created: {new_panel.id}"
            else:
                msg = f"Panel {new_panel.name} already exists: {new_panel.id}"

            output_to_loggers(msg, "info", CONSOLE, MOD_DB)

            for gene in panel_data["genes"]:
                # create gene
                new_gene, gene_created = Gene.objects.get_or_create(
                    hgnc_id=gene
                )

                if gene_created:
                    msg = f"Gene {new_gene.hgnc_id} created: {new_gene.id}"
                    output_to_loggers(msg, "info", CONSOLE, MOD_DB)

                # get the gene feature type id
                gene_feature_type_id = FeatureType.objects.get(type="gene").id

                # create feature
                new_feature, feature_created = Feature.objects.get_or_create(
                    gene_id=new_gene.id, feature_type_id=gene_feature_type_id
                )

                if feature_created:
                    msg = (
                        f"Feature for gene {new_feature.gene_id} created: "
                        f"{new_feature.id}"
                    )
                    output_to_loggers(msg, "info", CONSOLE, MOD_DB)

                # create panel feature link
                panel_feature_link = PanelFeatures.objects.get_or_create(
                    panel_version=panel_data["version"],
                    feature_id=new_feature.id, panel_id=new_panel.id
                )

            # create clinical indication
            new_ci, ci_created = ClinicalIndication.objects.get_or_create(
                name=ci, gemini_name=gemini_name, clinical_indication_id=ci_id
            )

            if ci_created:
                msg = f"Clinical indication {new_ci.name} created: {new_ci.id}"
                output_to_loggers(msg, "info", CONSOLE, MOD_DB)

            # create clinical indication panel link
            ci_panel_link = ClinicalIndicationPanels.objects.get_or_create(
                clinical_indication_id=new_ci.id, panel_id=new_panel.id,
                ci_version=ci_data["version"]
            )

    msg = f"Finished importing {panel_form}"
    output_to_loggers(msg, "info", CONSOLE, MOD_DB)


def assign_CUH_code(clinical_indication: str):
    """ Assign new CUH code to clinical indication

    Args:
        clinical_indication (str): Clinical indication name

    Returns:
        str: CUH code for the clinical indication
    """

    # check if the clinical indication already exists
    ci_ids = ClinicalIndication.objects.filter(
        clinical_indication_id__startswith="C", name=clinical_indication
    ).values_list("clinical_indication_id")

    # if it exists, give a decimal point higher
    if ci_ids:
        latest_CUH_code = max(
            [version.parse(code[0].split("C")[1]) for code in ci_ids]
        )
        new_CUH_code = f"C{latest_CUH_code.major}.{latest_CUH_code.minor+1}"

    # if not, create new int/decimal number
    else:
        # get the latest C code
        all_C_codes = ClinicalIndication.objects.filter(
            clinical_indication_id__startswith="C"
        ).values_list("clinical_indication_id", flat=True)

        # if C codes already exists in the database:
        if all_C_codes:
            latest_CUH_code = max(
                [version.parse(code.split("C")[1]) for code in all_C_codes]
            )
            new_CUH_code = f"C{latest_CUH_code.major+1}.1"
        else:
            new_CUH_code = "C1.1"

    return new_CUH_code


def check_if_ci_data_in_database(data: dict):
    """ Check if the clinical indication and subsequent panels and genes
    already exists

    Args:
        data (dict): Dict of dicts containing the data for clinical indication,
        its panels and genes

    Returns:
        bool: Whether the check is positive or negative
    """

    for ci in data:
        ci_data = data[ci]

        for panel in ci_data["panels"]:
            panel_data = ci_data["panels"][panel]

            # get panel ids that match new panel name and version
            panels_check = PanelFeatures.objects.filter(
                panel__name=panel, panel_version=panel_data["version"]
            ).values_list("panel_id", flat=True)

            # get clinical indications that match new ci and version + panel_id
            # that way we get clinical indication + panel link
            ci_panels_check = ClinicalIndicationPanels.objects.filter(
                clinical_indication__name=ci,
                ci_version=ci_data["version"],
                panel_id__in=panels_check
            )

            # panel to be imported exists and a link from ci to panel exists
            if (panels_check and ci_panels_check):
                msg = (
                    f"Combination of '{ci}' {ci_data['version']} with panel "
                    f"'{panel}' {panel_data['version']} already exists"
                )
                output_to_loggers(msg, "warning", CONSOLE, MOD_DB)
                return True

    return False
