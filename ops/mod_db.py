import datetime
import os
import sys

import django

from .config import path_to_panel_palace
from .logger import setup_logging, output_to_loggers
from .utils import get_date, parse_hgnc_dump, parse_g2t

sys.path.append(path_to_panel_palace)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "panel_palace.settings")
django.setup()

from django.core import management
from django.core.management.commands import loaddata
from django.apps import apps
from panel_database.models import Genes2transcripts

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
    output_to_loggers(msg, CONSOLE, MOD_DB)

    try:
        # Call the loaddata cmd --> python manage.py loaddata path_to_json
        management.call_command(loaddata.Command(), path_to_json)
    except Exception as e:
        MOD_DB.error(f"{e}")
        raise e
    else:
        msg = f"Import of data using '{path_to_json}' successful"
        output_to_loggers(msg, CONSOLE, MOD_DB)


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
    output_to_loggers(msg, CONSOLE, MOD_DB)

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
    output_to_loggers(msg, CONSOLE, MOD_DB)


def import_new_g2t(path_to_g2t_file: str):
    """ Import new genes2transcripts file to update tables in the database
    It changes the clinical status and the date attribute of the g2t table

    Args:
        path_to_g2t_file (str): Path to the genes2transcripts file to import
    """

    msg = f"Importing new g2t using: '{path_to_g2t_file}'"
    output_to_loggers(msg, CONSOLE, MOD_DB)

    date = datetime.date.today()

    g2t_data = parse_g2t(path_to_g2t_file)
    g2t_rows = Genes2transcripts.objects.all()

    for gene in g2t_data:
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
                        f"'{row.values('id').get()}' - Clinical status goes "
                        f"from {clinical_transcript} to {clinical}, updating "
                        "date as well"
                    )
                    output_to_loggers(msg, CONSOLE, MOD_DB)
                    row.update(clinical_transcript=clinical, date=date)
            else:
                msg = (
                    f"Couldn't find object using '{gene}', '{transcript}', "
                    f"clinical_status '{clinical}', canonical_status "
                    f"'{canonical}'"
                )
                output_to_loggers(msg, CONSOLE, MOD_DB)

    msg = (
        f"Finished importing new g2t data using: '{path_to_g2t_file}'"
    )
    output_to_loggers(msg, CONSOLE, MOD_DB)
