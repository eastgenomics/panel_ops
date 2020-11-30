import os
import sys

import django

from .logger import setup_logging

sys.path.append('/home/egg-user/panels/panel_palace/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "panel_palace.settings")
django.setup()

from django.core import management
from django.core.management.commands import loaddata


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
