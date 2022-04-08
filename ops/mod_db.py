import datetime
import os
import sys

import django
from packaging import version

from .config import path_to_panel_palace
from .logger import setup_logging, output_to_loggers
from .utils import (
    parse_hgnc_dump, parse_g2t, parse_panel_form, get_latest_panel_version
)

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


def import_panel_form_data(panel_form: str):
    """ Import panel in the database

    Args:
        panel_form (str): Excel file containing data for the new panel form

    Raises:
        Exception: When the data to be imported is already present in the
        database
    """

    # parse panel form
    data, add_on = parse_panel_form(panel_form)

    msg = f"Checking {panel_form} data before importing it"
    output_to_loggers(msg, "info", CONSOLE, MOD_DB)

    # check if the clinical indication data doesn't already exist in the
    # database
    data_in_database, log_data = check_if_ci_data_in_database(data)

    if data_in_database:
        ci_id, ci_version, panel_id, panel_version, features = log_data
        msg = (
            "The clinical indication associated to this set of genes already "
            f"exists: ci_id {ci_id} - {ci_version} -> panel_id {panel_id} - "
            f"{panel_version} --> feature_id(s) {features}"
        )
        raise Exception(msg)

    msg = f"Importing {panel_form} into panel palace"
    output_to_loggers(msg, "info", CONSOLE, MOD_DB)

    # bool to indicate if single gene panels are involved
    single_gene_panel = False

    for ci in data:
        ci_data = data[ci]

        # if it's an add on panel, get the ci object to reuse its attributes
        if add_on:
            existing_ci = ClinicalIndication.objects.get(
                gemini_name__contains=ci_data["add_on"]
            )
        else:
            # assign "C code" to bespoke clinical indication
            ci_id = assign_CUH_code(ci)
            # assemble the gemini name from CUH code and the ci name
            gemini_name = f"{ci_id}_{ci}"

        for panel in ci_data["panels"]:
            panel_data = ci_data["panels"][panel]

            # get the panel type matching the in-house type
            in_house_panel_type_id = PanelType.objects.get(type="in-house").id

            if add_on:
                # get the existing panels associated to the existing clinical
                # indication
                existing_panel_ids = set(Panel.objects.filter(
                    clinicalindicationpanels__clinical_indication_id=existing_ci.id
                ).values_list("id", flat=True))

                if len(existing_panel_ids) == 1:
                    existing_panel_id = list(existing_panel_ids)[0]

                    # get the latest version of a panel to increment it when
                    # creating panel feature links
                    panel_versions = PanelFeatures.objects.filter(
                        panel_id=existing_panel_id
                    ).values_list("panel_version", flat=True)

                    latest_version = get_latest_panel_version(panel_versions)
                else:
                    # go through the ids to see if only single gene panels are
                    # associated
                    for existing_panel_id in existing_panel_ids:
                        # this filtering works as a check because single gene
                        # panels are untangible
                        panel = PanelFeatures.objects.filter(
                            panel_id=existing_panel_id
                        )

                        # looking through the panels associated with the
                        # clinical indication, check if one of them is a "real"
                        # panel because we don't want to modify a single gene
                        # panel
                        if len(panel) > 1:
                            raise Exception((
                                "2 not single gene panels are linked to the "
                                "same clinical indication. Please note that "
                                "add on form for clinical indications that "
                                "are linked to multiple non single genepanels"
                            ))

                    single_gene_panel = True
                    sg_panel_type_id = PanelType.objects.get(
                        type="single_gene"
                    ).id
            else:
                # create panel
                new_panel, panel_created = Panel.objects.get_or_create(
                    name=panel, panel_type_id=in_house_panel_type_id
                )

                if panel_created:
                    msg = f"Panel {new_panel.name} created: {new_panel.id}"
                else:
                    msg = f"Panel {new_panel.name} to get updated"

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

                if add_on:
                    if single_gene_panel:
                        # this should return only one result because we use the
                        # feature id and the HGNC pattern in the naming of
                        # the panel which is only used for single gene panels
                        candidate_panel_ids = PanelFeatures.objects.filter(
                            feature_id=new_feature,
                            panel__name__contains="HGNC"
                        ).values_list(
                            "panel_id", flat=True
                        )

                        # multiple single gene panels linked to the same gene?
                        if len(candidate_panel_ids) > 1:
                            raise Exception((
                                f"Check {candidate_panel_ids} for single gene"
                                "panels weirdness"
                            ))
                        elif len(candidate_panel_ids) == 1:
                            # need to link existing panel
                            sg_panel, sg_panel_created = Panel.objects.get_or_create(
                                name=f"{gene}_SG_panel",
                                panel_type_id=sg_panel_type_id
                            )
                        else:
                            # create a panel
                            sg_panel, sg_panel_created = Panel.objects.get_or_create(
                                name=f"{gene}_SG_panel",
                                panel_type_id=sg_panel_type_id
                            )
                            # create the link between panel and feature
                            panel_feature_link = PanelFeatures.objects.get_or_create(
                                panel_version="1.0.0",
                                feature_id=new_feature.id, panel_id=sg_panel.id
                            )

                        # create link between ci and single gene panel if the
                        # panel was either found or created
                        ci_panel_link = ClinicalIndicationPanels.objects.get_or_create(
                            clinical_indication_id=existing_ci.id,
                            panel_id=sg_panel.id, ci_version=ci_data["version"]
                        )

                    else:
                        # increment latest version appropriately and switch to
                        # a string for import in db
                        if latest_version[1] == "":
                            version_to_import = f"{latest_version[0]}|1"
                        else:
                            version_to_import = (
                                f"{latest_version[0]}|"
                                f"{int(latest_version[1]) + 1}"
                            )

                        panel_feature_link = PanelFeatures.objects.get_or_create(
                            panel_version=version_to_import,
                            feature_id=new_feature.id, panel_id=existing_panel_id
                        )

                else:
                    # create panel feature link
                    panel_feature_link = PanelFeatures.objects.get_or_create(
                        panel_version=panel_data["version"],
                        feature_id=new_feature.id, panel_id=new_panel.id
                    )

            if add_on:
                if single_gene_panel is False:
                    # just create links from the clinical indication to the
                    # existing panel and the add on panel
                    ci_panel_link = ClinicalIndicationPanels.objects.get_or_create(
                        clinical_indication_id=existing_ci.id,
                        panel_id=existing_panel_id, ci_version=ci_data["version"]
                    )
            else:
                # create clinical indication
                new_ci, ci_created = ClinicalIndication.objects.get_or_create(
                    name=ci, gemini_name=gemini_name, code=ci_id
                )

                # create clinical indication panel link
                ci_panel_link = ClinicalIndicationPanels.objects.get_or_create(
                    clinical_indication_id=new_ci.id, panel_id=new_panel.id,
                    ci_version=ci_data["version"]
                )

                if ci_created:
                    msg = (
                        f"Clinical indication {new_ci.name} created: "
                        f"{new_ci.id}"
                    )
                    output_to_loggers(msg, "info", CONSOLE, MOD_DB)

                # get panel id already linked to clinical indication
                existing_panels = Panel.objects.filter(
                    clinicalindicationpanels__clinical_indication_id=ci_object.id
                ).all()

                # gather genes from existing panels
                existing_panel_features = PanelFeatures.objects.filter(
                    panel_id__in=existing_panels
                ).all()

                for existing_panel_feature in existing_panel_features:
                    panel_feature_link = PanelFeatures.objects.get_or_create(
                        panel_version=panel_data["version"],
                        feature_id=existing_panel_feature.feature_id,
                        panel_id=new_panel.id
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
        code__startswith="C", name=clinical_indication
    ).values_list("code")

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
            code__startswith="C"
        ).values_list("code", flat=True)

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

        features_from_form = set()
        fields_to_filter_with = {
            "clinicalindicationpanels__ci_version": ci_data["version"]
        }

        if ci_data["add_on"]:
            # use R code used in form to find clinical indication
            fields_to_filter_with["code"] = ci_data["add_on"]
        else:
            # use name of bespoke clinical indication to find it
            fields_to_filter_with["name"] = ci

        ci_obj_id = set(
            ClinicalIndication.objects.filter(
                **fields_to_filter_with
            ).values_list("id", flat=True)
        )

        if ci_obj_id:
            ci_obj_id = list(ci_obj_id)[0]
            # gather genes from latest panel version
            panel_ids = Panel.objects.filter(
                clinicalindicationpanels__clinical_indication_id=ci_obj_id
            ).distinct().values_list("id", flat=True)

            # gather provided genes
            for panel in ci_data["panels"]:
                panel_data = ci_data["panels"][panel]

                for gene in panel_data["genes"]:
                    # get the feature id for the genes
                    feature_obj = Feature.objects.get(gene__hgnc_id=gene)
                    features_from_form.add(feature_obj.id)

            versions_of_panel = PanelFeatures.objects.filter(
                panel_id__in=panel_ids
            ).values_list("panel_version", flat=True)

            for version in versions_of_panel:
                features_from_database = set(
                    PanelFeatures.objects.filter(
                        panel_id__in=panel_ids, panel_version=version
                    ).values_list("feature_id", flat=True)
                )

                # compare features gathered
                if features_from_database == features_from_form:
                    return True, (
                        ci_obj_id, ci_data["version"],
                        ";".join([str(panel_id) for panel_id in panel_ids]),
                        version, features_from_form
                    )

    return False, ()
