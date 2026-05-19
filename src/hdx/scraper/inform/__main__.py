#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from hdx.scraper.inform._version import __version__
from hdx.scraper.inform.pipeline import Pipeline

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-inform"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: Inform"


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    logger.info(f"##### {_LOOKUP} version {__version__} ####")
    configuration = Configuration.read()
    User.check_current_user_write_access("e116c55a-d536-4b47-9308-94b1c7457afe")

    with wheretostart_tempdir_batch(folder=_LOOKUP) as info:
        tempdir = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=tempdir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=tempdir,
                save=save,
                use_saved=use_saved,
            )
            pipeline = Pipeline(configuration, retriever, tempdir)
            #
            # Steps to generate dataset
            #
            latest_data = pipeline.get_data("latest_url", "ValidityYear")
            trends_data = pipeline.get_data("trends_url", "GNAYear")

            dataset = Dataset.read_from_hdx(configuration["dataset_name"])
            if not dataset:
                logger.error(
                    f"Dataset {configuration['dataset_name']} not found on HDX"
                )
                return
            for resource in list(dataset.get_resources()):
                if not resource.get("description"):
                    logger.warning(
                        f"Deleting resource with missing description: {resource['name']}"
                    )
                    dataset.delete_resource(resource)
            pipeline.generate_dataset(latest_data, trends_data, dataset)
            dataset.create_in_hdx(
                remove_additional_resources=False,
                match_resource_order=True,
                updated_by_script=_UPDATED_BY_SCRIPT,
                batch=info["batch"],
            )


if __name__ == "__main__":
    facade(
        main,
        # hdx_site="demo",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
