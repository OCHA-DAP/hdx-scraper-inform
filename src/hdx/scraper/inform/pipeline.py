#!/usr/bin/python
"""Inform scraper"""

import logging
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir

    def get_data(self, url_id, year_col) -> dict:
        data_url = self._configuration[url_id]
        data = self._retriever.download_json(data_url)

        df = pd.DataFrame(data)
        df = df[df[year_col] != 0].dropna(subset=[year_col])
        df["CountryName"] = df["Iso3"].apply(
            lambda iso3: Country.get_country_name_from_iso3(iso3)
        )
        df_sorted = df.sort_values(by=["Iso3", year_col], ascending=[True, False])

        if url_id == "latest_url":
            new_order = [
                "CountryName",
                "Iso3",
                "ValidityYear",
                "IndicatorName",
                "IndicatorScore",
                "Unit",
            ]
            df_sorted = df_sorted[new_order]
        else:
            new_order = [
                "CountryName",
                "Iso3",
                "GNAYear",
                "IndicatorId",
                "FullName",
                "IndicatorScore",
            ]
            df_sorted = df_sorted[new_order]

        return df_sorted

    def generate_dataset(self, latest_df, trends_df) -> Optional[Dataset]:
        dataset_title = "INFORM Risk Index"
        dataset_name = slugify(dataset_title)

        # Get date range
        min_date, max_date = self.get_date_range(latest_df, trends_df)

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.set_time_period(min_date, max_date)
        dataset.add_tags(self._configuration["tags"])
        dataset.add_other_location("world")

        # Latest resource
        latest_resource_name = (
            f"{slugify(self._configuration['latest_name'], separator='_')}.csv"
        )
        latest_resource_data = {
            "name": latest_resource_name,
            "description": self._configuration["latest_description"],
        }

        dataset.generate_resource(
            folder=self._tempdir,
            filename=latest_resource_name,
            rows=latest_df.to_dict(orient="records"),
            resourcedata=latest_resource_data,
            headers=list(latest_df.columns),
        )

        # Trends resource
        trends_resource_name = (
            f"{slugify(self._configuration['trends_name'], separator='_')}.csv"
        )
        trends_resource_data = {
            "name": trends_resource_name,
            "description": self._configuration["trends_description"],
        }

        dataset.generate_resource(
            folder=self._tempdir,
            filename=trends_resource_name,
            rows=trends_df.to_dict(orient="records"),
            resourcedata=trends_resource_data,
            headers=list(trends_df.columns),
        )

        # Code book resource
        codebook_resource_name = "inform_codebook.pdf"
        codebook_resource_data = {
            "name": codebook_resource_name,
            "description": "INFORM Concept and Methodology report",
            "url": "https://drmkc.jrc.ec.europa.eu/inform-index/Portals/0/InfoRM/INFORM Concept and Methodology Version 2017 Pdf FINAL.pdf",
            "format": "PDF",
        }
        codebook_resource = Resource(
            {
                "name": codebook_resource_name,
                "description": "INFORM Concept and Methodology report",
            }
        )
        codebook_path = "https://drmkc.jrc.ec.europa.eu/inform-index/Portals/0/InfoRM/INFORM Concept and Methodology Version 2017 Pdf FINAL.pdf"
        codebook_resource.set_format("pdf")
        codebook_resource.set_file_to_upload(codebook_path)
        dataset.add_update_resources([codebook_resource_data])

        return dataset

    def get_date_range(self, latest_df, trends_df) -> Tuple:
        """
        Returns min and max date
        """
        latest_years = pd.to_numeric(latest_df["ValidityYear"], errors="coerce")
        trends_years = pd.to_numeric(trends_df["GNAYear"], errors="coerce")

        # Combine all valid years into one series
        all_years = (
            pd.concat([latest_years, trends_years])
            .apply(pd.to_numeric, errors="coerce")
            .dropna()
            .astype(int)
        )

        # Make sure years are within valid range
        this_year = datetime.now().year
        all_years = all_years[(all_years >= 1900) & (all_years <= this_year)]

        # Return the overall min and max dates
        min_date = pd.to_datetime(f"{all_years.min()}-01-01")
        max_date = pd.to_datetime(f"{all_years.max()}-01-01")
        return min_date, max_date
