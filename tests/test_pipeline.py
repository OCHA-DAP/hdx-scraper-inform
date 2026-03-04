from os.path import join

from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.inform.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestInform",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(configuration, retriever, tempdir)

                latest_data = pipeline.get_data("latest_url", "ValidityYear")
                trends_data = pipeline.get_data("trends_url", "GNAYear")

                dataset = pipeline.generate_dataset(latest_data, trends_data)
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )

                assert dataset == {
                    "caveats": None,
                    "name": "inform-risk-index",
                    "title": "INFORM Risk Index",
                    "dataset_date": "[2011-01-01T00:00:00 TO 2026-01-01T23:59:59]",
                    "tags": [
                        {
                            "name": "hazards and risk",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "license_id": "cc-by",
                    "methodology": "Composite Indicator",
                    "dataset_source": "INFORM",
                    "groups": [{"name": "world"}],
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "fdbb8e79-f020-4039-ab3a-9adb482273b8",
                    "owner_org": "e116c55a-d536-4b47-9308-94b1c7457afe",
                    "data_update_frequency": 180,
                    "notes": "The overall INFORM risk index identifies countries at risk from "
                    "humanitarian crises and disasters that could overwhelm national "
                    "response capacity. It is made up of three dimensions - hazards and "
                    "exposure, vulnerability and lack of coping capacity. Over the last "
                    "ten years (INFORM Risk Index 2016-2025 2nd edition), there has been "
                    "a general increase in the risk of humanitarian crises at global "
                    "level. While there has been an improvement in coping capacity, this "
                    "has been negated by large increases in the number of people exposed "
                    "to hazards, and to their vulnerability. The development of "
                    "institutions and infrastructure has helped decrease risks, but this "
                    "has not kept pace with the increased exposure to natural hazards "
                    "and conflict, combined with socioeconomic challenges.",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "inform-risk-index.csv",
                        "description": "CSV containing the latest national INFORM Risk Index values.",
                        "format": "csv",
                    },
                    {
                        "name": "inform-risk-index-trends.csv",
                        "description": "CSV containing national INFORM Risk Index trend data from the past 10 years.",
                        "format": "csv",
                    },
                    {
                        "description": "INFORM Concept and Methodology report",
                        "format": "PDF",
                        "name": "inform-codebook.pdf",
                        "url": "https://drmkc.jrc.ec.europa.eu/inform-index/Portals/0/InfoRM/INFORM "
                        "Concept and Methodology Version 2017 Pdf FINAL.pdf",
                    },
                ]
                for resource in resources:
                    if "url" in resource:
                        continue
                    filename = resource["name"]
                    actual = join(tempdir, filename)
                    expected = join(fixtures_dir, filename)
                    assert_files_same(actual, expected)
