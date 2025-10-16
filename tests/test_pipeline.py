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
                    "dataset_date": "[2011-01-01T00:00:00 TO 2025-01-01T23:59:59]",
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
                    "notes": "The INFORM Risk Index is a global, open-source risk "
                    "assessment for humanitarian crises and disasters. It can support "
                    "decisions about prevention, preparedness and response.",
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
                ]
                for resource in resources:
                    filename = resource["name"]
                    actual = join(tempdir, filename)
                    expected = join(fixtures_dir, filename)
                    assert_files_same(actual, expected)
