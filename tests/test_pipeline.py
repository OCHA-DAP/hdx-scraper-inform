from os.path import join

from hdx.data.dataset import Dataset
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.inform.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir):
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

                dataset = Dataset(
                    {"name": "inform-risk-index-2021", "title": "INFORM Risk Index"}
                )
                pipeline.generate_dataset(latest_data, trends_data, dataset)

                assert dataset["name"] == "inform-risk-index-2021"
                assert dataset["title"] == "INFORM Risk Index"
                assert (
                    dataset["dataset_date"]
                    == "[2011-01-01T00:00:00 TO 2025-01-01T23:59:59]"
                )
                assert dataset["tags"] == [
                    {
                        "name": "hazards and risk",
                        "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                    }
                ]

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "inform_risk_index.csv",
                        "description": "CSV containing the latest national INFORM Risk Index values.",
                        "format": "csv",
                    },
                    {
                        "name": "inform_risk_index_trends.csv",
                        "description": "CSV containing national INFORM Risk Index trend data from the past 10 years.",
                        "format": "csv",
                    },
                    {
                        "description": "INFORM Concept and Methodology report",
                        "format": "PDF",
                        "name": "inform_codebook.pdf",
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
