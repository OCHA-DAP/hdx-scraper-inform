### Collector for INFORM Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-inform/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-inform/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-inform/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-inform?branch=main)

This script connects to the [INFORM Risk API](https://drmkc.jrc.ec.europa.eu/inform-index/About) and the [INFORM Severity API](https://api.acaps.org/) extracts data and metadata to creating a dataset in HDX. It makes xx reads from the APIs and 1 read/write (API calls) to HDX. It creates xx temporary files each a couple MB which it uploads into HDX. It is run every month.


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yaml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yaml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-inform** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, TEMP_DIR, LOG_FILE_ONLY