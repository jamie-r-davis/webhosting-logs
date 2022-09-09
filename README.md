# Web Hosting Logs

A simple script to parse & aggregate traffic from web server logs.

This script will parse csv files from a source directory and count the unique number of hits for a given domain within a particular month. Unique hits are defined as unique client + user agent combinations logged within a single day (eg, if my browser visits a domain twice in one day, that will count as one hit).


## Getting Started

To get started, you will need to have Python 3.10 installed on your machine along with an install of pipenv, which is used for package management.
```bash
pip install --upgrade pip
pip install --upgrade pipenv
```

Once you have Python and Pipenv setup, clone this repository to your local machine. Then navigate into the root project folder and install dependencies with pipenv.

```bash
git clone https://github.com/jamie-r-davis/webhosting-logs.git
cd webhosting-logs
pipenv install
```

Once you have all of the project dependencies installed, you will be ready to begin parsing your log files.

## Usage
This script is configured to parse the web logs in two phases. The first phase is a 'preprocessing' phase where data from each domain is parsed and filtered into a collection of csv files, each containing a subset of filtered entries that meet the project criteria.

Once data has been preprocessed, the 'processing' phase aggregates the monthly statistics for each domain by deduplicating client + user agent entries by day.

To run the preprocessing, use the following command:
```bash
pipenv run python main.py preprocess all
```
This will iterate through each domain in your `data/` folder and parse the raw logs. If you want to only parse a particular subset of domains, replace the `all` argument with a list of domains (eg, `detroit debate2020 admissions`).

Once the preprocessing has finished, a collection of csvs will have been generated in the `output/preprocessed/` directory. To parse the monthly statistics, run:

```bash
pipenv run python main.py process
```

This will generate a set of csvs containing the monthly statistics for each domain in the `output/processed/` directory.
