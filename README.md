# Web Hosting Logs

A simple script to parse & aggregate traffic from web server logs, counting the number of hits using different methodologies.


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

To begin analyzing your log files, you will need to have the available on your machine with the files for each domain within their own subfolders:
```text
logfiles/
├─ domain1/
│  ├─ domain1.080122.gz
│  ├─ domain1.080222.gz
│  ├─ ...
├─ domain2/
   ├─ ...
```

and set a `SRC_DIR` environment variable with the absolute path to the directory containing your logs. For convenience, you can create a `.env` file in the root of the project folder and declare your variable like so:
```dotenv
# .env
SRC_DIR=/source/to/your/logfiles
```

Then, from the command line, run the following command to start the analysis of all domains:
```bash
pipenv run python main.py analyze --counter daily-traffic all
```

If you only want to analyze a subset of domains, replace the `all` argument with a list of domains:
```bash
pipenv run python main.py analyze --counter daily-traffic domain1 domain2
```

The script will analyze any log files that it finds for the specified domains and keep a count of all valid traffic. What qualifies as valid traffic depends on the counter you choose at runtime. If a `--counter` option is not specified from the command line, you will be prompted before the anlaysis begins.

## Output

Once the script has finished analyzing a domain, a statistics csv will be generated in the output folder within the project. This will contain the domain, date, visits, and views for the analyzed logs.


## Counters

### Acquia
The Acquia implements the logic laid out by Acquia for how they count visits and views:
- Only 200-level GET requests
- Exclude requests for static content
- Exclude traffic from known bots
- Visits are counted as unique user agent + remote host combinations per hour
- Views are all requests that meet the above criteria

### Daily Traffic
The Daily Traffic counter intends to count unique visits by implementing the following logic:
- Only 200 & 300 level GET requests
- Exclude traffic to `robots.txt`, `favicon`, and `.well-known` URIs
- Visits are defined as unique user agent + remote host combinations per day
- Views are all requests that meet the above criteria