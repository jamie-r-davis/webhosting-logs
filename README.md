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

By default, the script will search for any `.csv` files in the `data/` directory. You can change the search location by specifying a different `SRC_DIR` in `main.py`. As the script parses each source file, it will generate the aggregated stats for that file in `output/{filename}.processed.{ext}`. You can modify the output path by changing `OUTPUT_DIR`. 

Once you have placed the appropriate log files in the source directory, run the script like so:
```bash
python main.py
```

The script processes your source files in parallel using a thread pool that will automatically scale based on the number of cores available to your machine. As it finishes processing, output will be logged to stdout and the results will be saved in the output directory.