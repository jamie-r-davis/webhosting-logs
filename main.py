import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import click
import pandas as pd

from config import Config
from src.counters import AcquiaCounter, DailyTrafficCounter
from src.exceptions import DomainContinuityError
from src.handlers import CSVFileHandler, StdOutHandler
from src.preprocessing import parse_domain_by_month
from src.processors import process_logfile
from src.services import count_log_entries, threaded_count_log_entries
from src.utils import gather_domains, timeit

SRC_DIR = Config.SRC_DIR
OUTPUT_DIR = Config.OUTPUT_DIR
PREPROCESSED_DIR = Config.PREPROCESSED_DIR
PROCESSED_DIR = Config.PROCESSED_DIR


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "-c",
    "--counter",
    type=click.Choice(["acquia", "daily-traffic"]),
    prompt="Which counter should be used?",
    help="The counting method to use for analysis.",
)
@click.argument("domains", nargs=-1)
@timeit
def analyze(counter: str, domains: tuple[str, ...]):
    """
    Parse log files for the given domains, returning counts of views and visitors by day.

    The methodology used depends upon the type of counter that is chosen. Currently, only two counters are implemented:

        :Acquia: Filters and counts traffic according to the methodology outlined by Acquia.

        :Daily Traffic: Filters and counts daily traffic with unique client/user agent combinations.
    """
    # administrative tasks
    domains = gather_domains(domains, SRC_DIR)
    counter_dir = OUTPUT_DIR / "counts"
    counter_dir.mkdir(parents=True, exist_ok=True)
    counter_map = {"acquia": AcquiaCounter, "daily-traffic": DailyTrafficCounter}
    counter_class = counter_map[counter]

    # iterate over each domain, parsing the logs via a thread pool,
    # collecting the results into a dataframe, and writing the collected
    # results to the output folder
    for domain in domains:
        print(f"Parsing domain: {domain.name}")
        df = pd.DataFrame()
        with ProcessPoolExecutor() as ex:
            futures = {}
            for file in domain.rglob("*.[0-9]?????"):
                print(f"  - Submitted for processing: {file.name}")
                future = ex.submit(
                    threaded_count_log_entries,
                    logfile=file,
                    log_format=Config.LOG_FORMAT,
                    counter_class=counter_class,
                )
                futures[future] = file
            for future in as_completed(futures):
                file = futures[future]
                try:
                    counter_obj = future.result()
                except Exception as e:
                    print(f"{file.name} raised an exception: {e}")
                else:
                    report = counter_obj.report()
                    total_visits = report.visits.sum()
                    total_views = report.views.sum()
                    print(
                        f"  - Processed: {file.name} ({total_visits:,} visits; {total_views:,} views)"
                    )
                    df = pd.concat([df, counter_obj.report()])

        if len(df) > 0:
            report_file = counter_dir / f"{domain.name}-{counter_obj.name}.csv"
            df = df.groupby(["domain", "date"]).sum(numeric_only=False)
            df.to_csv(report_file)
            print(f"Report Created: {report_file.name}")


if __name__ == "__main__":
    cli()
