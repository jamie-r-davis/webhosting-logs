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


def collect_stats():
    df = pd.concat(pd.read_csv(f) for f in PROCESSED_DIR.glob("*/*.csv"))
    df.to_csv(PROCESSED_DIR / "monthly_stats.csv", index=False, header=True)
    print(df)


@cli.command()
@timeit
def process():
    files = PREPROCESSED_DIR.rglob("*.csv")
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_logfile, file): file for file in files}
        for future in as_completed(futures):
            filename: Path = futures[future]
            try:
                output = future.result()
            except Exception as e:
                print(f"{filename.name} generated an exception:\n{e}")
            else:
                print(f"{filename.name} processed")
                domain = filename.parent.name
                output_name = f"{filename.stem}.processed.csv"
                output_path = PROCESSED_DIR / domain / output_name
                output_path.parent.mkdir(parents=True, exist_ok=True)
                print(output_path)
                handlers = [CSVFileHandler(filepath=output_path), StdOutHandler()]
                for handler in handlers:
                    handler.handle_output(output)
    collect_stats()


@cli.command()
@click.argument("domains", nargs=-1)
@timeit
def preprocess(domains, start=None, end=None):
    domains = gather_domains(domains, SRC_DIR)
    print(f"Preprocessing domains: {', '.join(x.name for x in domains)}...")
    start = start or datetime(2021, 8, 1)
    end = end or datetime(2022, 7, 31)
    out_dir = Path("output/preprocessed")
    out_dir.mkdir(parents=True, exist_ok=True)
    for domain in domains:
        domain_out_dir = out_dir / domain.name
        try:
            parse_domain_by_month(domain, domain_out_dir, start, end, validate=False)
        except DomainContinuityError as e:
            ex_type, ex_value, ex_traceback = sys.exc_info()
            print(f"{ex_type.__name__}: {ex_value}")
            pass


@cli.command()
@click.argument("domains", nargs=-1)
@timeit
def count(domains: tuple[str]):
    """Count the number of hits for the provided domains"""
    domains = gather_domains(domains, SRC_DIR)
    print(f"Preprocessing domains: {', '.join(x.name for x in domains)}...")
    counter_dir = OUTPUT_DIR / "counts"
    counter_dir.mkdir(parents=True, exist_ok=True)
    for domain in domains:
        acquia = AcquiaCounter()
        daily = DailyTrafficCounter()
        print(f"Parsing domain: {domain.name}")
        for file in domain.rglob("*.[0-9]?????"):
            print(f"  - Reading: {file.name}...")
            count_log_entries(file, [acquia, daily])
        acquia.to_df().to_csv(counter_dir / f"{domain.name}-acquia.csv", index=False)
        daily.to_df().to_csv(
            counter_dir / f"{domain.name}-dailytraffic.csv", index=False
        )


@cli.command()
@click.argument("domains", nargs=-1)
@timeit
def count_threaded(domains: tuple[str]):
    # administrative tasks
    domains = gather_domains(domains, SRC_DIR)
    counter_dir = OUTPUT_DIR / "counts"
    counter_dir.mkdir(parents=True, exist_ok=True)

    # iterate over each domain, parsing the logs via a thread pool,
    # collecting the results into a dataframe, and writing the collected
    # results to the output folder
    for domain in domains:
        print(f"Parsing domain: {domain.name}")
        df = pd.DataFrame()
        with ProcessPoolExecutor(6) as ex:
            futures = {}
            for file in domain.rglob("*.[0-9]?????"):
                print(f"  - Submitted for processing: {file.name}")
                future = ex.submit(
                    threaded_count_log_entries,
                    logfile=file,
                    log_format=Config.LOG_FORMAT,
                    counter_class=DailyTrafficCounter,
                )
                futures[future] = file
            for future in as_completed(futures):
                file = futures[future]
                try:
                    counter = future.result()
                except Exception as e:
                    print(f"{file.name} raised an exception: {e}")
                else:
                    report = counter.report()
                    total_visits = report.visits.sum()
                    total_views = report.views.sum()
                    print(
                        f"  - Processed: {file.name} ({total_visits:,} visits; {total_views:,} views)"
                    )
                    df = pd.concat([df, counter.report()])

        if len(df) > 0:
            report_file = counter_dir / f"{domain.name}.csv"
            df = df.groupby(["domain", "date"]).sum(numeric_only=False)
            df.to_csv(report_file)
            print(f"Report Created: {report_file.name}")


if __name__ == "__main__":
    cli()
