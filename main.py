import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import click
import pandas as pd

from src.exceptions import DomainContinuityError
from src.handlers import CSVFileHandler, StdOutHandler
from src.preprocessing import parse_domain_by_month
from src.processors import process_logfile
from src.utils import timeit

SRC_DIR = Path("data")
OUTPUT_DIR = Path("output")
PREPROCESSED_DIR = OUTPUT_DIR / "preprocessed"
PROCESSED_DIR = OUTPUT_DIR / "processed"


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
    if domains[0] == "all":
        domains = [x for x in SRC_DIR.iterdir() if x.is_dir()]
    else:
        domains = [SRC_DIR / domain for domain in domains]
    print(f"Preprocessing domains: {', '.join(x.name for x in domains)}...")
    start = start or datetime(2021, 8, 1)
    end = end or datetime(2022, 7, 31)
    out_dir = Path("output/preprocessed")
    out_dir.mkdir(parents=True, exist_ok=True)
    for domain in domains:
        domain_out_dir = out_dir / domain.name
        try:
            parse_domain_by_month(domain, domain_out_dir, start, end)
        except DomainContinuityError as e:
            ex_type, ex_value, ex_traceback = sys.exc_info()
            print(f"{ex_type.__name__}: {ex_value}")
            pass


if __name__ == "__main__":
    cli()
