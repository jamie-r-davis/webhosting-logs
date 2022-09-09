from dataclasses import dataclass
from pathlib import Path
from typing import Union

import pandas as pd


@dataclass
class HitCounter:
    def __init__(self):
        self.data = set()

    def add_data(self, df: pd.DataFrame) -> None:
        for x in df.itertuples():
            self.data.add((x.domain, x.time, x.client, x.user_agent))

    def add_entry(self, entry: tuple):
        self.data.add(entry)

    def to_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            self.data, columns=["domain", "time", "client", "user_agent"]
        )

    @property
    def hits_per_month(self) -> pd.DataFrame:
        """Returns a dataframe aggregating the number of unique hits (defined by client + user agent by day) per domain by month"""
        df = self.to_df().drop_duplicates()
        df["time"] = pd.to_datetime(df.time)
        df["month"] = df.time.dt.to_period("M")
        return (
            df.groupby(["domain", "month"])[["client"]]
            .count()
            .rename(columns={"client": "hits"})
            .reset_index()
        )


def process_logfile(filepath: Union[str, Path]) -> pd.DataFrame:
    """Processes a single log file, returning a dataframe of hits per unique clients/user agents per day for each
    domain"""
    chunksize = 50_000
    counter = HitCounter()
    print(f"Processing started: {filepath}")

    dtypes = {
        "domain": pd.StringDtype(),
        "client": pd.StringDtype(),
        "user_agent": pd.StringDtype(),
        "time": pd.StringDtype(),
    }
    with pd.read_csv(
        filepath, sep="\t", dtype=dtypes, parse_dates=["time"], chunksize=chunksize
    ) as reader:
        for i, chunk in enumerate(reader):
            counter.add_data(chunk)
            print(f"{filepath.name}: {(i+1)*chunksize:,} rows processed")
    return counter.hits_per_month


def process_raw_file(filepath: Union[str, Path]) -> pd.DataFrame:
    """Processes a file using standard i/o"""
    if isinstance(filepath, str):
        filepath = Path(filepath)

    counter = HitCounter()
    with open(filepath) as f:
        f.readline()
        for i, line in enumerate(f.readlines()):
            domain, client, user_agent, time = line.split("\t")
            counter.add_entry((domain, time, client, user_agent))
            if i > 0 and i % 1000 == 0:
                print(f"{filepath.name}: Processed {i+1} lines...")

    return counter.hits_per_month
