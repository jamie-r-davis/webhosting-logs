from abc import ABC
from collections import Counter
from datetime import datetime
from typing import Iterable

import pandas as pd

from src import filters
from src.models import LogRecord


class AbstractCounter(ABC):
    name: str
    filters: list[filters.Filter]
    fields: list[str]
    adapters: {}

    def __init__(self):
        self.data = Counter()

    def filter(self, record: LogRecord) -> bool:
        for filter in self.filters:
            if filter.filter(record.entry) is False:
                return False
        return True

    def handle(self, record: LogRecord) -> bool:
        if self.filter(record):
            self.add_entry(record)

    def add_entry(self, record: LogRecord):
        attrs = []
        for field in self.fields:
            value = getattr(record, field)
            if field in self.adapters:
                value = self.adapters[field](value)
            attrs.append(value)
        self.data[tuple(attrs)] += 1

    def reset(self):
        self.data = set()

    @property
    def views(self) -> int:
        return self.data.total()

    @property
    def visits(self) -> int:
        return len(self.data)

    @property
    def flattened_data(self) -> Iterable[dict]:
        """Flattens self.data into an iterable of dicts"""
        for keys, views in self.data.items():
            row = {field: value for field, value in zip(self.fields, keys)}
            row["views"] = views
            yield row

    def to_df(self) -> pd.DataFrame:
        if len(self.data) == 0:
            df = pd.DataFrame(columns=self.fields + ["views"])
        else:
            df = pd.DataFrame(self.flattened_data)
        if "request_time" in self.fields:
            df["request_time"] = pd.to_datetime(df.request_time)
        return df

    def report(self) -> pd.DataFrame:
        raise NotImplementedError


class AcquiaCounter(AbstractCounter):
    """
    A counter class which aggregates the number of unique visits per domain per day according to the log
    described on Acquia's Usage Limits page (https://docs.acquia.com/cloud-platform/subs/usage-limits/#what-s-a-view).
    """

    name = "acquia"
    filters = [
        filters.StatusFilter(redirects_ok=False),
        filters.MethodFilter(["GET"]),
        filters.UriFilter(["favicon.ico", "robots.txt", ".well-known"]),
        filters.UriExtensionFilter(),
    ]
    fields = ["domain", "request_time", "remote_host", "user_agent"]
    adapters = {"request_time": lambda x: datetime(x.year, x.month, x.day, x.hour)}

    def report(self) -> pd.DataFrame:
        """Aggregates visits & views by hour"""
        df = self.to_df()
        grouped = df.groupby(["domain", df.request_time.dt.to_period("D")]).agg(
            {"views": ["count", "sum"]}
        )
        # drop the top level of the grouped df, so there's just one level of 'count', 'sum' columns
        grouped.columns = grouped.columns.droplevel()

        # reset the index and rename columns
        grouped = grouped.reset_index().rename(
            columns={"request_time": "date", "count": "visits", "sum": "views"}
        )
        return grouped


class DailyTrafficCounter(AbstractCounter):
    """
    A counter class which aggregates the number of unique visits per domain per day.

    Unique visits are defined as unique combinations of remote host + user agent. So if you were to visit multiple
    pages within the domain on the same day, your activity would count as one visit.
    """

    name = "daily-traffic"
    filters = [
        filters.StatusFilter(redirects_ok=True),
        filters.MethodFilter(["GET"]),
        filters.UriFilter(["favicon.ico", "robots.txt", ".well-known"]),
    ]
    fields = ["domain", "request_time", "remote_host", "user_agent"]
    adapters = {"request_time": lambda x: x.date()}

    def report(self) -> pd.DataFrame:
        """Generates a report with columns 'domain', 'date', 'visits', 'views'"""
        df = self.to_df()
        grouped = df.groupby(["domain", "request_time"]).agg(
            {"views": ["count", "sum"]}
        )
        grouped.columns = grouped.columns.droplevel()
        grouped.reset_index(inplace=True)
        grouped.rename(
            columns={"request_time": "date", "count": "visits", "sum": "views"},
            inplace=True,
        )
        return grouped
