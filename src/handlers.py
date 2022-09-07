from pathlib import Path
from typing import Protocol, Union

import pandas as pd


class OutputHandler(Protocol):
    def handle_output(self, output: pd.DataFrame) -> None:
        ...


class CSVFileHandler:
    def __init__(self, filepath: Union[str, Path], delimiter=","):
        self.filepath = filepath
        self.delimiter = delimiter
        if not filepath.exists():
            filepath.parent.mkdir(parents=True, exist_ok=True)

    def handle_output(self, output: pd.DataFrame):
        """Writes the given output to the file system"""
        with self.filepath.open("w") as f:
            output.to_csv(f, index=False, header=True, sep=self.delimiter)


class StdOutHandler:
    def handle_output(self, output: pd.DataFrame):
        """Dumps the given output to stdout"""
        print("Ouput DataFrame (head):")
        print(output.head())
        print(f"N rows: {len(output)}")
