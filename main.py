from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from src.handlers import CSVFileHandler, StdOutHandler
from src.processors import process_logfile

SRC_DIR = Path("data")
OUTPUT_DIR = Path("output")
GLOB_PATTERN = "*.csv"


if __name__ == "__main__":
    print(f"Processing started: {datetime.now():%Y-%m-%d %H:%M:%S}")
    files = SRC_DIR.glob(GLOB_PATTERN)
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
                output_path = OUTPUT_DIR / f"{filename.stem}.processed{filename.suffix}"
                print(output_path)
                handlers = [CSVFileHandler(filepath=output_path), StdOutHandler()]
                for handler in handlers:
                    handler.handle_output(output)
    print(f"Processing finished: {datetime.now():%Y-%m-%d %H:%M:%S}")
