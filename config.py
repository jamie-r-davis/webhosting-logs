import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class Config:
    LOG_FORMAT = '%h %l %u %t "%r" %s %b "%{Referer}i" "%{User-agent}i" %D'
    SRC_DIR = Path(os.getenv("SRC_DIR", "data"))
    OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output"))
    PREPROCESSED_DIR = OUTPUT_DIR / "preprocessed"
    PROCESSED_DIR = OUTPUT_DIR / "processed"
