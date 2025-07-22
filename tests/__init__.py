import sys
from pathlib import Path

PROJECT_PATH = Path(__file__).cwd()
SOURCE_FILES_PATH = PROJECT_PATH.joinpath("src")
sys.path.append(str(SOURCE_FILES_PATH))
