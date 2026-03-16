import os
from pathlib import Path

# project root directory
# __file__ is the relative path to this exact file (paths.py), config/paths.py  ← relative
# .resolve() converts it to a full absolute path, i.e /home/user/projects/ecommerce-elt/config/paths.py,
# and makes the path bulletproof so it works the same whether you run the script from inside the project folder or from somewhere else on your machine.
# .parent.parent steps up two levels to the project i.e /home/user/projects/ecommerce-elt/
# BASE_DIR = /home/user/projects/ecommerce-elt/ regardless of where you run the script from, as long as it's within the project folder. 
BASE_DIR = Path(os.getenv("PROJECT_DIR", Path(__file__).resolve().parent.parent))

# data directories
# DATA_DIR set via .env variable DATA_DIR=.. → (Docker environment)
# or if not set, it defaults to BASE_DIR/data → (local development environment)
# This allows the code to be flexible and work in different environments without needing to change the code
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
# = /home/user/projects/ecommerce-elt/data


# Raw and processed data directories, defined relative to DATA_DIR
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"




