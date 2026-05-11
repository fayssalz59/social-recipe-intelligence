from pathlib import Path
import os
import subprocess
import sys

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent
DBT_DIR = REPO_ROOT / "dbt_project"

load_dotenv(REPO_ROOT / ".env", override=True)

env = os.environ.copy()

cmd = ["dbt", *sys.argv[1:], "--profiles-dir", "."]

result = subprocess.run(
    cmd,
    cwd=DBT_DIR,
    env=env,
)

raise SystemExit(result.returncode)