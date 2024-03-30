#!/bin/bash

set -e

# cd /path/to/your/project  # Ensure we're in the right directory

poetry update
dvc pull
sleep 5
poetry run python load_traces/main.py
sleep 5
dvc add data/
git add data.dvc
git commit -m "Daily backup of data"
git push
dvc push
