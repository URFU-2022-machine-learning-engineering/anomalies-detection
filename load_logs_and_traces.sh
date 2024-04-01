#!/bin/bash

set -e

cd "$HOME/anomalies-detection" || exit 1  # Ensure we're in the right directory
source .venv/bin/activate


git pull
dvc pull
sleep 5
python load_traces/main.py
sleep 5
dvc add data/
git add data.dvc
git commit -m "Daily backup of data"
git push
poetry run dvc push

