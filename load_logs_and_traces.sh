#!/bin/bash

set -e

cd "$HOME/anomalies-detection" || exit 1  # Ensure we're in the right directory
source .venv/bin/activate

echo "Pull code changes"
git pull
echo "Pull data changes"
dvc pull
echo "Starting script"
python load_traces/main.py
echo "add changes"
dvc add data/
git add data.dvc
echo "Commit changes"
git commit -m "Daily backup of data"
echo "Push changes"
git push
dvc push

