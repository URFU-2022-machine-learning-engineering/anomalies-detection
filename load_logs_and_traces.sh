#!/bin/bash

set -e

cd "$HOME/anomalies-detection" || exit 1  # Ensure we're in the right directory

git pull
poetry run dvc pull
sleep 5
poetry run python load_traces/main.py
sleep 5
poetry run dvc add data/
git add data.dvc
git commit -m "Daily backup of data"
git push
poetry run dvc push

