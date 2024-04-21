#!/bin/bash

cd "$HOME/anomalies-detection" || exit 1  # Ensure we're in the right directory
source .venv/bin/activate

git pull
dvc pull
python load_logs/logs_to_ds_collector.py

