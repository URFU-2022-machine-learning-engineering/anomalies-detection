# URFU 2022 Machine Learning Engineering Project
## Overview

This project is designed to collect, process, and enrich span and log data from Jaeger traces, focusing on specific service operations. The collected data is processed into a structured format, enriched with log details, and saved for further analysis.

## Project Structure
- main.py: The main script to fetch and process Jaeger traces and logs.
- logs_to_ds_collector.py: Handles the collection and processing of log data.
- concat.py: Concatenates multiple CSV files and enriches the span data with logs.


## Installation
#### Prerequisites
- Python 3.x
- Poetry ^1.7.1
- Necessary Python libraries: Install using poetry.

#### Setup
- Clone the repository

```bash
git clone https://github.com/URFU-2022-machine-learning-engineering
cd URFU-2022-machine-learning-engineering
```
#### Install dependencies

```bash
poetry install
```
## Configuration
- Create a config.yml file in the root directory with server details.
- Set environment variables for JAEGER_USERNAME and JAEGER_PASSWORD.

## Usage
Fetching and Processing Jaeger Traces

Run the main script to fetch and process Jaeger traces:

```bash
python load_traces/main.py
```
Logs to DataFrame Collector

This script collects logs and prepares them for enrichment:

```bash
python logs/logs_to_ds_collector.py
```
Concatenation and Enrichment

To concatenate CSV files and enrich spans with log data:

```bash
python concat.py
```

## File Details
### main.py

- Purpose: Fetches Jaeger traces, checks for existing data, loads it, and updates it with new data.
- Functions:
- - get_most_recent_file(directory, pattern): Finds the most recent file matching a pattern.
- - fetch_jaeger_traces: Fetches traces based on configuration.
- - convert_tag_value: Utility to handle tag value conversion.
- - get_date_strings: Utility to fetch date ranges.

### logs_to_ds_collector.py

- Purpose: Collects log data and converts it into a structured DataFrame.
- Functions:
- - collect_logs(directory_path): Collects and processes log files into a DataFrame.
- - enrich_spans_with_logs(spans_df, logs_file_path): Enriches span data with logs.

### concat.py

- Purpose: Concatenates multiple CSV files and removes duplicates.
- Functions:
- - process_spans_data(directory_path): Processes and cleans span data from multiple CSV files.
- - enrich_spans_with_logs(spans_df, logs_file_path): Merges log data with span data.

## Contributing
- Fork the repository.
- Create a new feature branch.
- Commit your changes.
- Push to the branch.
- Open a Pull Request.

## License

This project is licensed under the GPLv3 License - see the LICENSE file for details.

