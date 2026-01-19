# CMS Provider Hospitals ETL

## Overview
This project downloads CMS Provider datasets related to the Hospitals theme, normalizes column names to snake_case, and stores the results locally. The job supports incremental daily runs and only processes datasets that have been modified since the previous ingestion.

## Features
- Filters CMS datasets by the "Hospitals" theme
- Downloads CSV datasets in parallel
- Converts column names to snake_case
- Tracks last run time using a metadata file

## Requirements
- Python 3.9+
- Packages listed in `requirements.txt`

## Setup
```bash
pip install -r requirements.txt
