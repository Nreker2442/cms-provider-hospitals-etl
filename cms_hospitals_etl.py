import os
import re
import json
import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

API_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "cms_provider_hospitals_csv_output")
METADATA_FILE = os.path.join(OUTPUT_DIR, "metadata_last_update.json")
MAX_WORKERS = 5 # Number of threads for parallel processing

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Functions to process CMS Provider Hospital Data

# logging helper
def log(msg):
    print(f"{msg} [{datetime.now()}]")


# Load last run timestamp from metadata file
def load_last_update_date():
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            data = json.load(f)
            return datetime.fromisoformat(data.get("last_run"))
    print(f"Metadata file {METADATA_FILE} does not exist. Assuming first run.")
    return datetime.min

# Update metadata file with last run timestamp
def update_last_run_date():
    with open(METADATA_FILE, "w") as f:
        json.dump({"last_run": datetime.now().isoformat()}, f)

# Convert column names to snake_case
def to_snake_case(col_name):
    col_name = col_name.lower()                     # Convert to lowercase
    col_name = re.sub(r"[^a-z0-9]+", "_", col_name) # Replace sequence of non-alphanumeric characters with underscore               
    return col_name.strip()                         # Strip leading/trailing spaces

# Fetch datasets from the CMS API where theme = "Hospitals"
def fetch_hospital_datasets():
    response = requests.get(API_URL)
    response.raise_for_status()
    datasets = response.json()
    # Filter for theme containing "Hospitals"
    return [ds for ds in datasets if ds.get("theme") and "Hospitals" in ds["theme"]]


# Download, process, and save the dataset
def process_dataset(dataset, last_run_date):
    dataset_id = dataset.get("identifier")
    modified = dataset.get("modified")
    if not dataset_id or not modified:
        return None

    modified_date = datetime.fromisoformat(modified)
    if modified_date <= last_run_date:
        log(f"Skipping unchanged dataset: {dataset_id}")
        return None

    # Get CSV download URL
    distributions = dataset.get("distribution", [])
    if not distributions:
        log(f"No download URL for dataset {dataset_id}")
        return None

    download_url = distributions[0].get("downloadURL")
    if not download_url:
        log(f"No valid download URL for dataset {dataset_id}")
        return None

    try:
        log(f"Downloading dataset: {dataset_id}")
        df = pd.read_csv(download_url, dtype=str)
        # Convert columns to snake_case
        df.columns = [to_snake_case(col) for col in df.columns]
        output_file = os.path.join(OUTPUT_DIR, f"{dataset_id}.csv")
        df.to_csv(output_file, index=False)
        log(f"Saved dataset: {dataset_id} as {output_file}")
        return dataset_id
    except Exception as e:
        log(f"Failed to process dataset {dataset_id}: {e}")
        return None


def run_etl_job():
    log("CMS Provider Hospital ETL Job Started at")
    last_run_date = load_last_update_date()
    cms_provider_hospital_datasets = fetch_hospital_datasets()
    processed_datasets = []

    # Use ThreadPoolExecutor for parallel downloads
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_dataset, ds, last_run_date): ds for ds in cms_provider_hospital_datasets}
        for future in as_completed(futures):
            result = future.result()
            if result:
                processed_datasets.append(result)

    # Update metadata
    update_last_run_date()
    log(f"CMS Provider Hospital ETL Job Completed. Datasets processed: {len(processed_datasets)}")


if __name__ == "__main__":
    run_etl_job()

