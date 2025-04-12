#!/usr/bin/env python3
import requests
import time
import json
import sys
import os
from datetime import datetime, timedelta

# API and query parameters

# Ensure the data directory exists.
os.makedirs("data", exist_ok=True)

output_filename = "data/reports.json"
with open(output_filename, "w") as outfile:
    json.dump(all_reports, outfile, indent=2)
print(f"Data successfully written to {output_filename}")
SERVICE_CODE = "Mayor's 24 Hour Hotline:Needle Program:Needle Pickup"
API_ENDPOINT = "https://311.boston.gov/open311/v2/requests.json"
PER_PAGE = 100  # maximum allowed per request
SLEEP_SECONDS = 8  # to enforce fewer than 10 queries per minute

HEADERS = {"User-Agent": "BostonNeedleReportsDownloader/1.0"}

def fetch_reports_for_date_range(updated_after, updated_before, page=1):
    """
    Fetches one page of reports for the specified date range.
    updated_after and updated_before should be ISO8601 formatted strings.
    """
    params = {
        "service_code": SERVICE_CODE,
        "per_page": PER_PAGE,
        "page": page,
        "updated_after": updated_after,
        "updated_before": updated_before
    }
    print(f"Fetching page {page} for date range {updated_after} - {updated_before}")
    response = requests.get(API_ENDPOINT, params=params, headers=HEADERS)
    if response.status_code != 200:
        print(f"Error: Received status code {response.status_code} on page {page}")
        print(f"Response: {response.text}")
        sys.exit(1)
    data = response.json()
    if "result" in data:
        records = data["result"].get("requests") or data["result"].get("records") or []
    else:
        records = data if isinstance(data, list) else []
    return records

def fetch_reports_for_range(updated_after, updated_before):
    """
    Retrieves all paginated reports for a given time interval defined by updated_after and updated_before.
    """
    all_records = []
    page = 1
    while True:
        records = fetch_reports_for_date_range(updated_after, updated_before, page=page)
        print(f"Fetched {len(records)} records on page {page}")
        if not records:
            break
        all_records.extend(records)
        if len(records) < PER_PAGE:
            # No more pages in this time window.
            break
        page += 1
        time.sleep(SLEEP_SECONDS)
    return all_records

def main():
    # Define the overall date range you want (modify as needed)
    overall_start = datetime(2020, 1, 1)
    overall_end = datetime.now()  # or a fixed end date
    print(f"Fetching data from {overall_start.isoformat()} to {overall_end.isoformat()}")

    # We'll break the overall date range into intervals (e.g., one week each)
    delta = timedelta(weeks=1)
    current_start = overall_start
    combined_records = []
    
    while current_start < overall_end:
        current_end = min(current_start + delta, overall_end)
        # Convert to ISO format
        updated_after = current_start.isoformat() + "Z"  # append Z if needed for UTC
        updated_before = current_end.isoformat() + "Z"
        records = fetch_reports_for_range(updated_after, updated_before)
        print(f"Total records for {updated_after} to {updated_before}: {len(records)}")
        combined_records.extend(records)
        current_start = current_end
        time.sleep(SLEEP_SECONDS)  # Additional pause to avoid rate limits

    print(f"Total combined records fetched: {len(combined_records)}")
    
    # Save to JSON file.
    output_filename = "reports.json"
    try:
        with open(output_filename, "w") as outfile:
            json.dump(combined_records, outfile, indent=2)
        print(f"Data successfully written to {output_filename}")
    except Exception as e:
        print(f"Error writing data to file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
