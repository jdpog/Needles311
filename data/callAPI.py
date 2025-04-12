#!/usr/bin/env python3
import requests
import time
import json
import sys
from datetime import datetime, timedelta

# API and query parameters
SERVICE_CODE = "Mayor's 24 Hour Hotline:Needle Program:Needle Pickup"
API_ENDPOINT = "https://311.boston.gov/open311/v2/requests.json"
PER_PAGE = 100  # Maximum allowed per request.
SLEEP_SECONDS = 6  # Pause between requests to keep under 10 queries per minute.

# User-Agent header (customize as needed)
HEADERS = {"User-Agent": "BostonNeedleReportsDownloader/1.0"}

def fetch_reports_for_date_range(updated_after, updated_before, page=1):
    """
    Fetches one page of reports updated between the given dates.
    Both updated_after and updated_before should be ISO8601 strings.
    """
    params = {
        "service_code": SERVICE_CODE,
        "per_page": PER_PAGE,
        "page": page,
        "updated_after": updated_after,
        "updated_before": updated_before
    }
    print(f"Fetching page {page} for date range {updated_after} to {updated_before}")
    response = requests.get(API_ENDPOINT, params=params, headers=HEADERS)
    if response.status_code != 200:
        print(f"Error: Received status code {response.status_code} for page {page}")
        print(f"Response: {response.text}")
        sys.exit(1)
    try:
        data = response.json()
    except Exception as e:
        print(f"Error parsing JSON for page {page}: {e}")
        sys.exit(1)
    # Try common keys for records.
    if "result" in data:
        records = data["result"].get("requests") or data["result"].get("records") or []
    else:
        records = data if isinstance(data, list) else []
    return records

def fetch_reports_for_range(updated_after, updated_before):
    """
    Retrieves all pages of reports for a given time interval defined by updated_after and updated_before.
    """
    reports_in_range = []
    page = 1
    while True:
        records = fetch_reports_for_date_range(updated_after, updated_before, page=page)
        print(f"Fetched {len(records)} records on page {page}")
        if not records:
            break
        reports_in_range.extend(records)
        if len(records) < PER_PAGE:
            # This interval is complete.
            break
        page += 1
        time.sleep(SLEEP_SECONDS)
    return reports_in_range

def main():
    # Define the overall date range you want to retrieve.
    # Adjust overall_start and overall_end as needed.
    overall_start = datetime(2020, 1, 1)
    overall_end = datetime.now()  # Or specify a fixed end date.
    print(f"Fetching data from {overall_start.isoformat()} to {overall_end.isoformat()}")

    # Define the interval length. Here we use a one-week interval.
    delta = timedelta(weeks=1)
    current_start = overall_start
    all_reports = []  # We'll accumulate all reports in this list.
    
    while current_start < overall_end:
        current_end = min(current_start + delta, overall_end)
        # Convert to ISO8601 strings. Append "Z" to indicate UTC.
        updated_after = current_start.isoformat() + "Z"
        updated_before = current_end.isoformat() + "Z"
        print(f"Processing interval: {updated_after} to {updated_before}")
        interval_reports = fetch_reports_for_range(updated_after, updated_before)
        print(f"Total records for this interval: {len(interval_reports)}")
        all_reports.extend(interval_reports)
        current_start = current_end
        time.sleep(SLEEP_SECONDS)  # Additional pause between intervals.

    print(f"Total reports fetched: {len(all_reports)}")
    
    # Write all fetched reports to a JSON file in the repository's data folder.
    output_filename = "data/reports.json"
    # Ensure the 'data' folder exists.
    import os
    os.makedirs("data", exist_ok=True)
    try:
        with open(output_filename, "w") as outfile:
            json.dump(all_reports, outfile, indent=2)
        print(f"Data successfully written to {output_filename}")
    except Exception as e:
        print(f"Error writing data to file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
