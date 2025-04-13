#!/usr/bin/env python3
import requests
import time
import json
import sys
import os
from datetime import datetime, timedelta

# API parameters
SERVICE_CODE = "Mayor's 24 Hour Hotline:Needle Program:Needle Pickup"
API_ENDPOINT = "https://311.boston.gov/open311/v2/requests.json"
PER_PAGE = 100          # Maximum allowed per request
SLEEP_SECONDS = 8       # Pause between API calls (fewer than 10 queries per minute)

# User-Agent header (customize as needed)
HEADERS = {"User-Agent": "BostonNeedleReportsDownloader/1.0"}

def fetch_reports_for_date_range(start_date_param, end_date_param, page=1):
    """
    Fetch a single page of reports for the given interval using start_date and end_date.
    Both parameters should be ISO8601 strings.
    """
    params = {
        "service_code": SERVICE_CODE,
        "per_page": PER_PAGE,
        "page": page,
        "start_date": start_date_param,
        "end_date": end_date_param
    }
    print(f"Fetching page {page} for interval {start_date_param} to {end_date_param}")
    try:
        response = requests.get(API_ENDPOINT, params=params, headers=HEADERS)
    except Exception as e:
        print(f"Error fetching page {page}: {e}")
        sys.exit(1)

    if response.status_code != 200:
        print(f"Error: Received status code {response.status_code} on page {page}")
        print(response.text)
        sys.exit(1)

    try:
        data = response.json()
    except Exception as e:
        print(f"Error parsing JSON for page {page}: {e}")
        sys.exit(1)

    # The API may return the records under different keys.
    if "result" in data:
        records = data["result"].get("requests") or data["result"].get("records") or []
    else:
        records = data if isinstance(data, list) else []
    return records

def fetch_reports_for_range(start_date_param, end_date_param):
    """
    For a given time interval (start_date to end_date), paginate through results.
    """
    interval_reports = []
    page = 1
    while True:
        records = fetch_reports_for_date_range(start_date_param, end_date_param, page=page)
        print(f"Fetched {len(records)} records on page {page}")
        if not records:
            break
        interval_reports.extend(records)
        if len(records) < PER_PAGE:
            # No further pages in this interval.
            break
        page += 1
        time.sleep(SLEEP_SECONDS)
    return interval_reports

def main():
    # Define the overall date range: January 1, 2015 until now.
    overall_start = datetime(2015, 1, 1)
    overall_end = datetime.now()
    print(f"Fetching reports from {overall_start.isoformat()} to {overall_end.isoformat()}")

    # Use a 90-day interval (you can adjust this if needed).
    interval_delta = timedelta(days=90)
    current_start = overall_start
    all_reports = []

    while current_start < overall_end:
        current_end = min(current_start + interval_delta, overall_end)
        # Create ISO8601 strings for start_date and end_date.
        # Append "Z" to indicate UTC time.
        start_date_str = current_start.isoformat() + "Z"
        end_date_str = current_end.isoformat() + "Z"
        print(f"\nProcessing interval: {start_date_str} to {end_date_str}")
        interval_reports = fetch_reports_for_range(start_date_str, end_date_str)
        print(f"Retrieved {len(interval_reports)} records for this interval")
        all_reports.extend(interval_reports)
        current_start = current_end
        time.sleep(SLEEP_SECONDS)  # Additional sleep between intervals.

    print(f"\nTotal reports fetched: {len(all_reports)}")

    # Ensure the data folder exists and write the combined results.
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)
    output_filename = os.path.join(output_dir, "reports.json")
    try:
        with open(output_filename, "w") as outfile:
            json.dump(all_reports, outfile, indent=2)
        print(f"Data successfully written to {output_filename}")
    except Exception as e:
        print(f"Error writing data to file {output_filename}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
