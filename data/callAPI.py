#!/usr/bin/env python3
import requests
import time
import json
import sys

# API and query parameters
SERVICE_CODE = "Mayor's 24 Hour Hotline:Needle Program:Needle Pickup"
API_ENDPOINT = "https://311.boston.gov/open311/v2/requests.json"
PER_PAGE = 100  # maximum allowed per request
SLEEP_SECONDS = 10  # to enforce fewer than 10 queries per minute

# Customize your User-Agent header per API requirements.
HEADERS = {"User-Agent": "BostonNeedleReportsDownloader/1.0"}

def fetch_all_reports():
    all_results = []
    page = 1

    while True:
        # Construct parameters for current page request.
        params = {
            "service_code": SERVICE_CODE,
            "per_page": PER_PAGE,
            "page": page
        }
        print(f"Fetching page {page}...")
        try:
            response = requests.get(API_ENDPOINT, params=params, headers=HEADERS)
        except Exception as e:
            print(f"Error fetching page {page}: {e}")
            sys.exit(1)

        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code} on page {page}")
            print(f"Response: {response.text}")
            break

        try:
            data = response.json()
        except Exception as e:
            print(f"Error parsing JSON on page {page}: {e}")
            break

        # The API may return records in different ways—check for common keys.
        if "result" in data:
            if "requests" in data["result"]:
                page_results = data["result"]["requests"]
            elif "records" in data["result"]:
                page_results = data["result"]["records"]
            else:
                page_results = []
        else:
            # If the API returns a plain list (just in case)
            page_results = data if isinstance(data, list) else []

        print(f"Fetched {len(page_results)} records on page {page}.")

        if not page_results:
            # No more data; exit the loop.
            break

        all_results.extend(page_results)

        if len(page_results) < PER_PAGE:
            # Last page (fewer than maximum per page)
            break

        page += 1

        # Sleep before the next request to keep queries below 10 per minute.
        time.sleep(SLEEP_SECONDS)

    return all_results

def main():
    print("Starting download of API data...")
    all_reports = fetch_all_reports()
    print(f"Total reports fetched: {len(all_reports)}")
    
    # Store the full data into a JSON file.
    output_filename = "reports.json"
    try:
        with open(output_filename, "w") as outfile:
            json.dump(all_reports, outfile, indent=2)
        print(f"Data successfully written to {output_filename}")
    except Exception as e:
        print(f"Error writing data to file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
