#!/usr/bin/env python3
import requests
import time
import json
import sys
import os
from datetime import datetime, timedelta

# API parameters
#
# Boston replaced its 311 system in September 2026. Reports filed before the
# switch are only returned under the legacy service code; reports filed after
# it (from 2026-09-10 on) only under the new one. Both are fetched, each from
# the first date worth querying.
SERVICES = [
    ("Mayor's 24 Hour Hotline:Needle Program:Needle Pickup", datetime(2015, 1, 1)),  # legacy system
    ("d29084c4-45ef-42a7-b431-e0d8e5e773c3",                 datetime(2026, 9, 1)),  # "Needle Pickup" in the new system
]
API_ENDPOINT   = "https://311.boston.gov/open311/v2/requests.json"
PER_PAGE       = 100          # Maximum allowed per request
SLEEP_SECONDS  = 8            # Pause between API calls (<10/minute)

# User-Agent header
HEADERS = {"User-Agent": "BostonNeedleReportsDownloader/1.0"}

def fetch_reports_for_date_range(service_code, start_date_param, end_date_param, page=1):
    params = {
        "service_code": service_code,
        "per_page": PER_PAGE,
        "page": page,
        "start_date": start_date_param,
        "end_date": end_date_param
    }
    print(f"Fetching page {page} for interval {start_date_param} → {end_date_param}")
    resp = requests.get(API_ENDPOINT, params=params, headers=HEADERS)
    if resp.status_code != 200:
        print(f"Error: status {resp.status_code} on page {page}")
        print(resp.text)
        sys.exit(1)
    data = resp.json()
    # API may wrap in {"result": {...}}
    if isinstance(data, dict) and "result" in data:
        records = data["result"].get("requests") or data["result"].get("records") or []
    else:
        records = data if isinstance(data, list) else []
    return records

def fetch_reports_for_range(service_code, start_date_param, end_date_param):
    all = []
    page = 1
    while True:
        batch = fetch_reports_for_date_range(service_code, start_date_param, end_date_param, page=page)
        print(f"  → fetched {len(batch)} records on page {page}")
        if not batch:
            break
        all.extend(batch)
        if len(batch) < PER_PAGE:
            break
        page += 1
        time.sleep(SLEEP_SECONDS)
    return all

def main():
    overall_end    = datetime.now()
    interval_delta = timedelta(days=90)
    raw_reports    = []
    seen_ids       = set()

    for service_code, overall_start in SERVICES:
        print(f"\n=== Service code {service_code!r}: {overall_start.isoformat()} → {overall_end.isoformat()}")
        current_start = overall_start

        while current_start < overall_end:
            current_end = min(current_start + interval_delta, overall_end)
            start_str   = current_start.isoformat() + "Z"
            end_str     = current_end.isoformat()   + "Z"
            print(f"\nInterval {start_str} → {end_str}")
            batch = fetch_reports_for_range(service_code, start_str, end_str)
            added = 0
            for r in batch:
                # Consecutive intervals share a boundary instant, so a report
                # can come back twice; keep the first copy.
                rid = r.get("service_request_id")
                if rid:
                    if rid in seen_ids:
                        continue
                    seen_ids.add(rid)
                raw_reports.append(r)
                added += 1
            print(f" Interval total: {len(batch)} records ({added} new)")
            current_start = current_end
            time.sleep(SLEEP_SECONDS)

    print(f"\nTotal raw reports fetched: {len(raw_reports)}")

    # --- filter down to only the six fields we want ---
    filtered = []
    for r in raw_reports:
        filtered.append({
            "description":         r.get("description", ""),
            "requested_datetime":  r.get("requested_datetime", ""),
            "address":             r.get("address", ""),
            "lat":                 r.get("lat", ""),
            "long":                r.get("long", ""),
            "media_url":           r.get("media_url", "")
        })

    # ensure output directory
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "reports.json")

    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(filtered, f, indent=2, ensure_ascii=False)
        print(f"Filtered data written ({len(filtered)} records) to {output_file}")
    except Exception as e:
        print(f"Error writing to {output_file}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
