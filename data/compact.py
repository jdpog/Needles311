#!/usr/bin/env python3
"""
Build compact data files for the website from data/reports.json.

callAPI.py keeps writing the full, human-readable data/reports.json.  This
script derives two much smaller files from it that the page loads instead:

  data/reports.core.json  everything needed to draw the map and stats:
                          times, coordinates and (de-duplicated) addresses,
                          stored as parallel columns sorted by time.
  data/reports.text.json  descriptions and photo URLs, aligned to the same
                          row order.  The page fetches this in the background
                          after the first paint, since it is only needed for
                          popups and the keyword filter.

If this script fails, the page detects that the compact files are stale and
falls back to data/reports.json, so a failure here never breaks the site.
"""
import json
import math
import os
import sys
from datetime import datetime, timezone

SRC = os.path.join("data", "reports.json")
CORE = os.path.join("data", "reports.core.json")
TEXT = os.path.join("data", "reports.text.json")
MEDIA_PREFIX = "https://spot-boston-res.cloudinary.com/image/upload/"

# Anything outside this generous box around Boston is a geocoding error.
LAT_RANGE = (41.5, 43.0)
LNG_RANGE = (-72.0, -70.0)


def parse_time(value):
    """ISO 8601 string -> epoch seconds (int), or None if unparseable."""
    if not value or not isinstance(value, str):
        return None
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def normalize_address(addr):
    return " ".join((addr or "").split())


def main():
    with open(SRC, encoding="utf-8") as f:
        rows = json.load(f)
    if not isinstance(rows, list):
        print("reports.json is not a list", file=sys.stderr)
        return 1

    records = []
    seen = set()
    skipped = 0
    for r in rows:
        if not isinstance(r, dict):
            skipped += 1
            continue
        try:
            lat = float(r.get("lat"))
            lng = float(r.get("long"))
        except (TypeError, ValueError):
            skipped += 1
            continue
        if not (math.isfinite(lat) and math.isfinite(lng)):
            skipped += 1
            continue
        if not (LAT_RANGE[0] <= lat <= LAT_RANGE[1] and LNG_RANGE[0] <= lng <= LNG_RANGE[1]):
            skipped += 1
            continue
        ts = parse_time(r.get("requested_datetime"))
        if ts is None:
            skipped += 1
            continue
        desc = r.get("description") or ""
        media = r.get("media_url") or ""
        addr = normalize_address(r.get("address"))
        # The collector fetches 90-day windows whose edges touch, so a report
        # stamped exactly on a boundary can appear twice.  Drop exact repeats.
        key = (ts, round(lat, 6), round(lng, 6), desc, media)
        if key in seen:
            skipped += 1
            continue
        seen.add(key)
        records.append((ts, lat, lng, addr, desc, media))

    records.sort(key=lambda rec: rec[0])
    n = len(records)
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Address dictionary keyed case-insensitively; keep the first spelling seen.
    addr_index = {}
    addrs = []
    a_col = []
    for rec in records:
        k = rec[3].lower()
        idx = addr_index.get(k)
        if idx is None:
            idx = len(addrs)
            addr_index[k] = idx
            addrs.append(rec[3])
        a_col.append(idx)

    # Times as a first absolute value followed by deltas (seconds).
    t_col = []
    prev = 0
    for rec in records:
        t_col.append(rec[0] - prev)
        prev = rec[0]

    core = {
        "v": 1,
        "generated": generated,
        "n": n,
        "t": t_col,
        "lat": [round(rec[1] * 1e6) for rec in records],
        "lng": [round(rec[2] * 1e6) for rec in records],
        "a": a_col,
        "addrs": addrs,
    }

    def strip_media(url):
        return url[len(MEDIA_PREFIX):] if url.startswith(MEDIA_PREFIX) else url

    text = {
        "v": 1,
        "generated": generated,
        "n": n,
        "media_prefix": MEDIA_PREFIX,
        "d": [rec[4] for rec in records],
        "m": [strip_media(rec[5]) for rec in records],
    }

    for path, payload in ((CORE, core), (TEXT, text)):
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, separators=(",", ":"), ensure_ascii=False)
        os.replace(tmp, path)
        print(f"wrote {path} ({os.path.getsize(path):,} bytes)")

    print(f"{n:,} reports kept, {skipped:,} skipped, {len(addrs):,} unique addresses")
    return 0


if __name__ == "__main__":
    sys.exit(main())
