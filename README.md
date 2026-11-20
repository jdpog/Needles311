# Needles311

A visualization of Boston 311 needle reports that shows how the reports have changed both geographically and over time.

Live site: https://needles311.surge.sh (also deployed to GitHub Pages).

## How it works

- `data/callAPI.py` pulls every "Needle Pickup" request from the Boston 311 Open311 API and writes `data/reports.json`. Boston switched 311 systems in September 2026, so it queries both the legacy service code (reports up to 2026-09-09) and the current one (`SERVICES` at the top of the script). A GitHub Actions workflow (`.github/workflows/callAPI.yml`) runs it daily.
- `data/compact.py` then derives two small files the website actually loads:
  - `data/reports.core.json` – times, coordinates and addresses as compact columns (about 1 MB gzipped).
  - `data/reports.text.json` – descriptions and photo URLs, fetched in the background after the map is already interactive.
  If the compact files are missing or stale, `index.html` falls back to `reports.json` automatically.
- `index.html` is the whole site: Leaflet map with a canvas point layer, optional heatmap, timeline playback, address search and a yearly chart. No build step.

Pushes to `main` deploy to surge (`deploy.yml`) and GitHub Pages (`pages_deploy.yml`).

## Base maps

CARTO's Positron / Dark Matter tiles require an API key since September 2026 (free for non-commercial use, 5 million tile requests a month: https://carto.com/basemaps/apikey/). The key lives in `CARTO_API_KEY` near the top of the script in `index.html`. Like any basemap key it is visible to visitors, so restrict it to this site's domains in the CARTO dashboard if you want to protect the quota. If the key is empty, or CARTO rejects it (quota spent, key revoked, origin not allowed), the Light and Dark layers fall back automatically to Esri's gray canvas (crisp up to zoom 16).

Other layers: Streets (OpenStreetMap), B&W (MapTiler Toner, key in `index.html`), Satellite (Esri World Imagery).

## Local preview

Any static server works, e.g.

```bash
python -m http.server 8787
```

then open http://localhost:8787/. To rebuild the compact data after changing `reports.json`:

```bash
python data/compact.py
```
