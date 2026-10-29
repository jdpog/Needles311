# Needles311

A visualization of Boston 311 needle reports that shows how the reports have changed both geographically and over time.

Live site: https://needles311.surge.sh (also deployed to GitHub Pages).

## How it works

- `data/callAPI.py` pulls every "Needle Pickup" request from the Boston 311 Open311 API and writes `data/reports.json`. A GitHub Actions workflow (`.github/workflows/callAPI.yml`) runs it daily.
- `data/compact.py` then derives two small files the website actually loads:
  - `data/reports.core.json` – times, coordinates and addresses as compact columns (about 1 MB gzipped).
  - `data/reports.text.json` – descriptions and photo URLs, fetched in the background after the map is already interactive.
  If the compact files are missing or stale, `index.html` falls back to `reports.json` automatically.
- `index.html` is the whole site: Leaflet map with a canvas point layer, optional heatmap, timeline playback, address search and a yearly chart. No build step.

Pushes to `main` deploy to surge (`deploy.yml`) and GitHub Pages (`pages_deploy.yml`).

## Base maps

CARTO's Positron / Dark Matter tiles require an API key since September 2026. The page ships without one and uses Esri's gray canvas for the Light and Dark layers instead (crisp up to zoom 16). To get the CARTO maps back, request a free non-commercial key at https://carto.com/basemaps/apikey/ and paste it into `CARTO_API_KEY` near the top of the script in `index.html`.

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
