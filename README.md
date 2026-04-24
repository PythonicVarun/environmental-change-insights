# OlmoEarth Change Overlay

This project builds district or state level change layers from **OlmoEarth embeddings** and **Sentinel-2 annual composites**, then exports:

- `overlay.geojson` for map overlays
- `summary.json` for downstream apps or newsroom pipelines
- `report.md` for a quick written brief
- `ui/` static files for an interactive Leaflet overlay

The generator is designed around **India** by default because the request was framed in terms of `state` and `district`, but the boundary lookup itself works with any `geoBoundaries` ISO3 country code that has ADM1 and ADM2 coverage.

## Setup

```bash
uv sync
```

## Generate One District

```bash
uv run python scripts/generate_change_data.py \
  --country IND \
  --state "Uttar Pradesh" \
  --district "Gautam Buddha Nagar" \
  --output-dir outputs/noida \
  --base-year 2025 \
  --periods 1 5 10
```

## Single Colab Script

If you want one script to run directly from Colab, use:

```bash
python scripts/colab_generate_data.py \
  --state "Uttar Pradesh" \
  --district "Gautam Buddha Nagar" \
  --output-dir /content/outputs/noida \
  --periods 1 5 \
  --max-tiles 1 \
  --zip-output
```

Notes:

- it bootstraps missing Python packages automatically unless you pass `--skip-install`
- it can use `--device auto` to pick GPU on Colab when available
- with `--zip-output`, it creates a downloadable archive next to the output folder

Useful runtime controls:

- `--max-tiles 1` for a quick pilot run only
- `--tile-size-m 1280` or `2560` to control area per tile
- `--model tiny` for the best CPU tradeoff
- `--display-aggregation 4` to keep the UI responsive

For a boundary-shaped final map, do not use `--max-tiles`. That flag intentionally processes only the top overlap tiles, which is useful for fast smoke tests but not for a complete district/state overlay.

## Explore The Map

Serve the output directory so the browser can fetch `summary.json` and `overlay.geojson`:

```bash
cd outputs/noida
python -m http.server 8000
```

Then open `http://localhost:8000/ui/`.

Do not open `ui/index.html` directly with `file://`. That can break local `fetch(...)` calls, and OpenStreetMap's tile usage policy expects requests to include a valid HTTP `Referer`, which direct file opens do not provide.

The UI includes:

- metric selector for embedding shift, vegetation, water, urbanization, and bare soil
- period slider for 1y / 5y / 10y comparisons
- color-scaled overlay cells on a Leaflet base map
- hotspot cards sourced from the generated summary

If you want to suppress the basemap intentionally, open:

```text
http://localhost:8000/ui/?basemap=none
```

## Multi-Location Pilot Scan

```bash
uv run python scripts/run_india_news_scan.py \
  --config configs/target.json \
  --output-dir outputs/india-news-scan \
  --max-tiles 1
```

That script keeps the run intentionally small and is meant for newsroom scouting or rapid prototyping. For a full district analysis, run `generate_change_data.py` without `--max-tiles`.

## What The Pipeline Computes

For each requested year snapshot, the pipeline:

1. Resolves the target district/state boundary from geoBoundaries.
2. Tiles the area in the local UTM CRS.
3. Downloads a cloud-median Sentinel-2 L2A annual composite from Microsoft Planetary Computer.
4. Computes OlmoEarth embeddings locally with the open `olmoearth-pretrain` model.
5. Derives interpretable spectral deltas:
   `NDVI`, `MNDWI`, `NDBI`, and `BSI`.
6. Writes overlay cells with per-period properties like:
   `embedding_change_5y`, `vegetation_delta_5y`, `water_delta_5y`, `urban_delta_5y`.

> [!NOTE]
> the map uses **embedding L2 shift** as the main OlmoEarth change score because annual OlmoEarth vectors can stay nearly parallel across time, which makes cosine distance too flat for an interactive overlay.

## Caveats

- The default long-baseline `10y` request can reach back into `2015`, where Sentinel-2 coverage is not as complete as later years.
- Annual median composites suppress seasonal noise well, but they also smooth short-lived events.
- The static UI is intended for exploration; for publication you may want to add labels, annotation layers, and editorial notes.
