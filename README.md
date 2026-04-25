# OlmoEarth Change Overlay

This project builds district or state level change layers from **OlmoEarth embeddings** and **Sentinel-2 annual composites**, adds **WorldPop population change**, then exports:

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
- `--resolution-m 20` for a much faster coarse run (default is 10)
- `--model tiny` for the best CPU tradeoff
- `--display-aggregation 4` to keep the UI responsive
- `--skip-population` if you want the fastest possible run and do not need WorldPop
- `--skip-pollution` if you want to skip the Sentinel-2 aerosol pollution proxy
- `--skip-wards` if you want to skip OSM ward aggregation and keep only cell overlays
- `--skip-change-rasters` to skip writing per-tile change rasters and reduce I/O
- `--no-save-composites` to skip writing per-tile yearly composite GeoTIFFs

For a boundary-shaped final map, do not use `--max-tiles`. That flag intentionally processes only the top overlap tiles, which is useful for fast smoke tests but not for a complete district/state overlay.

## Speed Up Large Runs

If you are seeing multi-hour runtime (for example, 1,000+ tiles), run in stages:

1. Fast scan (quickly identify hotspots):

```bash
uv run python scripts/generate_change_data.py \
   --country IND \
   --state "Uttar Pradesh" \
   --district "Gautam Buddha Nagar" \
   --output-dir outputs/noida-fast \
   --base-year 2025 \
   --periods 1 5 \
   --model nano \
   --resolution-m 20 \
   --skip-population \
   --skip-pollution \
   --skip-wards \
   --skip-change-rasters
```

2. Balanced run (better quality with still lower runtime):

```bash
uv run python scripts/generate_change_data.py \
   --country IND \
   --state "Uttar Pradesh" \
   --district "Gautam Buddha Nagar" \
   --output-dir outputs/noida-balanced \
   --base-year 2025 \
   --periods 1 5 \
   --model tiny \
   --resolution-m 20 \
   --skip-wards
```

3. Final publication run (full quality):

```bash
uv run python scripts/generate_change_data.py \
   --country IND \
   --state "Uttar Pradesh" \
   --district "Gautam Buddha Nagar" \
   --output-dir outputs/noida-final \
   --base-year 2025 \
   --periods 1 5 10 \
   --model tiny \
   --resolution-m 10
```

## Explore The Map

Serve the output directory so the browser can fetch `summary.json` and `overlay.geojson` and optional `ward_overlay.geojson`:

```bash
cd outputs/noida
python -m http.server 8000
```

Then open `http://localhost:8000/ui/`.

Do not open `ui/index.html` directly with `file://`. That can break local `fetch(...)` calls, and OpenStreetMap's tile usage policy expects requests to include a valid HTTP `Referer`, which direct file opens do not provide.

The UI includes:

- basemap selector for OSM, light basemap, imagery, or no basemap
- analysis-unit switcher for cell overlays and ward overlays when ward boundaries are available
- metric selector for embedding shift, vegetation, water, urbanization, bare soil, pollution proxy, and population delta
- period slider for 1y / 5y / 10y comparisons
- color-scaled overlays on a Leaflet base map
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
6. Derives a pollution proxy from Sentinel-2 L2A `AOT` (aerosol optical thickness), which is
   useful for aerosol loading changes but is not direct PM2.5 or gas concentration.
7. Downloads WorldPop 1km constrained population rasters for supported years (`2015`-`2030`)
   and allocates those counts into the display cells.
8. Optionally resolves OSM ward-like administrative polygons for the selected district and
   aggregates cell metrics into ward polygons.
9. Writes overlay cells with per-period properties like:
   `embedding_change_5y`, `vegetation_delta_5y`, `water_delta_5y`, `urban_delta_5y`,
   `pollution_delta_5y`, `population_delta_5y`, plus a `ward_overlay.geojson` when ward boundaries are available.

> [!NOTE]
> the map uses **embedding L2 shift** as the main OlmoEarth change score because annual OlmoEarth vectors can stay nearly parallel across time, which makes cosine distance too flat for an interactive overlay.


## Data Attribution

- **Administrative boundaries:** [geoBoundaries](https://www.geoboundaries.org), the geoBoundaries Global Database of Political Administrative Boundaries, is used for state and district boundaries. geoBoundaries requests web attribution and distributes data under [CC BY 4.0](https://www.geoboundaries.org).

- **Satellite imagery:** annual composites are built from [Copernicus Sentinel-2 L2A](https://planetarycomputer.microsoft.com/dataset/sentinel-2-l2a) imagery accessed via Microsoft Planetary Computer. Sentinel-2 data remains subject to the [Copernicus Sentinel Data Terms and Conditions](https://dataspace.copernicus.eu/terms-and-conditions).

- **Pollution proxy:** the `pollution_delta_*` metric is derived from Sentinel-2 L2A `AOT` (aerosol optical thickness) as documented in the official [Copernicus Sentinel-2 L2A documentation](https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Data/S2L2A.html). It is an aerosol-loading proxy, not direct PM2.5, NO2, or regulatory air-quality data.

- **Population:** population overlays come from [WorldPop](https://www.worldpop.org/methods/) annual gridded population products. WorldPop's datasets are available under [CC BY 4.0](https://www.worldpop.org/faq/).

- **Ward overlays and OSM-derived layers:** ward polygons are resolved from [OpenStreetMap](https://www.openstreetmap.org/copyright) administrative data on a best-effort basis. OSM data is licensed under the [ODbL 1.0](https://www.openstreetmap.org/copyright).

- **Basemaps in the UI:** when using the built-in map baselayers, preserve the attribution shown in the UI for OpenStreetMap, CARTO, and Esri.

- **Model attribution:** embeddings are generated with [OlmoEarth / `olmoearth_pretrain`](https://github.com/allenai/olmoearth_pretrain) from Ai2.

## Caveats

- The default long-baseline `10y` request can reach back into `2015`, where Sentinel-2 coverage is not as complete as later years.
- Annual median composites suppress seasonal noise well, but they also smooth short-lived events.
- The static UI is intended for exploration; for publication you may want to add labels, annotation layers, and editorial notes.
