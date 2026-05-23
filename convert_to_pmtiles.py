#!/usr/bin/env python3
"""Convert GeoJSON overlays to PMTiles for all city outputs.

Usage:
    python convert_to_pmtiles.py [--city CITY] [--min-zoom N] [--max-zoom N]

Outputs per city:
    overlay.pmtiles + overlay.meta.json
    ward_overlay.pmtiles + ward_overlay.meta.json  (if ward_overlay.geojson exists)

Large PMTiles (>50 MB) are uploaded to a new GitHub release.
"""

import argparse
import gzip
import json
import math
import os
import subprocess
import sys
from pathlib import Path

import geopandas as gpd
from shapely.affinity import affine_transform
from shapely.geometry import box
import mapbox_vector_tile
from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import Writer

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_MIN_ZOOM = 7
DEFAULT_MAX_ZOOM = 12
LARGE_FILE_MB = 50.0
GH_REPO = "PythonicVarun/olmoearth-change-insights"
RELEASE_TAG_GEOJSON = "assets-2026-05-19"
NEW_RELEASE_TAG = "pmtiles-overlays-2026-05-23"

# Cities whose overlay.geojson lives in a GitHub release (not committed locally)
RELEASE_CITIES = {
    "chengalputtu": f"{RELEASE_TAG_GEOJSON}/chengalputtu-overlay.geojson",
    "jaipur": f"{RELEASE_TAG_GEOJSON}/jaipur-overlay.geojson",
    "kanpur_nagar": f"{RELEASE_TAG_GEOJSON}/kanpur_nagar-overlay.geojson",
    "pune": f"{RELEASE_TAG_GEOJSON}/pune-overlay.geojson",
    "surat": f"{RELEASE_TAG_GEOJSON}/surat-overlay.geojson",
}


# ── Tile math ──────────────────────────────────────────────────────────────────

def lon_lat_to_tile(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    n = 1 << zoom
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n)
    return (max(0, min(x, n - 1)), max(0, min(y, n - 1)))


def tile_bounds_wgs84(tx: int, ty: int, zoom: int) -> tuple[float, float, float, float]:
    """Return (west, south, east, north) in WGS84 for tile (tx, ty, zoom)."""
    n = 1 << zoom
    west = tx / n * 360.0 - 180.0
    east = (tx + 1) / n * 360.0 - 180.0
    north = math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * ty / n))))
    south = math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * (ty + 1) / n))))
    return (west, south, east, north)


def project_to_tile(geom, west: float, south: float, east: float, north: float, extents: int = 4096):
    """Affine-project WGS84 geometry into tile pixel space [0, extents]."""
    sx = extents / (east - west)
    sy = extents / (north - south)
    # x → (lon - west) * sx   (west=0, east=extents)
    # y → (north - lat) * sy  (north=0, south=extents — Y-flip)
    return affine_transform(geom, [sx, 0, 0, -sy, -west * sx, north * sy])


# ── Property helpers ──────────────────────────────────────────────────────────

def clean_props(row_dict: dict) -> dict:
    """Strip None/NaN and convert numpy scalars to Python natives."""
    result = {}
    for k, v in row_dict.items():
        if k == "geometry":
            continue
        if v is None:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        if hasattr(v, "item"):          # numpy scalar → Python native
            v = v.item()
        if isinstance(v, float) and math.isnan(v):
            continue
        result[k] = v
    return result


def compute_bounds_by_property(gdf) -> dict:
    bounds = {}
    for col in gdf.columns:
        if col == "geometry":
            continue
        try:
            numeric = (
                gdf[col]
                .apply(lambda v: float(v) if v is not None and not (isinstance(v, float) and math.isnan(v)) else None)
                .dropna()
            )
            if len(numeric) > 0:
                bounds[col] = {"min": float(numeric.min()), "max": float(numeric.max())}
        except (TypeError, ValueError):
            pass
    return bounds


# ── Core converter ─────────────────────────────────────────────────────────────

def convert_geojson_to_pmtiles(
    geojson_path: Path,
    pmtiles_path: Path,
    meta_path: Path,
    layer_name: str = "overlay",
    min_zoom: int = DEFAULT_MIN_ZOOM,
    max_zoom: int = DEFAULT_MAX_ZOOM,
) -> float:
    """Convert a GeoJSON file to PMTiles + meta.json. Returns file size in MB."""
    print(f"    Reading {geojson_path.name} …", flush=True)
    gdf = gpd.read_file(str(geojson_path))
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")

    # Reset index so row indices are 0-based sequential integers (used as feature IDs)
    gdf = gdf.reset_index(drop=True)

    bbox = gdf.total_bounds   # [minx, miny, maxx, maxy]
    print(f"    {len(gdf):,} features  bbox [{bbox[0]:.3f},{bbox[1]:.3f},{bbox[2]:.3f},{bbox[3]:.3f}]")

    # ── Write meta.json ──────────────────────────────────────────────────────
    meta = {
        "bbox": [float(v) for v in bbox],
        "center": [float((bbox[0] + bbox[2]) / 2), float((bbox[1] + bbox[3]) / 2)],
        "featureCount": int(len(gdf)),
        "boundsByProperty": compute_bounds_by_property(gdf),
    }
    meta_path.write_text(json.dumps(meta, indent=2))
    print(f"    Written {meta_path.name}")

    # Pre-extract geometry bounds for fast inside-tile check
    geom_bounds = gdf.geometry.bounds  # DataFrame: minx miny maxx maxy

    # ── Write PMTiles ────────────────────────────────────────────────────────
    with open(str(pmtiles_path), "wb") as f:
        writer = Writer(f)
        grand_total = 0

        for zoom in range(min_zoom, max_zoom + 1):
            x_min, y_min = lon_lat_to_tile(bbox[0], bbox[3], zoom)  # NW
            x_max, y_max = lon_lat_to_tile(bbox[2], bbox[1], zoom)  # SE
            tile_count = (x_max - x_min + 1) * (y_max - y_min + 1)
            print(f"    z{zoom}: {tile_count} candidate tiles …", end="", flush=True)
            written = 0

            # Minimum projected area to include (< 0.5 px² skipped)
            tile_deg_w = 360.0 / (1 << zoom)
            tile_deg_h = tile_deg_w  # approx for mid-latitudes
            px_per_deg = 4096.0 / tile_deg_w
            min_area = (0.5 / px_per_deg) ** 2

            for tx in range(x_min, x_max + 1):
                for ty in range(y_min, y_max + 1):
                    west, south, east, north = tile_bounds_wgs84(tx, ty, zoom)
                    tile_box = box(west, south, east, north)

                    candidates = list(gdf.sindex.query(tile_box))
                    if not candidates:
                        continue

                    subset = gdf.iloc[candidates]
                    intersecting = subset[subset.geometry.intersects(tile_box)]
                    if intersecting.empty:
                        continue

                    # Get pre-computed bounds for fast inside-tile check
                    sub_bounds = geom_bounds.iloc[intersecting.index]

                    features = []
                    # itertuples is ~100× faster than iterrows for large DataFrames
                    for row in intersecting.itertuples():
                        geom = row.geometry
                        if geom is None or geom.is_empty:
                            continue

                        # Fast bounds-based check to skip clipping when fully inside
                        gb = geom_bounds.iloc[row.Index]
                        if (gb.minx >= west and gb.miny >= south and
                                gb.maxx <= east and gb.maxy <= north):
                            clipped = geom  # fully inside tile, no clip needed
                        else:
                            clipped = geom.intersection(tile_box)
                            if clipped.is_empty:
                                continue

                        # Skip sub-pixel features at coarse zooms
                        if clipped.area < min_area:
                            continue

                        projected = project_to_tile(clipped, west, south, east, north)
                        props = clean_props(row._asdict())
                        features.append({
                            "geometry": projected.wkt,
                            "properties": props,
                            "id": row.Index,  # sequential integer → enables MapLibre feature-state
                        })

                    if not features:
                        continue

                    tile_bytes = mapbox_vector_tile.encode({"name": layer_name, "features": features})
                    writer.write_tile(zxy_to_tileid(zoom, tx, ty), gzip.compress(tile_bytes))
                    written += 1

            grand_total += written
            print(f" {written} non-empty")

        print(f"    Total tiles written: {grand_total}")

        header = {
            "tile_type": TileType.MVT,
            "tile_compression": Compression.GZIP,
            "min_zoom": min_zoom,
            "max_zoom": max_zoom,
            "min_lon_e7": int(bbox[0] * 1e7),
            "min_lat_e7": int(bbox[1] * 1e7),
            "max_lon_e7": int(bbox[2] * 1e7),
            "max_lat_e7": int(bbox[3] * 1e7),
            "center_zoom": min_zoom + (max_zoom - min_zoom) // 2,
            "center_lon_e7": int((bbox[0] + bbox[2]) / 2 * 1e7),
            "center_lat_e7": int((bbox[1] + bbox[3]) / 2 * 1e7),
        }
        writer.finalize(header, {"attribution": "OlmoEarth", "name": layer_name, "format": "pbf"})

    size_mb = pmtiles_path.stat().st_size / 1024 / 1024
    print(f"    Written {pmtiles_path.name} ({size_mb:.2f} MB)")
    return size_mb


# ── GitHub helpers ─────────────────────────────────────────────────────────────

def gh_download_geojson(asset_path: str, dest: Path) -> None:
    """Download a release asset via gh CLI."""
    tag, filename = asset_path.split("/", 1)
    print(f"    Downloading {filename} from release {tag} …", flush=True)
    result = subprocess.run(
        ["gh", "release", "download", tag,
         "--repo", GH_REPO,
         "--pattern", filename,
         "--dir", str(dest.parent),
         "--clobber"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"gh download failed: {result.stderr.strip()}")
    # gh downloads with the original filename; rename if needed
    downloaded = dest.parent / filename
    if downloaded != dest:
        downloaded.rename(dest)
    print(f"    Downloaded → {dest.name}")


def gh_upload_release(tag: str, files: list[Path], script_dir: Path) -> None:
    """Create/update a GitHub release and upload files."""
    # Check if release exists
    check = subprocess.run(
        ["gh", "release", "view", tag, "--repo", GH_REPO],
        capture_output=True, text=True,
    )
    if check.returncode != 0:
        subprocess.run(
            ["gh", "release", "create", tag,
             "--repo", GH_REPO,
             "--title", "PMTiles Overlays",
             "--notes", "Large PMTiles overlay files (>50 MB). Auto-generated."],
            cwd=str(script_dir), check=True,
        )
    for path in files:
        print(f"    Uploading {path.name} ({path.stat().st_size / 1024 / 1024:.1f} MB) …")
        subprocess.run(
            ["gh", "release", "upload", tag, str(path), "--repo", GH_REPO, "--clobber"],
            cwd=str(script_dir), check=True,
        )
        print(f"    ✓ {path.name}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--city", help="Convert only this city slug")
    parser.add_argument("--min-zoom", type=int, default=DEFAULT_MIN_ZOOM)
    parser.add_argument("--max-zoom", type=int, default=DEFAULT_MAX_ZOOM)
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    outputs_dir = script_dir / "outputs"

    large_pmtiles: list[tuple[str, Path]] = []  # (city-layer label, path)
    url_map: dict[str, dict] = {}               # city → {overlay_url, ward_overlay_url}

    city_dirs = sorted(d for d in outputs_dir.iterdir() if d.is_dir())
    if args.city:
        city_dirs = [d for d in city_dirs if d.name == args.city]
        if not city_dirs:
            sys.exit(f"City '{args.city}' not found in {outputs_dir}")

    for city_dir in city_dirs:
        city = city_dir.name
        print(f"\n{'─'*60}")
        print(f"  {city}")
        print(f"{'─'*60}")

        url_map[city] = {}

        # ── overlay ────────────────────────────────────────────────────────
        overlay_src = None
        temp_geojson = None

        if city in RELEASE_CITIES:
            temp_geojson = city_dir / "_overlay_dl.geojson"
            try:
                gh_download_geojson(RELEASE_CITIES[city], temp_geojson)
                overlay_src = temp_geojson
            except RuntimeError as e:
                print(f"    WARNING: {e} — skipping overlay")
        elif (city_dir / "overlay.geojson").exists():
            overlay_src = city_dir / "overlay.geojson"
        else:
            print("    No overlay.geojson — skipping")

        if overlay_src:
            pmtiles_path = city_dir / "overlay.pmtiles"
            meta_path = city_dir / "overlay.meta.json"
            size_mb = convert_geojson_to_pmtiles(
                overlay_src, pmtiles_path, meta_path,
                layer_name="overlay",
                min_zoom=args.min_zoom, max_zoom=args.max_zoom,
            )
            if size_mb > LARGE_FILE_MB:
                large_pmtiles.append((f"{city}/overlay", pmtiles_path))
                url_map[city]["overlay_pmtiles"] = f"RELEASE:{pmtiles_path.name}"
            else:
                url_map[city]["overlay_pmtiles"] = "overlay.pmtiles"

            if temp_geojson and temp_geojson.exists():
                temp_geojson.unlink()

        # ── ward_overlay ───────────────────────────────────────────────────
        ward_src = city_dir / "ward_overlay.geojson"
        if ward_src.exists():
            pmtiles_path = city_dir / "ward_overlay.pmtiles"
            meta_path = city_dir / "ward_overlay.meta.json"
            size_mb = convert_geojson_to_pmtiles(
                ward_src, pmtiles_path, meta_path,
                layer_name="ward_overlay",
                min_zoom=args.min_zoom, max_zoom=args.max_zoom,
            )
            if size_mb > LARGE_FILE_MB:
                large_pmtiles.append((f"{city}/ward_overlay", pmtiles_path))
                url_map[city]["ward_pmtiles"] = f"RELEASE:{pmtiles_path.name}"
            else:
                url_map[city]["ward_pmtiles"] = "ward_overlay.pmtiles"

    # ── Upload large files ─────────────────────────────────────────────────
    if large_pmtiles:
        print(f"\n{'='*60}")
        print(f"  Uploading {len(large_pmtiles)} large PMTiles to GitHub release: {NEW_RELEASE_TAG}")
        print(f"{'='*60}")
        gh_upload_release(NEW_RELEASE_TAG, [p for _, p in large_pmtiles], script_dir)

    # ── Summary ────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("  Conversion complete. URL map:")
    print(f"{'='*60}")
    for city, urls in url_map.items():
        if urls:
            print(f"\n  {city}:")
            for key, val in urls.items():
                if val.startswith("RELEASE:"):
                    fname = val.removeprefix("RELEASE:")
                    release_url = f"https://github.com/{GH_REPO}/releases/download/{NEW_RELEASE_TAG}/{fname}"
                    print(f"    {key}: {release_url}")
                else:
                    print(f"    {key}: {val}  (local)")


if __name__ == "__main__":
    main()
