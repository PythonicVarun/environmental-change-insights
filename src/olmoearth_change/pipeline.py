import warnings

warnings.filterwarnings("ignore", message=".*olmo-core not installed.*")
warnings.filterwarnings("ignore", message=".*helios.*renamed to.*olmoearth_pretrain.*")

import json
import math
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import geopandas as gpd
import numpy as np
import planetary_computer
import rasterio
import rasterio.fill
import stackstac
import torch
from affine import Affine
from geopandas import GeoDataFrame
from olmoearth_pretrain.data.constants import Modality
from olmoearth_pretrain.data.normalize import Normalizer, Strategy
from olmoearth_pretrain.datatypes import MaskedOlmoEarthSample, MaskValue
from olmoearth_pretrain.model_loader import ModelID, load_model_from_id
from pystac_client import Client
from rasterio.transform import from_origin
from rasterio.windows import Window, from_bounds
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry
from tqdm import tqdm

from .boundaries import (
    ResolvedBoundary,
    resolve_admin_boundary,
    resolve_ward_boundaries,
)

S2_BANDS = [
    "B02",
    "B03",
    "B04",
    "B08",
    "B05",
    "B06",
    "B07",
    "B8A",
    "B11",
    "B12",
    "B01",
    "B09",
]

BLUE_IDX = 0
GREEN_IDX = 1
RED_IDX = 2
NIR_IDX = 3
SWIR1_IDX = 8
SWIR2_IDX = 9

WORLDPOP_MIN_YEAR = 2015
WORLDPOP_MAX_YEAR = 2030
WORLDPOP_RELEASE = "R2025A"
WORLDPOP_VERSION = "v1"
WORLDPOP_SOURCE_LABEL = "WorldPop Global 2 constrained 1km population counts"
WORLDPOP_CITATION_URL = "https://www.worldpop.org/methods/populations/"
WORLDPOP_LICENSE_URL = "https://creativecommons.org/licenses/by/4.0/"
POLLUTION_SOURCE_LABEL = "Sentinel-2 L2A AOT aerosol optical thickness proxy"
POLLUTION_CITATION_URL = (
    "https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Data/S2L2A.html"
)


@dataclass(frozen=True)
class AnalysisConfig:
    country_iso3: str = "IND"
    state_name: str | None = None
    district_name: str | None = None
    output_dir: Path = Path("outputs/latest")
    cache_dir: Path = Path(".cache")
    model_name: str = "tiny"
    base_year: int = date.today().year - 1
    periods: tuple[int, ...] = (1, 5, 10)
    tile_size_m: int = 2_560
    resolution_m: int = 10
    patch_size: int = 4
    crop_size: int = 128
    display_aggregation: int = 4
    cloud_max: int = 40
    fill_holes_pixels: int = 48
    max_tiles: int | None = None
    workers: int = 1
    device: str = "auto"
    include_population: bool = True
    include_pollution: bool = True
    include_ward_overlay: bool = True
    save_composites: bool = True
    save_embedding_change_rasters: bool = True

    def __post_init__(self) -> None:
        if self.workers < 1:
            raise ValueError("workers must be >= 1.")
        if self.tile_size_m % self.resolution_m != 0:
            raise ValueError("tile_size_m must be divisible by resolution_m.")
        if (self.tile_size_m // self.resolution_m) % self.crop_size != 0:
            raise ValueError(
                "tile pixel size must be divisible by crop_size. Adjust tile_size_m or crop_size."
            )
        if self.crop_size % self.patch_size != 0:
            raise ValueError("crop_size must be divisible by patch_size.")


@dataclass
class TileYearData:
    tile_id: str
    year: int
    transform: Affine
    embedding_transform: Affine
    display_transform: Affine
    crs: str
    embeddings: np.ndarray
    ndvi_display: np.ndarray
    mndwi_display: np.ndarray
    ndbi_display: np.ndarray
    bsi_display: np.ndarray
    population_display: np.ndarray | None
    pollution_display: np.ndarray | None
    scene_count: int
    cache_hit: bool = False


@dataclass(frozen=True)
class TileTask:
    tile_id: str
    geometry: BaseGeometry


def run_analysis(config: AnalysisConfig) -> dict[str, Any]:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    boundary = resolve_admin_boundary(
        country_iso3=config.country_iso3,
        cache_dir=config.cache_dir,
        state_name=config.state_name,
        district_name=config.district_name,
    )
    metadata = {
        "label": boundary.label,
        "country_iso3": boundary.country_iso3,
        "admin_level": boundary.admin_level,
        "state_name": boundary.state_name,
        "district_name": boundary.district_name,
        "area_sq_km": boundary.area_sq_km,
        "generated_at_utc": date.today().strftime("%Y-%m-%d"),
    }
    if config.include_population:
        metadata["population_source"] = WORLDPOP_SOURCE_LABEL
        metadata["population_citation_url"] = WORLDPOP_CITATION_URL
        metadata["population_license_url"] = WORLDPOP_LICENSE_URL
    if config.include_pollution:
        metadata["pollution_source"] = POLLUTION_SOURCE_LABEL
        metadata["pollution_citation_url"] = POLLUTION_CITATION_URL
        metadata["pollution_note"] = (
            "Pollution uses Sentinel-2 aerosol optical thickness as an annual aerosol proxy, "
            "not direct PM2.5 or gas concentration."
        )
    write_boundary_geojson(boundary, config.output_dir / "boundary.geojson")

    tiles = build_tiles(boundary, config)
    required_years = sorted(
        {config.base_year, *[config.base_year - period for period in config.periods]}
    )
    year_results: dict[int, dict[str, TileYearData]] = {
        year: {} for year in required_years
    }
    cache_hits = 0
    cache_misses = 0
    tile_rows = [
        TileTask(tile_id=str(tile_id), geometry=geometry)
        for tile_id, geometry in zip(
            tiles["tile_id"].astype(str).tolist(),
            tiles.geometry.tolist(),
            strict=False,
        )
    ]
    tile_crs = str(tiles.crs)

    requested_workers = min(config.workers, max(1, len(tile_rows)))
    resolved_device = resolve_torch_device(config.device)
    if resolved_device.type == "cuda" and requested_workers > 1:
        metadata["workers_note"] = (
            "workers>1 was requested, but CUDA execution currently runs with workers=1 "
            "to avoid GPU contention."
        )
        requested_workers = 1

    if resolved_device.type == "cpu" and requested_workers > 1:
        cpu_count = os_cpu_count() or requested_workers
        torch_threads = max(1, cpu_count // requested_workers)
        os.environ["OLMOEARTH_TORCH_NUM_THREADS"] = str(torch_threads)
        metadata["workers"] = int(requested_workers)
        metadata["torch_threads_per_worker"] = int(torch_threads)
    else:
        os.environ.pop("OLMOEARTH_TORCH_NUM_THREADS", None)

    for year in required_years:
        if requested_workers == 1:
            for tile in tqdm(
                tile_rows,
                total=len(tile_rows),
                desc=f"Processing {year}",
            ):
                tile_result = process_tile_year(
                    tile_id=tile.tile_id,
                    tile_geometry=tile.geometry,
                    tile_crs=tile_crs,
                    year=year,
                    boundary=boundary,
                    config=config,
                )
                if tile_result is not None:
                    year_results[year][tile.tile_id] = tile_result
                    if tile_result.cache_hit:
                        cache_hits += 1
                    else:
                        cache_misses += 1
            continue

        with ThreadPoolExecutor(max_workers=requested_workers) as executor:
            future_to_tile_id = {
                executor.submit(
                    process_tile_year,
                    tile_id=tile.tile_id,
                    tile_geometry=tile.geometry,
                    tile_crs=tile_crs,
                    year=year,
                    boundary=boundary,
                    config=config,
                ): tile.tile_id
                for tile in tile_rows
            }
            for future in tqdm(
                as_completed(future_to_tile_id),
                total=len(future_to_tile_id),
                desc=f"Processing {year} ({requested_workers} workers)",
            ):
                tile_id = future_to_tile_id[future]
                tile_result = future.result()
                if tile_result is not None:
                    year_results[year][tile_id] = tile_result
                    if tile_result.cache_hit:
                        cache_hits += 1
                    else:
                        cache_misses += 1

    metadata["tile_display_cache_hits"] = int(cache_hits)
    metadata["tile_display_cache_misses"] = int(cache_misses)
    cache_total = cache_hits + cache_misses
    metadata["tile_display_cache_hit_rate"] = (
        round(100.0 * cache_hits / cache_total, 2) if cache_total else 0.0
    )

    overlay = build_overlay(boundary, tiles, year_results, config)
    overlay_path = config.output_dir / "overlay.geojson"
    overlay.to_file(overlay_path, driver="GeoJSON")
    ward_overlay_path = config.output_dir / "ward_overlay.geojson"
    coverage_sq_km = overlay_coverage_sq_km(overlay)
    metadata["coverage_sq_km"] = round(coverage_sq_km, 3)
    metadata["coverage_percent"] = round(
        100.0 * coverage_sq_km / max(boundary.area_sq_km, 1e-9),
        3,
    )
    metadata["tile_count"] = int(len(tiles))
    metadata["partial_coverage"] = bool(
        config.max_tiles is not None and metadata["coverage_percent"] < 99.0
    )
    ward_overlay = None
    if config.include_ward_overlay and boundary.district_name:
        try:
            ward_overlay = build_ward_overlay(boundary, overlay, config)
        except Exception as exc:
            metadata["ward_overlay_error"] = str(exc)
            ward_overlay = None
    if ward_overlay is not None and not ward_overlay.empty:
        ward_overlay.to_file(ward_overlay_path, driver="GeoJSON")
        metadata["ward_overlay_available"] = True
        metadata["ward_count"] = int(len(ward_overlay))
    else:
        ward_overlay_path.unlink(missing_ok=True)
        metadata["ward_overlay_available"] = False
        metadata["ward_count"] = 0

    summary = build_summary(metadata, overlay, year_results, config)
    (config.output_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    (config.output_dir / "report.md").write_text(render_report(summary))

    copy_ui_bundle(config.output_dir)
    return summary


def build_tiles(boundary: ResolvedBoundary, config: AnalysisConfig) -> GeoDataFrame:
    utm_crs = utm_crs_for_geometry(boundary.geometry)
    boundary_proj = gpd.GeoDataFrame(
        [{"geometry": boundary.geometry}],
        crs="EPSG:4326",
    ).to_crs(utm_crs)

    xmin, ymin, xmax, ymax = boundary_proj.total_bounds
    tile_size = config.tile_size_m
    x_steps = math.ceil((xmax - xmin) / tile_size)
    y_steps = math.ceil((ymax - ymin) / tile_size)

    rows: list[dict[str, Any]] = []
    tile_index = 0
    for yi in range(y_steps):
        for xi in range(x_steps):
            left = xmin + xi * tile_size
            bottom = ymin + yi * tile_size
            geom = box(left, bottom, left + tile_size, bottom + tile_size)
            overlap = geom.intersection(boundary_proj.geometry.iloc[0])
            if overlap.is_empty:
                continue
            rows.append(
                {
                    "tile_id": f"tile_{tile_index:03d}",
                    "geometry": geom,
                    "overlap_area_sq_m": overlap.area,
                }
            )
            tile_index += 1

    tiles = gpd.GeoDataFrame(rows, crs=utm_crs)
    if tiles.empty:
        raise RuntimeError("No tiles were generated for the requested boundary.")

    tiles = tiles.sort_values("overlap_area_sq_m", ascending=False).reset_index(
        drop=True
    )
    if config.max_tiles is not None:
        tiles = tiles.head(config.max_tiles).copy()
    return tiles


def process_tile_year(
    *,
    tile_id: str,
    tile_geometry: BaseGeometry,
    tile_crs: str,
    year: int,
    boundary: ResolvedBoundary,
    config: AnalysisConfig,
) -> TileYearData | None:
    year_dir = config.output_dir / "years" / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)
    composite_path = year_dir / f"{tile_id}_composite.tif"
    pollution_path = year_dir / f"{tile_id}_pollution_proxy.tif"
    processed_cache_path = year_dir / f"{tile_id}_processed_display.npz"
    processed_cache_meta_path = year_dir / f"{tile_id}_processed_display.meta.json"

    expected_cache_context = build_tile_processed_cache_context(
        tile_id=tile_id,
        tile_geometry=tile_geometry,
        year=year,
        boundary=boundary,
        config=config,
        composite_path=composite_path,
        pollution_path=pollution_path,
    )
    cached_tile = load_tile_year_cache(
        cache_path=processed_cache_path,
        cache_meta_path=processed_cache_meta_path,
        expected_context=expected_cache_context,
    )
    if cached_tile is not None:
        return cached_tile

    fetch_raster = True
    if composite_path.exists():
        try:
            composite, transform, crs = read_raster(composite_path)
            scene_count = int(read_tags(composite_path).get("scene_count", "0"))
            fetch_raster = False
        except:
            composite_path.unlink(missing_ok=True)

    if fetch_raster:
        composite_data = fetch_sentinel2_composite(
            tile_geometry=tile_geometry,
            tile_crs=tile_crs,
            year=year,
            config=config,
        )
        if composite_data is None:
            return None

        composite, transform, crs, scene_count = composite_data
        if config.save_composites:
            write_multiband_raster(
                composite_path,
                composite,
                transform,
                crs,
                tags={"scene_count": scene_count, "year": year, "tile_id": tile_id},
            )

    embeddings = compute_embeddings(
        composite=composite,
        year=year,
        model_name=config.model_name,
        patch_size=config.patch_size,
        crop_size=config.crop_size,
        device_preference=config.device,
    )
    embedding_transform = transform * Affine.scale(config.patch_size)

    display_factor = config.patch_size * config.display_aggregation
    display_transform = transform * Affine.scale(display_factor)

    ndvi_display = downsample_mean(ndvi(composite), display_factor)
    mndwi_display = downsample_mean(mndwi(composite), display_factor)
    ndbi_display = downsample_mean(ndbi(composite), display_factor)
    bsi_display = downsample_mean(bsi(composite), display_factor)
    population_display = None
    if config.include_population:
        population_display = fetch_population_display(
            tile_geometry=tile_geometry,
            tile_crs=tile_crs,
            display_transform=display_transform,
            display_shape=ndvi_display.shape,
            country_iso3=boundary.country_iso3,
            year=year,
            cache_dir=config.cache_dir,
        )
    pollution_display = None
    if config.include_pollution:
        if pollution_path.exists():
            pollution_array, _, _ = read_raster(pollution_path)
            pollution = pollution_array[0]
        else:
            pollution_data = fetch_sentinel2_aot_composite(
                tile_geometry=tile_geometry,
                tile_crs=tile_crs,
                year=year,
                config=config,
            )
            pollution = None
            if pollution_data is not None:
                pollution, pollution_transform, pollution_crs, _ = pollution_data
                write_single_band_raster(
                    pollution_path,
                    pollution,
                    pollution_transform,
                    pollution_crs,
                )
        if pollution is not None:
            pollution_display = downsample_mean(pollution, display_factor)

    tile_year_data = TileYearData(
        tile_id=tile_id,
        year=year,
        transform=transform,
        embedding_transform=embedding_transform,
        display_transform=display_transform,
        crs=crs,
        embeddings=embeddings,
        ndvi_display=ndvi_display,
        mndwi_display=mndwi_display,
        ndbi_display=ndbi_display,
        bsi_display=bsi_display,
        population_display=population_display,
        pollution_display=pollution_display,
        scene_count=scene_count,
    )

    cache_context = build_tile_processed_cache_context(
        tile_id=tile_id,
        tile_geometry=tile_geometry,
        year=year,
        boundary=boundary,
        config=config,
        composite_path=composite_path,
        pollution_path=pollution_path,
    )
    try:
        save_tile_year_cache(
            cache_path=processed_cache_path,
            cache_meta_path=processed_cache_meta_path,
            context=cache_context,
            tile_data=tile_year_data,
        )
    except Exception:
        # Cache write failures should never break a successful analysis run.
        pass

    return tile_year_data


def raster_file_signature(path: Path) -> dict[str, int] | None:
    if not path.exists():
        return None
    stats = path.stat()
    return {
        "size": int(stats.st_size),
        "mtime_ns": int(stats.st_mtime_ns),
    }


def build_tile_processed_cache_context(
    *,
    tile_id: str,
    tile_geometry: BaseGeometry,
    year: int,
    boundary: ResolvedBoundary,
    config: AnalysisConfig,
    composite_path: Path,
    pollution_path: Path,
) -> dict[str, Any]:
    return {
        "cache_version": 1,
        "tile_id": tile_id,
        "tile_bounds": [round(float(value), 3) for value in tile_geometry.bounds],
        "year": int(year),
        "country_iso3": boundary.country_iso3,
        "model_name": config.model_name,
        "tile_size_m": int(config.tile_size_m),
        "resolution_m": int(config.resolution_m),
        "patch_size": int(config.patch_size),
        "crop_size": int(config.crop_size),
        "display_aggregation": int(config.display_aggregation),
        "cloud_max": int(config.cloud_max),
        "fill_holes_pixels": int(config.fill_holes_pixels),
        "include_population": bool(config.include_population),
        "include_pollution": bool(config.include_pollution),
        "composite_signature": raster_file_signature(composite_path),
        "pollution_signature": (
            raster_file_signature(pollution_path) if config.include_pollution else None
        ),
    }


def load_tile_year_cache(
    *,
    cache_path: Path,
    cache_meta_path: Path,
    expected_context: dict[str, Any],
) -> TileYearData | None:
    if not cache_path.exists() or not cache_meta_path.exists():
        return None

    try:
        metadata = json.loads(cache_meta_path.read_text())
    except Exception:
        return None

    if metadata.get("context") != expected_context:
        return None

    try:
        with np.load(cache_path) as payload:
            has_population = bool(int(payload["has_population"]))
            has_pollution = bool(int(payload["has_pollution"]))
            population_display = (
                payload["population_display"].astype(np.float32)
                if has_population
                else None
            )
            pollution_display = (
                payload["pollution_display"].astype(np.float32)
                if has_pollution
                else None
            )

            return TileYearData(
                tile_id=str(metadata["context"]["tile_id"]),
                year=int(metadata["context"]["year"]),
                transform=Affine(
                    *[float(value) for value in payload["transform_coeffs"].tolist()]
                ),
                embedding_transform=Affine(
                    *[
                        float(value)
                        for value in payload["embedding_transform_coeffs"].tolist()
                    ]
                ),
                display_transform=Affine(
                    *[
                        float(value)
                        for value in payload["display_transform_coeffs"].tolist()
                    ]
                ),
                crs=str(metadata["crs"]),
                embeddings=payload["embeddings"].astype(np.float32),
                ndvi_display=payload["ndvi_display"].astype(np.float32),
                mndwi_display=payload["mndwi_display"].astype(np.float32),
                ndbi_display=payload["ndbi_display"].astype(np.float32),
                bsi_display=payload["bsi_display"].astype(np.float32),
                population_display=population_display,
                pollution_display=pollution_display,
                scene_count=int(payload["scene_count"]),
                cache_hit=True,
            )
    except Exception:
        return None


def save_tile_year_cache(
    *,
    cache_path: Path,
    cache_meta_path: Path,
    context: dict[str, Any],
    tile_data: TileYearData,
) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    temp_cache_path = cache_path.with_suffix(f"{cache_path.suffix}.tmp")
    temp_meta_path = cache_meta_path.with_suffix(f"{cache_meta_path.suffix}.tmp")

    with temp_cache_path.open("wb") as handle:
        np.savez_compressed(
            handle,
            embeddings=tile_data.embeddings.astype(np.float32),
            ndvi_display=tile_data.ndvi_display.astype(np.float32),
            mndwi_display=tile_data.mndwi_display.astype(np.float32),
            ndbi_display=tile_data.ndbi_display.astype(np.float32),
            bsi_display=tile_data.bsi_display.astype(np.float32),
            population_display=(
                tile_data.population_display.astype(np.float32)
                if tile_data.population_display is not None
                else np.empty((0, 0), dtype=np.float32)
            ),
            pollution_display=(
                tile_data.pollution_display.astype(np.float32)
                if tile_data.pollution_display is not None
                else np.empty((0, 0), dtype=np.float32)
            ),
            has_population=np.array(
                1 if tile_data.population_display is not None else 0, dtype=np.uint8
            ),
            has_pollution=np.array(
                1 if tile_data.pollution_display is not None else 0, dtype=np.uint8
            ),
            transform_coeffs=np.array(tuple(tile_data.transform), dtype=np.float64),
            embedding_transform_coeffs=np.array(
                tuple(tile_data.embedding_transform), dtype=np.float64
            ),
            display_transform_coeffs=np.array(
                tuple(tile_data.display_transform), dtype=np.float64
            ),
            scene_count=np.array(int(tile_data.scene_count), dtype=np.int32),
        )
    temp_cache_path.replace(cache_path)

    temp_meta_path.write_text(
        json.dumps(
            {
                "context": context,
                "crs": tile_data.crs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    temp_meta_path.replace(cache_meta_path)


def fetch_sentinel2_composite(
    *,
    tile_geometry: BaseGeometry,
    tile_crs: str,
    year: int,
    config: AnalysisConfig,
) -> tuple[np.ndarray, Affine, str, int] | None:
    bounds = tile_geometry.bounds
    items = search_sentinel2_items(
        tile_geometry=tile_geometry,
        tile_crs=tile_crs,
        year=year,
        config=config,
    )
    if not items:
        return None

    epsg = int(str(tile_crs).split(":")[-1])
    stack = stackstac.stack(
        items,
        assets=S2_BANDS,
        bounds=bounds,
        resolution=config.resolution_m,
        epsg=epsg,
        dtype=np.float64,
        rescale=False,
        fill_value=np.nan,
        chunksize=512,
    )
    median = stack.median(dim="time", skipna=True).compute()
    array = median.values.astype(np.float32)
    array = np.clip((array - 1000.0) / 10000.0, 0.0, 1.0)

    if config.fill_holes_pixels > 0:
        array = fill_holes(array, config.fill_holes_pixels)

    x = median.x.values
    y = median.y.values
    transform = from_origin(
        float(np.min(x) - config.resolution_m / 2.0),
        float(np.max(y) + config.resolution_m / 2.0),
        config.resolution_m,
        config.resolution_m,
    )
    return array, transform, f"EPSG:{epsg}", len(items)


def fetch_sentinel2_aot_composite(
    *,
    tile_geometry: BaseGeometry,
    tile_crs: str,
    year: int,
    config: AnalysisConfig,
) -> tuple[np.ndarray, Affine, str, int] | None:
    items = search_sentinel2_items(
        tile_geometry=tile_geometry,
        tile_crs=tile_crs,
        year=year,
        config=config,
    )
    if not items:
        return None

    bounds = tile_geometry.bounds
    epsg = int(str(tile_crs).split(":")[-1])
    stack = stackstac.stack(
        items,
        assets=["AOT"],
        bounds=bounds,
        resolution=config.resolution_m,
        epsg=epsg,
        dtype=np.float64,
        rescale=False,
        fill_value=np.nan,
        chunksize=512,
    )
    median = stack.median(dim="time", skipna=True).compute()
    aot = median.values.astype(np.float32)[0] / 1000.0
    aot = np.clip(aot, 0.0, 2.0)
    if config.fill_holes_pixels > 0:
        aot = fill_holes(aot[None, ...], config.fill_holes_pixels)[0]

    x = median.x.values
    y = median.y.values
    transform = from_origin(
        float(np.min(x) - config.resolution_m / 2.0),
        float(np.max(y) + config.resolution_m / 2.0),
        config.resolution_m,
        config.resolution_m,
    )
    return aot, transform, f"EPSG:{epsg}", len(items)


def search_sentinel2_items(
    *,
    tile_geometry: BaseGeometry,
    tile_crs: str,
    year: int,
    config: AnalysisConfig,
) -> list[Any] | None:
    catalog = planetary_catalog()
    bbox_latlon = (
        gpd.GeoSeries([tile_geometry], crs=tile_crs)
        .to_crs("EPSG:4326")
        .total_bounds.tolist()
    )
    datetime_range = f"{year}-01-01/{year}-12-31"
    for cloud_cap in [config.cloud_max, 60, 80]:
        search = catalog.search(
            collections=["sentinel-2-l2a"],
            bbox=bbox_latlon,
            datetime=datetime_range,
            query={"eo:cloud_cover": {"lt": cloud_cap}},
        )
        found = list(search.items())
        if found:
            return found
    return None


def compute_embeddings(
    *,
    composite: np.ndarray,
    year: int,
    model_name: str,
    patch_size: int,
    crop_size: int,
    device_preference: str = "auto",
) -> np.ndarray:
    image = composite.transpose(1, 2, 0)[None, :, :, None, :]
    normalizer = Normalizer(Strategy.COMPUTED)
    image = normalizer.normalize(Modality.SENTINEL2_L2A, image).astype(np.float32)

    _, height, width, _, _ = image.shape
    pad_h = (crop_size - (height % crop_size)) % crop_size
    pad_w = (crop_size - (width % crop_size)) % crop_size
    if pad_h or pad_w:
        image = np.pad(
            image, ((0, 0), (0, pad_h), (0, pad_w), (0, 0), (0, 0)), mode="edge"
        )

    device = resolve_torch_device(device_preference)
    model = load_olmoearth_model(model_name, device.type)
    output_h = image.shape[1] // patch_size
    output_w = image.shape[2] // patch_size
    embed_dim = embedding_dim_for_model(model_name)
    output = np.zeros((embed_dim, output_h, output_w), dtype=np.float32)
    counts = np.zeros((output_h, output_w), dtype=np.float32)

    timestamp = torch.tensor([[[15, 6, year]]], dtype=torch.int64, device=device)
    mask_value = float(MaskValue.ONLINE_ENCODER.value)

    for top in range(0, image.shape[1], crop_size):
        for left in range(0, image.shape[2], crop_size):
            crop = image[:, top : top + crop_size, left : left + crop_size, :, :]
            tensor = torch.tensor(crop, dtype=torch.float32, device=device)
            mask = torch.full(
                (1, crop_size, crop_size, 1, 3),
                mask_value,
                dtype=torch.float32,
                device=device,
            )
            sample = MaskedOlmoEarthSample(
                sentinel2_l2a=tensor,
                sentinel2_l2a_mask=mask,
                timestamps=timestamp,
            )
            with torch.no_grad():
                encoded = model.encoder(sample, fast_pass=True, patch_size=patch_size)[
                    "tokens_and_masks"
                ].sentinel2_l2a
                pooled = encoded.mean(dim=[3, 4])[0].permute(2, 0, 1).cpu().numpy()

            out_top = top // patch_size
            out_left = left // patch_size
            out_h = pooled.shape[1]
            out_w = pooled.shape[2]
            output[:, out_top : out_top + out_h, out_left : out_left + out_w] += pooled
            counts[out_top : out_top + out_h, out_left : out_left + out_w] += 1.0

    counts = np.maximum(counts, 1.0)
    output /= counts[None, :, :]
    return output[:, : height // patch_size, : width // patch_size]


def build_overlay(
    boundary: ResolvedBoundary,
    tiles: GeoDataFrame,
    year_results: dict[int, dict[str, TileYearData]],
    config: AnalysisConfig,
) -> GeoDataFrame:
    base_results = year_results[config.base_year]
    boundary_proj = gpd.GeoDataFrame(
        [{"geometry": boundary.geometry}],
        crs="EPSG:4326",
    ).to_crs(tiles.crs)
    boundary_geom_proj = boundary_proj.geometry.iloc[0]

    rows: list[dict[str, Any]] = []
    display_factor = config.display_aggregation

    for tile in tiles.itertuples(index=False):
        base = base_results.get(str(tile.tile_id))
        if base is None:
            continue

        period_changes: dict[int, np.ndarray] = {}
        period_metrics: dict[int, dict[str, np.ndarray]] = {}
        for period in config.periods:
            year = config.base_year - period
            ref = year_results.get(year, {}).get(str(tile.tile_id))
            if ref is None:
                continue
            period_changes[period] = downsample_mean(
                embedding_shift(base.embeddings, ref.embeddings),
                display_factor,
            )
            period_metrics[period] = {
                "ndvi": base.ndvi_display - ref.ndvi_display,
                "mndwi": base.mndwi_display - ref.mndwi_display,
                "ndbi": base.ndbi_display - ref.ndbi_display,
                "bsi": base.bsi_display - ref.bsi_display,
            }
            if (
                base.population_display is not None
                and ref.population_display is not None
            ):
                period_metrics[period]["population_base"] = base.population_display
                period_metrics[period]["population_ref"] = ref.population_display
            if base.pollution_display is not None and ref.pollution_display is not None:
                period_metrics[period]["pollution"] = (
                    base.pollution_display - ref.pollution_display
                )

            if config.save_embedding_change_rasters:
                write_single_band_raster(
                    config.output_dir
                    / "rasters"
                    / f"{tile.tile_id}_embedding_change_{period}y.tif",
                    period_changes[period],
                    base.display_transform,
                    base.crs,
                )

        any_change = next(iter(period_changes.values()), None)
        if any_change is None:
            continue

        height, width = any_change.shape
        for row_idx in range(height):
            for col_idx in range(width):
                cell_geom = pixel_polygon(base.display_transform, row_idx, col_idx)
                clipped_geom = cell_geom.intersection(boundary_geom_proj)
                if clipped_geom.is_empty or clipped_geom.area <= 0:
                    continue
                coverage_fraction = clipped_geom.area / max(cell_geom.area, 1e-9)
                feature: dict[str, Any] = {
                    "tile_id": str(tile.tile_id),
                    "row": row_idx,
                    "col": col_idx,
                    "base_year": config.base_year,
                    "scene_count_base_year": base.scene_count,
                    "geometry": clipped_geom,
                }
                for period in config.periods:
                    if period not in period_changes:
                        continue
                    change_value = float(period_changes[period][row_idx, col_idx])
                    ndvi_value = float(period_metrics[period]["ndvi"][row_idx, col_idx])
                    mndwi_value = float(
                        period_metrics[period]["mndwi"][row_idx, col_idx]
                    )
                    ndbi_value = float(period_metrics[period]["ndbi"][row_idx, col_idx])
                    bsi_value = float(period_metrics[period]["bsi"][row_idx, col_idx])
                    feature[f"embedding_change_{period}y"] = round(change_value, 6)
                    feature[f"vegetation_delta_{period}y"] = round(ndvi_value, 6)
                    feature[f"water_delta_{period}y"] = round(mndwi_value, 6)
                    feature[f"urban_delta_{period}y"] = round(ndbi_value, 6)
                    feature[f"bare_soil_delta_{period}y"] = round(bsi_value, 6)
                    if "pollution" in period_metrics[period]:
                        pollution_value = float(
                            period_metrics[period]["pollution"][row_idx, col_idx]
                        )
                        feature[f"pollution_delta_{period}y"] = round(
                            pollution_value, 6
                        )
                    if "population_base" in period_metrics[period]:
                        population_base = (
                            float(
                                period_metrics[period]["population_base"][
                                    row_idx, col_idx
                                ]
                            )
                            * coverage_fraction
                        )
                        population_ref = (
                            float(
                                period_metrics[period]["population_ref"][
                                    row_idx, col_idx
                                ]
                            )
                            * coverage_fraction
                        )
                        population_delta = population_base - population_ref
                        feature[f"population_base_{period}y"] = round(
                            population_base, 6
                        )
                        feature[f"population_reference_{period}y"] = round(
                            population_ref, 6
                        )
                        feature[f"population_delta_{period}y"] = round(
                            population_delta, 6
                        )
                        feature[f"population_pct_change_{period}y"] = (
                            round(100.0 * population_delta / population_ref, 6)
                            if population_ref > 1e-6
                            else None
                        )
                    feature[f"story_{period}y"] = classify_story(
                        ndvi_delta=ndvi_value,
                        water_delta=mndwi_value,
                        urban_delta=ndbi_value,
                        bare_soil_delta=bsi_value,
                        embedding_change=change_value,
                    )
                rows.append(feature)

    overlay = gpd.GeoDataFrame(rows, crs=tiles.crs).to_crs("EPSG:4326")
    return overlay


def build_ward_overlay(
    boundary: ResolvedBoundary,
    overlay: GeoDataFrame,
    config: AnalysisConfig,
) -> GeoDataFrame:
    if overlay.empty:
        return gpd.GeoDataFrame(
            columns=["ward_id", "ward_name", "geometry"], crs="EPSG:4326"
        )

    wards = resolve_ward_boundaries(boundary=boundary, cache_dir=config.cache_dir)
    if wards.empty:
        return wards

    aggregation_crs = "EPSG:6933"
    wards_proj = wards.to_crs(aggregation_crs).copy()
    overlay_proj = overlay.to_crs(aggregation_crs).copy()
    wards_proj["ward_area_sq_m"] = wards_proj.geometry.area
    overlay_proj["cell_area_sq_m"] = overlay_proj.geometry.area

    mean_metric_columns: list[str] = []
    sum_metric_columns: list[str] = []
    for period in config.periods:
        mean_metric_columns.extend(
            [
                f"embedding_change_{period}y",
                f"vegetation_delta_{period}y",
                f"water_delta_{period}y",
                f"urban_delta_{period}y",
                f"bare_soil_delta_{period}y",
                f"pollution_delta_{period}y",
            ]
        )
        sum_metric_columns.extend(
            [
                f"population_base_{period}y",
                f"population_reference_{period}y",
                f"population_delta_{period}y",
            ]
        )
    mean_metric_columns = [
        column for column in mean_metric_columns if column in overlay_proj.columns
    ]
    sum_metric_columns = [
        column for column in sum_metric_columns if column in overlay_proj.columns
    ]

    intersections = gpd.overlay(
        wards_proj[["ward_id", "ward_name", "admin_level", "geometry"]],
        overlay_proj[
            ["geometry", "cell_area_sq_m", *mean_metric_columns, *sum_metric_columns]
        ],
        how="intersection",
        keep_geom_type=False,
    )
    if intersections.empty:
        return gpd.GeoDataFrame(
            columns=["ward_id", "ward_name", "geometry"], crs="EPSG:4326"
        )

    intersections = intersections[
        intersections.geometry.geom_type.isin(["Polygon", "MultiPolygon"])
    ].copy()
    if intersections.empty:
        return gpd.GeoDataFrame(
            columns=["ward_id", "ward_name", "geometry"], crs="EPSG:4326"
        )

    intersections["intersection_area_sq_m"] = intersections.geometry.area
    intersections = intersections[intersections["intersection_area_sq_m"] > 0].copy()
    if intersections.empty:
        return gpd.GeoDataFrame(
            columns=["ward_id", "ward_name", "geometry"], crs="EPSG:4326"
        )

    for column in mean_metric_columns:
        intersections[f"{column}__weighted"] = (
            intersections[column] * intersections["intersection_area_sq_m"]
        )
    for column in sum_metric_columns:
        intersections[f"{column}__share"] = (
            intersections[column]
            * intersections["intersection_area_sq_m"]
            / intersections["cell_area_sq_m"].clip(lower=1.0)
        )

    aggregation_columns = {
        "intersection_area_sq_m": "sum",
        **{f"{column}__weighted": "sum" for column in mean_metric_columns},
        **{f"{column}__share": "sum" for column in sum_metric_columns},
    }
    aggregated = (
        intersections.groupby(["ward_id", "ward_name", "admin_level"], dropna=False)
        .agg(aggregation_columns)
        .reset_index()
    )
    wards_aggregated = wards_proj.merge(
        aggregated,
        on=["ward_id", "ward_name", "admin_level"],
        how="left",
    )
    wards_aggregated = wards_aggregated[
        wards_aggregated["intersection_area_sq_m"].fillna(0.0) > 0
    ].copy()
    if wards_aggregated.empty:
        return gpd.GeoDataFrame(
            columns=["ward_id", "ward_name", "geometry"], crs="EPSG:4326"
        )

    wards_aggregated["coverage_percent"] = (
        100.0
        * wards_aggregated["intersection_area_sq_m"]
        / wards_aggregated["ward_area_sq_m"].clip(lower=1.0)
    )
    for column in mean_metric_columns:
        wards_aggregated[column] = wards_aggregated[
            f"{column}__weighted"
        ] / wards_aggregated["intersection_area_sq_m"].clip(lower=1.0)
    for column in sum_metric_columns:
        wards_aggregated[column] = wards_aggregated[f"{column}__share"]

    for period in config.periods:
        embedding_column = f"embedding_change_{period}y"
        if embedding_column not in wards_aggregated.columns:
            continue
        population_reference_column = f"population_reference_{period}y"
        population_delta_column = f"population_delta_{period}y"
        population_pct_column = f"population_pct_change_{period}y"
        if (
            population_reference_column in wards_aggregated.columns
            and population_delta_column in wards_aggregated.columns
        ):
            wards_aggregated[population_pct_column] = np.where(
                wards_aggregated[population_reference_column] > 1e-6,
                100.0
                * wards_aggregated[population_delta_column]
                / wards_aggregated[population_reference_column],
                np.nan,
            )
        wards_aggregated[f"story_{period}y"] = wards_aggregated.apply(
            lambda row: classify_story(
                ndvi_delta=clean_number(row.get(f"vegetation_delta_{period}y", 0.0)),
                water_delta=clean_number(row.get(f"water_delta_{period}y", 0.0)),
                urban_delta=clean_number(row.get(f"urban_delta_{period}y", 0.0)),
                bare_soil_delta=clean_number(
                    row.get(f"bare_soil_delta_{period}y", 0.0)
                ),
                embedding_change=clean_number(row.get(embedding_column, 0.0)),
            ),
            axis=1,
        )

    keep_columns = [
        "ward_id",
        "ward_name",
        "admin_level",
        "coverage_percent",
        "geometry",
    ]
    keep_columns.extend(
        column
        for column in mean_metric_columns + sum_metric_columns
        if column in wards_aggregated.columns
    )
    keep_columns.extend(
        column
        for column in [f"population_pct_change_{period}y" for period in config.periods]
        if column in wards_aggregated.columns
    )
    keep_columns.extend(
        column
        for column in [f"story_{period}y" for period in config.periods]
        if column in wards_aggregated.columns
    )
    ward_overlay = wards_aggregated[keep_columns].copy().to_crs("EPSG:4326")
    numeric_columns = [
        column
        for column in ward_overlay.columns
        if column not in {"ward_id", "ward_name", "admin_level", "geometry"}
        and not column.startswith("story_")
    ]
    for column in numeric_columns:
        ward_overlay[column] = ward_overlay[column].round(6)
    return ward_overlay


def build_summary(
    metadata: dict[str, Any],
    overlay: GeoDataFrame,
    year_results: dict[int, dict[str, TileYearData]],
    config: AnalysisConfig,
) -> dict[str, Any]:
    periods: dict[str, Any] = {}
    for period in config.periods:
        key = f"embedding_change_{period}y"
        if key not in overlay.columns:
            continue
        frame = overlay.dropna(subset=[key]).copy()
        if frame.empty:
            continue

        hotspots = frame.sort_values(key, ascending=False).head(8)
        period_key = f"{period}y"
        metrics = {
            "embedding_change_median": round(float(frame[key].median()), 6),
            "embedding_change_p95": round(float(frame[key].quantile(0.95)), 6),
            "vegetation_delta_mean": round(
                float(frame[f"vegetation_delta_{period}y"].mean()), 6
            ),
            "water_delta_mean": round(float(frame[f"water_delta_{period}y"].mean()), 6),
            "urban_delta_mean": round(float(frame[f"urban_delta_{period}y"].mean()), 6),
            "bare_soil_delta_mean": round(
                float(frame[f"bare_soil_delta_{period}y"].mean()), 6
            ),
        }
        pollution_column = f"pollution_delta_{period}y"
        if pollution_column in frame.columns:
            metrics["pollution_delta_mean"] = round(
                float(frame[pollution_column].mean()), 6
            )

        periods[period_key] = {
            "metrics": metrics,
            "story_counts": frame[f"story_{period}y"].value_counts().to_dict(),
            "hotspots": [
                {
                    "latitude": round(float(row.geometry.centroid.y), 6),
                    "longitude": round(float(row.geometry.centroid.x), 6),
                    "embedding_change": round(
                        float(row[f"embedding_change_{period}y"]), 6
                    ),
                    "vegetation_delta": round(
                        float(row[f"vegetation_delta_{period}y"]), 6
                    ),
                    "water_delta": round(float(row[f"water_delta_{period}y"]), 6),
                    "urban_delta": round(float(row[f"urban_delta_{period}y"]), 6),
                    "bare_soil_delta": round(
                        float(row[f"bare_soil_delta_{period}y"]), 6
                    ),
                    "pollution_delta": (
                        round(clean_number(row[pollution_column]), 6)
                        if pollution_column in frame.columns
                        and not (
                            isinstance(row[pollution_column], (float, np.floating))
                            and np.isnan(row[pollution_column])
                        )
                        else None
                    ),
                    "story": str(row[f"story_{period}y"]),
                    "tile_id": str(row["tile_id"]),
                }
                for _, row in hotspots.iterrows()
            ],
        }
        population_delta_key = f"population_delta_{period}y"
        if population_delta_key in frame.columns:
            population_base_key = f"population_base_{period}y"
            population_reference_key = f"population_reference_{period}y"
            population_base_total = float(frame[population_base_key].fillna(0.0).sum())
            population_reference_total = float(
                frame[population_reference_key].fillna(0.0).sum()
            )
            population_delta_total = float(
                frame[population_delta_key].fillna(0.0).sum()
            )
            periods[period_key]["metrics"].update(
                {
                    "population_base_total": round(population_base_total, 3),
                    "population_reference_total": round(population_reference_total, 3),
                    "population_delta_total": round(population_delta_total, 3),
                    "population_delta_mean": round(
                        float(frame[population_delta_key].fillna(0.0).mean()),
                        6,
                    ),
                    "population_pct_change_total": (
                        round(
                            100.0 * population_delta_total / population_reference_total,
                            3,
                        )
                        if population_reference_total > 1e-6
                        else None
                    ),
                }
            )
            for hotspot, (_, row) in zip(
                periods[period_key]["hotspots"], hotspots.iterrows()
            ):
                population_delta = row.get(population_delta_key, 0.0)
                population_delta_value = (
                    0.0
                    if population_delta is None
                    or (
                        isinstance(population_delta, (float, np.floating))
                        and np.isnan(population_delta)
                    )
                    else float(population_delta)
                )
                hotspot["population_delta"] = round(
                    population_delta_value,
                    6,
                )
                population_pct = row.get(f"population_pct_change_{period}y")
                hotspot["population_pct_change"] = (
                    round(float(population_pct), 6)
                    if population_pct is not None
                    and not (
                        isinstance(population_pct, (float, np.floating))
                        and np.isnan(population_pct)
                    )
                    else None
                )

    scene_counts: dict[str, int] = {}
    for year, tile_dict in year_results.items():
        if tile_dict:
            scene_counts[str(year)] = int(
                sum(item.scene_count for item in tile_dict.values())
            )

    return {
        "metadata": metadata,
        "config": {
            "base_year": config.base_year,
            "periods": list(config.periods),
            "model_name": config.model_name,
            "tile_size_m": config.tile_size_m,
            "workers": config.workers,
            "include_population": config.include_population,
            "include_pollution": config.include_pollution,
            "display_cell_size_m": config.patch_size
            * config.display_aggregation
            * config.resolution_m,
        },
        "scene_counts": scene_counts,
        "feature_count": int(len(overlay)),
        "periods": periods,
    }


def render_report(summary: dict[str, Any]) -> str:
    metadata = summary["metadata"]
    lines = [
        f"# OlmoEarth Change Report: {metadata['label']}",
        "",
        f"- Area scanned: {metadata['area_sq_km']:.1f} sq km",
        f"- Base year: {summary['config']['base_year']}",
        f"- Periods: {', '.join(f'{period}y' for period in summary['config']['periods'])}",
        f"- Overlay cells: {summary['feature_count']}",
        "",
    ]
    for period_key, period_summary in summary["periods"].items():
        metrics = period_summary["metrics"]
        pollution_clause = ""
        if "pollution_delta_mean" in metrics:
            pollution_clause = (
                f" | pollution proxy delta: {metrics['pollution_delta_mean']:.4f}"
            )
        lines.extend(
            [
                f"## {period_key} View",
                "",
                (
                    f"- Median embedding change: {metrics['embedding_change_median']:.4f}"
                    f" | p95: {metrics['embedding_change_p95']:.4f}"
                ),
                (
                    f"- Mean vegetation delta: {metrics['vegetation_delta_mean']:.4f}"
                    f" | water delta: {metrics['water_delta_mean']:.4f}"
                    f" | urban delta: {metrics['urban_delta_mean']:.4f}"
                    f" | bare-soil delta: {metrics['bare_soil_delta_mean']:.4f}"
                    f"{pollution_clause}"
                ),
            ]
        )
        if "population_delta_total" in metrics:
            population_pct = metrics.get("population_pct_change_total")
            population_pct_text = (
                f"{population_pct:.2f}%" if population_pct is not None else "n/a"
            )
            lines.append(
                (
                    f"- Population total: {metrics['population_reference_total']:.1f}"
                    f" -> {metrics['population_base_total']:.1f}"
                    f" | delta: {metrics['population_delta_total']:.1f}"
                    f" | pct: {population_pct_text}"
                )
            )
        lines.extend(
            [
                f"- Dominant stories: {', '.join(f'{k} ({v})' for k, v in period_summary['story_counts'].items())}",
                "",
            ]
        )
        for hotspot in period_summary["hotspots"][:5]:
            population_clause = ""
            hotspot_pollution_clause = ""
            if "population_delta" in hotspot:
                population_pct = hotspot.get("population_pct_change")
                population_pct_text = (
                    f"{population_pct:.2f}%" if population_pct is not None else "n/a"
                )
                population_clause = f", population {hotspot['population_delta']:.2f} ({population_pct_text})"
            if hotspot.get("pollution_delta") is not None:
                hotspot_pollution_clause = (
                    f", pollution {hotspot['pollution_delta']:.4f}"
                )
            lines.append(
                (
                    f"- Hotspot near ({hotspot['latitude']}, {hotspot['longitude']}): "
                    f"{hotspot['story']} | embedding {hotspot['embedding_change']:.4f}, "
                    f"veg {hotspot['vegetation_delta']:.4f}, water {hotspot['water_delta']:.4f}, "
                    f"urban {hotspot['urban_delta']:.4f}, bare soil {hotspot['bare_soil_delta']:.4f}"
                    f"{hotspot_pollution_clause}"
                    f"{population_clause}"
                )
            )
        lines.append("")
    return "\n".join(lines)


def copy_ui_bundle(output_dir: Path) -> None:
    source_dir = Path(__file__).resolve().parents[2] / "app"
    target_dir = output_dir / "ui"
    target_dir.mkdir(parents=True, exist_ok=True)
    for name in ["index.html", "app.js", "styles.css"]:
        shutil.copy2(source_dir / name, target_dir / name)


def ndvi(composite: np.ndarray) -> np.ndarray:
    return safe_index(composite[NIR_IDX], composite[RED_IDX])


def mndwi(composite: np.ndarray) -> np.ndarray:
    return safe_index(composite[GREEN_IDX], composite[SWIR1_IDX])


def ndbi(composite: np.ndarray) -> np.ndarray:
    return safe_index(composite[SWIR1_IDX], composite[NIR_IDX])


def bsi(composite: np.ndarray) -> np.ndarray:
    numerator = (composite[SWIR1_IDX] + composite[RED_IDX]) - (
        composite[NIR_IDX] + composite[BLUE_IDX]
    )
    denominator = (composite[SWIR1_IDX] + composite[RED_IDX]) + (
        composite[NIR_IDX] + composite[BLUE_IDX]
    )
    return np.divide(
        numerator,
        denominator,
        out=np.zeros_like(numerator, dtype=np.float32),
        where=np.abs(denominator) > 1e-6,
    )


def safe_index(numerator: np.ndarray, denominator_term: np.ndarray) -> np.ndarray:
    denominator = numerator + denominator_term
    return np.divide(
        numerator - denominator_term,
        denominator,
        out=np.zeros_like(numerator, dtype=np.float32),
        where=np.abs(denominator) > 1e-6,
    )


def clean_number(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(number):
        return default
    return number


def downsample_mean(array: np.ndarray, factor: int) -> np.ndarray:
    height = (array.shape[0] // factor) * factor
    width = (array.shape[1] // factor) * factor
    trimmed = array[:height, :width]
    reshaped = trimmed.reshape(height // factor, factor, width // factor, factor)
    return reshaped.mean(axis=(1, 3))


def embedding_shift(
    base_embeddings: np.ndarray, ref_embeddings: np.ndarray
) -> np.ndarray:
    return np.linalg.norm(base_embeddings - ref_embeddings, axis=0)


def classify_story(
    *,
    ndvi_delta: float,
    water_delta: float,
    urban_delta: float,
    bare_soil_delta: float,
    embedding_change: float,
) -> str:
    if urban_delta > 0.08 and ndvi_delta < -0.03 and embedding_change > 0.001:
        return "Urban expansion replacing vegetation"
    if water_delta > 0.08 and embedding_change > 0.001:
        return "Water gain or inundation"
    if water_delta < -0.08 and embedding_change > 0.001:
        return "Water retreat or drying"
    if bare_soil_delta > 0.08 and ndvi_delta < -0.04 and embedding_change > 0.001:
        return "Bare-soil or industrial expansion"
    if ndvi_delta > 0.08 and embedding_change > 0.001:
        return "Greening or crop intensification"
    return "Mixed land-cover change"


def pixel_polygon(transform: Affine, row: int, col: int) -> BaseGeometry:
    left, top = transform * (col, row)
    right, bottom = transform * (col + 1, row + 1)
    return box(left, bottom, right, top)


def fill_holes(array: np.ndarray, max_distance: int) -> np.ndarray:
    filled = array.copy()
    for band_idx in range(filled.shape[0]):
        band = filled[band_idx]
        valid = np.isfinite(band)
        if np.all(valid):
            continue
        masked = np.where(valid, band, 0.0).astype(np.float32)
        filled[band_idx] = rasterio.fill.fillnodata(
            masked,
            mask=valid.astype(np.uint8),
            max_search_distance=float(max_distance),
            smoothing_iterations=0,
        )
    return filled


def fetch_population_display(
    *,
    tile_geometry: BaseGeometry,
    tile_crs: str,
    display_transform: Affine,
    display_shape: tuple[int, int],
    country_iso3: str,
    year: int,
    cache_dir: Path,
) -> np.ndarray | None:
    if year < WORLDPOP_MIN_YEAR or year > WORLDPOP_MAX_YEAR:
        return None

    population_path = ensure_worldpop_population_raster(
        country_iso3=country_iso3,
        year=year,
        cache_dir=cache_dir,
    )
    tile_bounds = (
        gpd.GeoSeries([tile_geometry], crs=tile_crs)
        .to_crs("EPSG:4326")
        .total_bounds.tolist()
    )
    with rasterio.open(population_path) as src:
        raw_window = from_bounds(*tile_bounds, transform=src.transform)
        if raw_window.width <= 0 or raw_window.height <= 0:
            return np.zeros(display_shape, dtype=np.float32)
        full_window = Window(col_off=0, row_off=0, width=src.width, height=src.height)
        clipped_window = raw_window.intersection(full_window)
        col_off = max(0, math.floor(clipped_window.col_off))
        row_off = max(0, math.floor(clipped_window.row_off))
        col_end = min(
            src.width,
            math.ceil(clipped_window.col_off + clipped_window.width),
        )
        row_end = min(
            src.height,
            math.ceil(clipped_window.row_off + clipped_window.height),
        )
        if col_end <= col_off or row_end <= row_off:
            return np.zeros(display_shape, dtype=np.float32)
        window = Window(
            col_off=col_off,
            row_off=row_off,
            width=col_end - col_off,
            height=row_end - row_off,
        )
        data = src.read(1, window=window, masked=True).astype(np.float32)
        window_transform = src.window_transform(window)

    valid = (~np.ma.getmaskarray(data)) & np.isfinite(data) & (data > 0)
    if not np.any(valid):
        return np.zeros(display_shape, dtype=np.float32)

    pixel_indices = np.argwhere(valid)
    source_geometries = [
        pixel_polygon(window_transform, int(row_idx), int(col_idx))
        for row_idx, col_idx in pixel_indices
    ]
    source_values = [
        float(data[row_idx, col_idx]) for row_idx, col_idx in pixel_indices
    ]
    projected_geometries = (
        gpd.GeoSeries(source_geometries, crs="EPSG:4326").to_crs(tile_crs).tolist()
    )

    display_cells = [
        [
            pixel_polygon(display_transform, row_idx, col_idx)
            for col_idx in range(display_shape[1])
        ]
        for row_idx in range(display_shape[0])
    ]
    population_display = np.zeros(display_shape, dtype=np.float32)
    for pixel_geom, pixel_value in zip(projected_geometries, source_values):
        pixel_area = pixel_geom.area
        if pixel_area <= 0:
            continue
        minx, miny, maxx, maxy = pixel_geom.bounds
        for row_idx in range(display_shape[0]):
            for col_idx in range(display_shape[1]):
                cell_geom = display_cells[row_idx][col_idx]
                cell_minx, cell_miny, cell_maxx, cell_maxy = cell_geom.bounds
                if (
                    cell_maxx <= minx
                    or cell_minx >= maxx
                    or cell_maxy <= miny
                    or cell_miny >= maxy
                ):
                    continue
                intersection_area = pixel_geom.intersection(cell_geom).area
                if intersection_area <= 0:
                    continue
                population_display[row_idx, col_idx] += float(
                    pixel_value * intersection_area / pixel_area
                )

    return population_display


def read_raster(path: Path) -> tuple[np.ndarray, Affine, str]:
    with rasterio.open(path) as src:
        return src.read().astype(np.float32), src.transform, str(src.crs)


def read_tags(path: Path) -> dict[str, str]:
    with rasterio.open(path) as src:
        return src.tags()


def write_multiband_raster(
    path: Path,
    array: np.ndarray,
    transform: Affine,
    crs: str,
    tags: dict[str, Any] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=array.shape[1],
        width=array.shape[2],
        count=array.shape[0],
        dtype="float32",
        crs=crs,
        transform=transform,
        compress="deflate",
    ) as dst:
        dst.write(array.astype(np.float32))
        if tags:
            dst.update_tags(**{k: str(v) for k, v in tags.items()})


def write_single_band_raster(
    path: Path,
    array: np.ndarray,
    transform: Affine,
    crs: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=array.shape[0],
        width=array.shape[1],
        count=1,
        dtype="float32",
        crs=crs,
        transform=transform,
        compress="deflate",
    ) as dst:
        dst.write(array.astype(np.float32), 1)


def utm_crs_for_geometry(geometry: BaseGeometry) -> str:
    lon = geometry.centroid.x
    lat = geometry.centroid.y
    zone = int((lon + 180.0) / 6.0) + 1
    epsg = 32600 + zone if lat >= 0 else 32700 + zone
    return f"EPSG:{epsg}"


def write_boundary_geojson(boundary: ResolvedBoundary, path: Path) -> None:
    gdf = gpd.GeoDataFrame(
        [
            {
                "label": boundary.label,
                "country_iso3": boundary.country_iso3,
                "admin_level": boundary.admin_level,
                "state_name": boundary.state_name,
                "district_name": boundary.district_name,
                "area_sq_km": boundary.area_sq_km,
                "geometry": boundary.geometry,
            }
        ],
        crs="EPSG:4326",
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(path, driver="GeoJSON")


def overlay_coverage_sq_km(overlay: GeoDataFrame) -> float:
    if overlay.empty:
        return 0.0
    return float(overlay.to_crs("EPSG:6933").geometry.area.sum() / 1_000_000.0)


def worldpop_population_url(country_iso3: str, year: int) -> str:
    iso3 = country_iso3.upper()
    iso_lower = iso3.lower()
    return (
        "https://data.worldpop.org/GIS/Population/Global_2015_2030/"
        f"{WORLDPOP_RELEASE}/{year}/{iso3}/{WORLDPOP_VERSION}/1km_ua/constrained/"
        f"{iso_lower}_pop_{year}_CN_1km_{WORLDPOP_RELEASE}_UA_{WORLDPOP_VERSION}.tif"
    )


def worldpop_population_cache_path(
    country_iso3: str, year: int, cache_dir: Path
) -> Path:
    iso3 = country_iso3.upper()
    iso_lower = iso3.lower()
    filename = (
        f"{iso_lower}_pop_{year}_CN_1km_{WORLDPOP_RELEASE}_UA_{WORLDPOP_VERSION}.tif"
    )
    return cache_dir / "worldpop" / WORLDPOP_RELEASE / iso3 / filename


def ensure_worldpop_population_raster(
    *,
    country_iso3: str,
    year: int,
    cache_dir: Path,
) -> Path:
    if year < WORLDPOP_MIN_YEAR or year > WORLDPOP_MAX_YEAR:
        raise ValueError(
            f"WorldPop population is only available for {WORLDPOP_MIN_YEAR}-{WORLDPOP_MAX_YEAR}."
        )

    target_path = worldpop_population_cache_path(country_iso3, year, cache_dir)
    if target_path.exists():
        return target_path

    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.with_suffix(f"{target_path.suffix}.part")
    request = Request(
        worldpop_population_url(country_iso3, year),
        headers={"User-Agent": "olmoearth-change/0.1"},
    )
    try:
        with urlopen(request) as response, temp_path.open("wb") as handle:
            shutil.copyfileobj(response, handle)
        temp_path.replace(target_path)
    except (HTTPError, URLError) as exc:
        temp_path.unlink(missing_ok=True)
        raise RuntimeError(
            "Could not download WorldPop population data. "
            f"country={country_iso3!r} year={year} url={worldpop_population_url(country_iso3, year)}"
        ) from exc

    return target_path


@lru_cache(maxsize=1)
def planetary_catalog() -> Client:
    return Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )


@lru_cache(maxsize=4)
def load_olmoearth_model(model_name: str, device_type: str = "cpu") -> torch.nn.Module:
    if device_type == "cpu":
        threads_override = os.environ.get("OLMOEARTH_TORCH_NUM_THREADS")
        if threads_override is not None:
            try:
                torch_threads = max(1, int(threads_override))
            except ValueError:
                torch_threads = max(1, (os_cpu_count() or 2) - 1)
        else:
            torch_threads = max(1, (os_cpu_count() or 2) - 1)
        torch.set_num_threads(torch_threads)
    model = load_model_from_id(model_id_from_name(model_name))
    model.to(torch.device(device_type))
    model.eval()
    return model


def resolve_torch_device(device_preference: str = "auto") -> torch.device:
    pref = device_preference.lower()
    if pref == "auto":
        if torch.cuda.is_available():
            return torch.device("cuda")
        return torch.device("cpu")
    if pref == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA was requested but no CUDA device is available.")
    if pref not in {"cpu", "cuda"}:
        raise ValueError("device must be one of: auto, cpu, cuda")
    return torch.device(pref)


def os_cpu_count() -> int | None:
    try:
        return os.cpu_count()
    except Exception:
        return None


def model_id_from_name(model_name: str) -> ModelID:
    mapping = {
        "nano": ModelID.OLMOEARTH_V1_NANO,
        "tiny": ModelID.OLMOEARTH_V1_TINY,
        "base": ModelID.OLMOEARTH_V1_BASE,
        "large": ModelID.OLMOEARTH_V1_LARGE,
    }
    try:
        return mapping[model_name.lower()]
    except KeyError as exc:
        raise ValueError(f"Unsupported model_name {model_name!r}.") from exc


def embedding_dim_for_model(model_name: str) -> int:
    mapping = {
        "nano": 128,
        "tiny": 192,
        "base": 768,
        "large": 1024,
    }
    return mapping[model_name.lower()]
