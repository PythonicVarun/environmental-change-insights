import json
import math
import shutil
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any

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
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry
from tqdm import tqdm

from .boundaries import ResolvedBoundary, resolve_admin_boundary

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
    device: str = "auto"
    save_composites: bool = True
    save_embedding_change_rasters: bool = True

    def __post_init__(self) -> None:
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
    scene_count: int


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
    write_boundary_geojson(boundary, config.output_dir / "boundary.geojson")

    tiles = build_tiles(boundary, config)
    required_years = sorted(
        {config.base_year, *[config.base_year - period for period in config.periods]}
    )
    year_results: dict[int, dict[str, TileYearData]] = {
        year: {} for year in required_years
    }

    for year in required_years:
        for tile in tqdm(
            tiles.itertuples(index=False),
            total=len(tiles),
            desc=f"Processing {year}",
        ):
            tile_result = process_tile_year(
                tile_id=str(tile.tile_id),
                tile_geometry=tile.geometry,
                tile_crs=str(tiles.crs),
                year=year,
                boundary=boundary,
                config=config,
            )
            if tile_result is not None:
                year_results[year][str(tile.tile_id)] = tile_result

    overlay = build_overlay(boundary, tiles, year_results, config)
    overlay_path = config.output_dir / "overlay.geojson"
    overlay.to_file(overlay_path, driver="GeoJSON")
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

    if composite_path.exists():
        composite, transform, crs = read_raster(composite_path)
        scene_count = int(read_tags(composite_path).get("scene_count", "0"))
    else:
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

    return TileYearData(
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
        scene_count=scene_count,
    )


def fetch_sentinel2_composite(
    *,
    tile_geometry: BaseGeometry,
    tile_crs: str,
    year: int,
    config: AnalysisConfig,
) -> tuple[np.ndarray, Affine, str, int] | None:
    catalog = planetary_catalog()
    bounds = tile_geometry.bounds
    bbox_latlon = (
        gpd.GeoSeries([tile_geometry], crs=tile_crs)
        .to_crs("EPSG:4326")
        .total_bounds.tolist()
    )
    datetime_range = f"{year}-01-01/{year}-12-31"

    items = None
    for cloud_cap in [config.cloud_max, 60, 80]:
        search = catalog.search(
            collections=["sentinel-2-l2a"],
            bbox=bbox_latlon,
            datetime=datetime_range,
            query={"eo:cloud_cover": {"lt": cloud_cap}},
        )
        found = list(search.items())
        if found:
            items = found
            break

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
        periods[f"{period}y"] = {
            "metrics": {
                "embedding_change_median": round(float(frame[key].median()), 6),
                "embedding_change_p95": round(float(frame[key].quantile(0.95)), 6),
                "vegetation_delta_mean": round(
                    float(frame[f"vegetation_delta_{period}y"].mean()), 6
                ),
                "water_delta_mean": round(
                    float(frame[f"water_delta_{period}y"].mean()), 6
                ),
                "urban_delta_mean": round(
                    float(frame[f"urban_delta_{period}y"].mean()), 6
                ),
                "bare_soil_delta_mean": round(
                    float(frame[f"bare_soil_delta_{period}y"].mean()), 6
                ),
            },
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
                    "story": str(row[f"story_{period}y"]),
                    "tile_id": str(row["tile_id"]),
                }
                for _, row in hotspots.iterrows()
            ],
        }

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
                ),
                f"- Dominant stories: {', '.join(f'{k} ({v})' for k, v in period_summary['story_counts'].items())}",
                "",
            ]
        )
        for hotspot in period_summary["hotspots"][:5]:
            lines.append(
                (
                    f"- Hotspot near ({hotspot['latitude']}, {hotspot['longitude']}): "
                    f"{hotspot['story']} | embedding {hotspot['embedding_change']:.4f}, "
                    f"veg {hotspot['vegetation_delta']:.4f}, water {hotspot['water_delta']:.4f}, "
                    f"urban {hotspot['urban_delta']:.4f}, bare soil {hotspot['bare_soil_delta']:.4f}"
                )
            )
        lines.append("")
    return "\n".join(lines)


def copy_ui_bundle(output_dir: Path) -> None:
    source_dir = Path(__file__).resolve().parents[2] / "app"
    target_dir = output_dir
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


@lru_cache(maxsize=1)
def planetary_catalog() -> Client:
    return Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )


@lru_cache(maxsize=4)
def load_olmoearth_model(model_name: str, device_type: str = "cpu") -> torch.nn.Module:
    if device_type == "cpu":
        torch.set_num_threads(max(1, (os_cpu_count() or 2) - 1))
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
        import os

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
