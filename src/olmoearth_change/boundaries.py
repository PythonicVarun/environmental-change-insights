import json
import re
import unicodedata
import urllib.request
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import geopandas as gpd
import osmnx as ox
import pandas as pd
from shapely.geometry.base import BaseGeometry

GEOBOUNDARIES_ENDPOINT = (
    "https://www.geoboundaries.org/api/current/gbOpen/{country}/{adm}/"
)
WARD_ADMIN_LEVELS = {"9", "10", "11"}
WARD_NAME_HINTS = ("ward", "division", "circle")


@dataclass(frozen=True)
class ResolvedBoundary:
    country_iso3: str
    admin_level: str
    state_name: str | None
    district_name: str | None
    label: str
    geometry: BaseGeometry
    area_sq_km: float


def _normalize_name(value: str) -> str:
    ascii_value = (
        unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    )
    return re.sub(r"[^a-z0-9]+", "", ascii_value.lower())


def _cache_path(cache_dir: Path, country_iso3: str, adm_level: str) -> Path:
    return (
        cache_dir
        / "geoboundaries"
        / f"{country_iso3.upper()}_{adm_level.upper()}.geojson"
    )


def _download_metadata(country_iso3: str, adm_level: str) -> dict[str, Any]:
    url = GEOBOUNDARIES_ENDPOINT.format(
        country=country_iso3.upper(),
        adm=adm_level.upper(),
    )
    with urllib.request.urlopen(url) as response:
        return json.load(response)


def _ensure_boundary_file(
    cache_dir: Path,
    country_iso3: str,
    adm_level: str,
) -> Path:
    out_path = _cache_path(cache_dir, country_iso3, adm_level)
    if out_path.exists():
        return out_path

    out_path.parent.mkdir(parents=True, exist_ok=True)
    metadata = _download_metadata(country_iso3, adm_level)
    urllib.request.urlretrieve(metadata["gjDownloadURL"], out_path)
    return out_path


def _load_boundary_layer(
    cache_dir: Path,
    country_iso3: str,
    adm_level: str,
) -> gpd.GeoDataFrame:
    path = _ensure_boundary_file(cache_dir, country_iso3, adm_level)
    return _load_boundary_layer_cached(str(path.resolve()))


@lru_cache(maxsize=32)
def _load_boundary_layer_cached(path: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path).to_crs("EPSG:4326")
    gdf["__norm_name"] = gdf["shapeName"].map(_normalize_name)
    return gdf


def _resolve_exact_or_close(
    gdf: gpd.GeoDataFrame,
    column: str,
    value: str,
) -> gpd.GeoDataFrame:
    normalized = _normalize_name(value)
    exact = gdf[gdf[column] == normalized]
    if not exact.empty:
        return exact

    contains = gdf[gdf[column].str.contains(normalized, regex=False)]
    if not contains.empty:
        return contains

    raise ValueError(f"Could not find a boundary matching {value!r}.")


def resolve_admin_boundary(
    *,
    country_iso3: str,
    cache_dir: Path,
    state_name: str | None = None,
    district_name: str | None = None,
) -> ResolvedBoundary:
    country_iso3 = country_iso3.upper()
    if district_name:
        adm2 = _load_boundary_layer(cache_dir, country_iso3, "ADM2")
        district_matches = _resolve_exact_or_close(adm2, "__norm_name", district_name)

        matched_state_name: str | None = None
        if state_name:
            adm1 = _load_boundary_layer(cache_dir, country_iso3, "ADM1")
            state_matches = _resolve_exact_or_close(adm1, "__norm_name", state_name)
            if len(state_matches) != 1:
                raise ValueError(
                    f"State match for {state_name!r} is ambiguous ({len(state_matches)} rows)."
                )
            state_row = state_matches.iloc[0]
            matched_state_name = str(state_row["shapeName"])
            points = district_matches.geometry.representative_point()
            district_matches = district_matches[points.within(state_row.geometry)]
            if district_matches.empty:
                raise ValueError(
                    f"District {district_name!r} was found, but none of the matches sit inside {state_name!r}."
                )

        if len(district_matches) > 1:
            candidate_names = ", ".join(
                sorted(district_matches["shapeName"].astype(str))
            )
            raise ValueError(
                f"District match for {district_name!r} is ambiguous. Candidates: {candidate_names}"
            )

        row = district_matches.iloc[0]
        geometry = row.geometry
        label_parts = [str(row["shapeName"])]
        if matched_state_name:
            label_parts.append(matched_state_name)
        label_parts.append(country_iso3)
        area_sq_km = (
            gpd.GeoSeries([geometry], crs="EPSG:4326")
            .to_crs(_local_equal_area_crs())
            .area.iloc[0]
            / 1_000_000.0
        )
        return ResolvedBoundary(
            country_iso3=country_iso3,
            admin_level="ADM2",
            state_name=matched_state_name or state_name,
            district_name=str(row["shapeName"]),
            label=", ".join(label_parts),
            geometry=geometry,
            area_sq_km=float(area_sq_km),
        )

    if state_name:
        adm1 = _load_boundary_layer(cache_dir, country_iso3, "ADM1")
        state_matches = _resolve_exact_or_close(adm1, "__norm_name", state_name)
        if len(state_matches) > 1:
            candidate_names = ", ".join(sorted(state_matches["shapeName"].astype(str)))
            raise ValueError(
                f"State match for {state_name!r} is ambiguous. Candidates: {candidate_names}"
            )
        row = state_matches.iloc[0]
        geometry = row.geometry
        area_sq_km = (
            gpd.GeoSeries([geometry], crs="EPSG:4326")
            .to_crs(_local_equal_area_crs())
            .area.iloc[0]
            / 1_000_000.0
        )
        return ResolvedBoundary(
            country_iso3=country_iso3,
            admin_level="ADM1",
            state_name=str(row["shapeName"]),
            district_name=None,
            label=f"{row['shapeName']}, {country_iso3}",
            geometry=geometry,
            area_sq_km=float(area_sq_km),
        )

    raise ValueError("Please provide at least a state_name or district_name.")


def _local_equal_area_crs() -> str:
    return "EPSG:6933"


def resolve_ward_boundaries(
    *,
    boundary: ResolvedBoundary,
    cache_dir: Path,
) -> gpd.GeoDataFrame:
    """Resolve ward-like administrative polygons inside a district boundary.

    This is best-effort and currently uses OSM administrative polygons as the
    source. If no reasonable ward-like polygons are available, an empty frame is
    returned so the rest of the pipeline can fall back to cell overlays.
    """

    if not boundary.district_name:
        return gpd.GeoDataFrame(
            columns=["ward_name", "admin_level", "geometry"], crs="EPSG:4326"
        )

    cache_path = _ward_cache_path(cache_dir, boundary)
    if cache_path.exists():
        return gpd.read_file(cache_path).to_crs("EPSG:4326")

    wards = _download_osm_ward_boundaries(boundary, cache_dir)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if wards.empty:
        cache_path.write_text(
            json.dumps({"type": "FeatureCollection", "features": []}, indent=2)
        )
        return wards

    wards.to_file(cache_path, driver="GeoJSON")
    return wards


def _ward_cache_path(cache_dir: Path, boundary: ResolvedBoundary) -> Path:
    state_part = _normalize_name(boundary.state_name or "state")
    district_part = _normalize_name(boundary.district_name or boundary.label)
    return (
        cache_dir
        / "wards"
        / "osm"
        / f"{boundary.country_iso3.upper()}_{state_part}_{district_part}.geojson"
    )


def _download_osm_ward_boundaries(
    boundary: ResolvedBoundary,
    cache_dir: Path,
) -> gpd.GeoDataFrame:
    ox.settings.use_cache = True
    ox.settings.cache_folder = str((cache_dir / "osmnx").resolve())
    features = ox.features_from_polygon(
        boundary.geometry, tags={"boundary": "administrative"}
    )
    if features.empty:
        return gpd.GeoDataFrame(
            columns=["ward_name", "admin_level", "geometry"], crs="EPSG:4326"
        )

    features = features.reset_index(drop=True).to_crs("EPSG:4326")
    features = features[
        features.geometry.geom_type.isin(["Polygon", "MultiPolygon"])
    ].copy()
    if features.empty:
        return gpd.GeoDataFrame(
            columns=["ward_name", "admin_level", "geometry"], crs="EPSG:4326"
        )

    english_names = (
        features["name:en"]
        if "name:en" in features.columns
        else pd.Series([None] * len(features), index=features.index, dtype="object")
    )
    local_names = (
        features["name"]
        if "name" in features.columns
        else pd.Series([None] * len(features), index=features.index, dtype="object")
    )
    admin_levels = (
        features["admin_level"]
        if "admin_level" in features.columns
        else pd.Series([""] * len(features), index=features.index, dtype="object")
    )
    features["ward_name"] = (
        english_names.fillna(local_names).astype("string").str.strip()
    )
    features["admin_level"] = admin_levels.astype("string")
    features = features[features["ward_name"].notna() & (features["ward_name"] != "")]
    if features.empty:
        return gpd.GeoDataFrame(
            columns=["ward_name", "admin_level", "geometry"], crs="EPSG:4326"
        )

    boundary_proj = gpd.GeoDataFrame(
        [{"geometry": boundary.geometry}], crs="EPSG:4326"
    ).to_crs(_local_equal_area_crs())
    boundary_geom_proj = boundary_proj.geometry.iloc[0]
    features_proj = features.to_crs(_local_equal_area_crs())
    features_proj["feature_area_sq_m"] = features_proj.geometry.area
    features_proj["intersection_area_sq_m"] = features_proj.geometry.intersection(
        boundary_geom_proj
    ).area
    features_proj = features_proj[features_proj["intersection_area_sq_m"] > 0].copy()
    if features_proj.empty:
        return gpd.GeoDataFrame(
            columns=["ward_name", "admin_level", "geometry"], crs="EPSG:4326"
        )

    boundary_area = float(boundary_geom_proj.area)
    name_hints = (
        features_proj["ward_name"]
        .str.lower()
        .str.contains(
            "|".join(WARD_NAME_HINTS),
            regex=True,
            na=False,
        )
    )
    admin_level_match = features_proj["admin_level"].isin(WARD_ADMIN_LEVELS)
    mostly_inside = (
        features_proj["intersection_area_sq_m"]
        / features_proj["feature_area_sq_m"].clip(lower=1.0)
        >= 0.5
    )
    smaller_than_boundary = (
        features_proj["intersection_area_sq_m"] < boundary_area * 0.6
    )
    features_proj = features_proj[
        mostly_inside & smaller_than_boundary & (admin_level_match | name_hints)
    ].copy()
    if features_proj.empty:
        return gpd.GeoDataFrame(
            columns=["ward_name", "admin_level", "geometry"], crs="EPSG:4326"
        )

    district_norm = _normalize_name(boundary.district_name or "")
    state_norm = _normalize_name(boundary.state_name or "")
    features_proj["ward_norm_name"] = features_proj["ward_name"].map(_normalize_name)
    features_proj = features_proj[
        ~features_proj["ward_norm_name"].isin({district_norm, state_norm})
    ].copy()
    if features_proj.empty:
        return gpd.GeoDataFrame(
            columns=["ward_name", "admin_level", "geometry"], crs="EPSG:4326"
        )

    clipped = gpd.overlay(
        features_proj[["ward_name", "admin_level", "ward_norm_name", "geometry"]],
        boundary_proj[["geometry"]],
        how="intersection",
        keep_geom_type=False,
    )
    if clipped.empty:
        return gpd.GeoDataFrame(
            columns=["ward_name", "admin_level", "geometry"], crs="EPSG:4326"
        )

    clipped = clipped[
        clipped.geometry.geom_type.isin(["Polygon", "MultiPolygon"])
    ].copy()
    if clipped.empty:
        return gpd.GeoDataFrame(
            columns=["ward_name", "admin_level", "geometry"], crs="EPSG:4326"
        )

    clipped["clip_area_sq_m"] = clipped.geometry.area
    clipped = clipped[clipped["clip_area_sq_m"] > 25_000].copy()
    if clipped.empty:
        return gpd.GeoDataFrame(
            columns=["ward_name", "admin_level", "geometry"], crs="EPSG:4326"
        )

    wards = (
        clipped.dissolve(
            by=["ward_norm_name", "ward_name", "admin_level"],
            as_index=False,
        )[["ward_name", "admin_level", "geometry"]]
        .to_crs("EPSG:4326")
        .reset_index(drop=True)
    )
    wards["ward_id"] = [f"ward_{idx:03d}" for idx in range(len(wards))]
    return wards[["ward_id", "ward_name", "admin_level", "geometry"]]
