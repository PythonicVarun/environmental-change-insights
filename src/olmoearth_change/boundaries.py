import json
import re
import unicodedata
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import geopandas as gpd
from shapely.geometry.base import BaseGeometry

GEOBOUNDARIES_ENDPOINT = (
    "https://www.geoboundaries.org/api/current/gbOpen/{country}/{adm}/"
)


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
