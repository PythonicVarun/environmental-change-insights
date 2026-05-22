import argparse
import json
from pathlib import Path

from olmoearth_change.pipeline import AnalysisConfig, run_analysis


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--country", default="IND", help="ISO3 country code. Default: IND."
    )
    parser.add_argument("--state", help="State / ADM1 name.")
    parser.add_argument("--district", help="District / ADM2 name.")
    parser.add_argument("--city", help="City name for whole-city generation.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory where outputs will be written.",
    )
    parser.add_argument("--base-year", type=int, default=2025)
    parser.add_argument(
        "--periods",
        type=int,
        nargs="+",
        default=[1, 5, 10],
        help="Lookback periods in years.",
    )
    parser.add_argument(
        "--model", default="tiny", choices=["nano", "tiny", "base", "large"]
    )
    parser.add_argument("--tile-size-m", type=int, default=2560)
    parser.add_argument(
        "--resolution-m",
        type=int,
        default=10,
        help="Spatial resolution for Sentinel-2 composites. 20 is much faster than 10.",
    )
    parser.add_argument("--crop-size", type=int, default=128)
    parser.add_argument("--patch-size", type=int, default=4)
    parser.add_argument("--display-aggregation", type=int, default=4)
    parser.add_argument("--cloud-max", type=int, default=40)
    parser.add_argument("--fill-holes-pixels", type=int, default=48)
    parser.add_argument("--device", default="auto", choices=["auto", "cpu", "cuda"])
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel tile workers for CPU runs.",
    )
    parser.add_argument(
        "--skip-population",
        action="store_true",
        help="Skip WorldPop population overlays.",
    )
    parser.add_argument(
        "--skip-pollution",
        action="store_true",
        help="Skip the Sentinel-2 aerosol pollution proxy.",
    )
    parser.add_argument(
        "--skip-wards",
        action="store_true",
        help="Skip ward-level overlay generation and keep only the cell overlay.",
    )
    parser.add_argument(
        "--no-save-composites",
        action="store_true",
        help="Do not write per-tile yearly composite GeoTIFFs to disk.",
    )
    parser.add_argument(
        "--skip-change-rasters",
        action="store_true",
        help="Skip writing per-tile embedding-change rasters to speed up I/O.",
    )
    parser.add_argument(
        "--skip-embeddings",
        action="store_true",
        help="Skip computing OlmoEarth embeddings to speed up processing.",
    )
    parser.add_argument(
        "--skip-ndvi",
        action="store_true",
        help="Skip computing NDVI (Normalized Difference Vegetation Index).",
    )
    parser.add_argument(
        "--skip-mndwi",
        action="store_true",
        help="Skip computing MNDWI (Modified Normalized Difference Water Index).",
    )
    parser.add_argument(
        "--skip-ndbi",
        action="store_true",
        help="Skip computing NDBI (Normalized Difference Built-up Index).",
    )
    parser.add_argument(
        "--skip-bsi",
        action="store_true",
        help="Skip computing BSI (Bare Soil Index).",
    )
    parser.add_argument(
        "--skip-all-metrics",
        action="store_true",
        help="Skip computing all metrics (embeddings, NDVI, MNDWI, NDBI, BSI).",
    )
    parser.add_argument(
        "--enable-historical-imagery",
        action="store_true",
        help="Export historical imagery previews and historical_imagery.json output.",
    )
    parser.add_argument(
        "--max-tiles",
        type=int,
        help="Optional cap for a faster pilot run. Omit to process the full district/state.",
    )
    parser.add_argument("--cache-dir", type=Path, default=Path(".cache"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    # If --skip-all-metrics is set, skip all metric computations
    if args.skip_all_metrics:
        args.skip_embeddings = True
        args.skip_ndvi = True
        args.skip_mndwi = True
        args.skip_ndbi = True
        args.skip_bsi = True
        args.skip_population = True
        args.skip_pollution = True

    config = AnalysisConfig(
        country_iso3=args.country,
        state_name=args.state,
        district_name=args.district,
        city_name=args.city,
        output_dir=args.output_dir,
        cache_dir=args.cache_dir,
        model_name=args.model,
        base_year=args.base_year,
        periods=tuple(args.periods),
        tile_size_m=args.tile_size_m,
        resolution_m=args.resolution_m,
        crop_size=args.crop_size,
        patch_size=args.patch_size,
        display_aggregation=args.display_aggregation,
        cloud_max=args.cloud_max,
        fill_holes_pixels=args.fill_holes_pixels,
        max_tiles=args.max_tiles,
        workers=args.workers,
        device=args.device,
        include_population=not args.skip_population,
        include_pollution=not args.skip_pollution,
        include_ward_overlay=not args.skip_wards,
        include_historical_imagery=args.enable_historical_imagery,
        save_composites=not args.no_save_composites,
        save_embedding_change_rasters=not args.skip_change_rasters,
        include_embeddings=not args.skip_embeddings,
        include_ndvi=not args.skip_ndvi,
        include_mndwi=not args.skip_mndwi,
        include_ndbi=not args.skip_ndbi,
        include_bsi=not args.skip_bsi,
    )
    summary = run_analysis(config)
    print(json.dumps(summary, indent=2))
    print(f"\nWrote outputs to {args.output_dir}")
    print(f"Serve {args.output_dir} with a local web server before opening the UI.")
    print("Example: cd OUTPUT_DIR && python -m http.server 8000")
    print("Then open http://localhost:8000/ui/ or use ?basemap=none.")


if __name__ == "__main__":
    main()
