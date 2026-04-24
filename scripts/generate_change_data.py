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
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory where rasters, GeoJSON, report, and UI files will be written.",
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
    parser.add_argument("--crop-size", type=int, default=128)
    parser.add_argument("--patch-size", type=int, default=4)
    parser.add_argument("--display-aggregation", type=int, default=4)
    parser.add_argument("--cloud-max", type=int, default=40)
    parser.add_argument("--fill-holes-pixels", type=int, default=48)
    parser.add_argument("--device", default="auto", choices=["auto", "cpu", "cuda"])
    parser.add_argument(
        "--skip-population",
        action="store_true",
        help="Skip WorldPop population overlays if you want the fastest possible run.",
    )
    parser.add_argument(
        "--skip-pollution",
        action="store_true",
        help="Skip the Sentinel-2 aerosol pollution proxy if you want the fastest possible run.",
    )
    parser.add_argument(
        "--skip-wards",
        action="store_true",
        help="Skip ward-level overlay generation and keep only the cell overlay.",
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
    config = AnalysisConfig(
        country_iso3=args.country,
        state_name=args.state,
        district_name=args.district,
        output_dir=args.output_dir,
        cache_dir=args.cache_dir,
        model_name=args.model,
        base_year=args.base_year,
        periods=tuple(args.periods),
        tile_size_m=args.tile_size_m,
        crop_size=args.crop_size,
        patch_size=args.patch_size,
        display_aggregation=args.display_aggregation,
        cloud_max=args.cloud_max,
        fill_holes_pixels=args.fill_holes_pixels,
        max_tiles=args.max_tiles,
        device=args.device,
        include_population=not args.skip_population,
        include_pollution=not args.skip_pollution,
        include_ward_overlay=not args.skip_wards,
    )
    summary = run_analysis(config)
    print(json.dumps(summary, indent=2))
    print(f"\nWrote outputs to {args.output_dir}")
    print(f"Serve {args.output_dir} with a local web server before opening the UI.")
    print("Example: cd OUTPUT_DIR && python -m http.server 8000")
    print(f"Then open http://localhost:8000/ui/ or use ?basemap=none.")


if __name__ == "__main__":
    main()
