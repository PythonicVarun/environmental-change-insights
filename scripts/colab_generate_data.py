"""
Example usage inside Colab:

    !python scripts/colab_generate_data.py \
      --state "Uttar Pradesh" \
      --district "Gautam Buddha Nagar" \
      --output-dir /content/outputs/noida \
      --periods 1 5 \
      --zip-output
"""

import argparse
import importlib
import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path

INSTALL_PACKAGES = [
    "geopandas>=1.1.1",
    "numpy>=1.26.4",
    "olmoearth-pretrain>=0.1.0",
    "pandas>=2.2.3",
    "planetary-computer>=1.0.0",
    "pyproj>=3.7.1",
    "pystac-client>=0.9.0",
    "rasterio>=1.4.3",
    "shapely>=2.0.7",
    "stackstac>=0.5.1",
    "torch>=2.7,<2.8",
    "tqdm>=4.67.1",
    "xarray>=2025.3.1",
]

REQUIRED_MODULES = [
    "geopandas",
    "numpy",
    "olmoearth_pretrain",
    "pandas",
    "planetary_computer",
    "pyproj",
    "pystac_client",
    "rasterio",
    "shapely",
    "stackstac",
    "torch",
    "tqdm",
    "xarray",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--country", default="IND", help="ISO3 country code. Default: IND."
    )
    parser.add_argument("--state", help="State / ADM1 name.")
    parser.add_argument("--district", help="District / ADM2 name.")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--base-year", type=int, default=2025)
    parser.add_argument("--periods", type=int, nargs="+", default=[1, 5, 10])
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
    parser.add_argument("--max-tiles", type=int)
    parser.add_argument("--cache-dir", type=Path, default=Path(".cache"))
    parser.add_argument(
        "--installer",
        default="pip",
        choices=["pip", "uv"],
        help="Dependency installer to use when packages are missing.",
    )
    parser.add_argument(
        "--skip-install",
        action="store_true",
        help="Skip dependency bootstrap and assume the environment is ready.",
    )
    parser.add_argument(
        "--zip-output",
        action="store_true",
        help="Create a zip archive beside the output directory for easy Colab download.",
    )
    return parser.parse_args()


def ensure_dependencies(installer: str) -> None:
    missing = [
        name for name in REQUIRED_MODULES if importlib.util.find_spec(name) is None
    ]
    if not missing:
        return

    print(f"Installing missing packages for modules: {', '.join(missing)}")
    if installer == "uv":
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "uv"],
            check=True,
        )
        subprocess.run(
            ["uv", "pip", "install", "--system", *INSTALL_PACKAGES],
            check=True,
        )
    else:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", *INSTALL_PACKAGES],
            check=True,
        )
    importlib.invalidate_caches()


def add_repo_src_to_path() -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    src_dir = repo_root / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return repo_root


def maybe_zip_output(output_dir: Path) -> Path:
    archive_base = str(output_dir)
    archive_path = Path(shutil.make_archive(archive_base, "zip", root_dir=output_dir))
    return archive_path


def main() -> None:
    args = parse_args()

    if not args.skip_install:
        ensure_dependencies(args.installer)

    add_repo_src_to_path()

    from olmoearth_change.pipeline import AnalysisConfig, run_analysis

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
    )

    summary = run_analysis(config)
    print(json.dumps(summary, indent=2))
    print(f"\nWrote outputs to {args.output_dir}")
    if args.max_tiles is not None:
        print(
            "Partial coverage mode was used because --max-tiles was set. "
            "Remove that flag for a full district/state boundary-shaped overlay."
        )

    if args.zip_output:
        archive_path = maybe_zip_output(args.output_dir)
        print(f"Created zip archive: {archive_path}")
        print("Colab download snippet:")
        print("from google.colab import files")
        print(f"files.download({archive_path.as_posix()!r})")


if __name__ == "__main__":
    main()
