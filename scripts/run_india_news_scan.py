import argparse
import json
import re
from pathlib import Path

from olmoearth_change.pipeline import AnalysisConfig, run_analysis


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/target.json"),
        help="JSON file listing state/district targets.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/india-news-scan"),
    )
    parser.add_argument("--base-year", type=int, default=2025)
    parser.add_argument(
        "--model", default="tiny", choices=["nano", "tiny", "base", "large"]
    )
    parser.add_argument(
        "--max-tiles",
        type=int,
        default=2,
        help="Keep this small for a quick pilot scan across many locations.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    targets = json.loads(args.config.read_text())
    summaries = []

    for target in targets:
        label = f"{target['district']}, {target['state']}"
        output_dir = args.output_dir / slugify(label)
        summary = run_analysis(
            AnalysisConfig(
                country_iso3=target.get("country", "IND"),
                state_name=target["state"],
                district_name=target["district"],
                output_dir=output_dir,
                model_name=args.model,
                base_year=args.base_year,
                max_tiles=args.max_tiles,
            )
        )
        summaries.append(summary)

    lines = ["# India Multi-Location OlmoEarth Scan", ""]
    for summary in summaries:
        label = summary["metadata"]["label"]
        lines.append(f"## {label}")
        for period_key, period_summary in summary["periods"].items():
            top_story = next(iter(period_summary["story_counts"].items()))
            lines.append(
                (
                    f"- {period_key}: median embedding change "
                    f"{period_summary['metrics']['embedding_change_median']:.4f}, "
                    f"p95 {period_summary['metrics']['embedding_change_p95']:.4f}, "
                    f"dominant story {top_story[0]} ({top_story[1]} cells)"
                )
            )
        lines.append("")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    report_path = args.output_dir / "country_scan_report.md"
    report_path.write_text("\n".join(lines))
    print(report_path)


if __name__ == "__main__":
    main()
