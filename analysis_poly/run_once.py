from __future__ import annotations

import argparse
import asyncio
import json
import uuid
from datetime import datetime
from pathlib import Path

if __package__ in (None, ""):
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from analysis_poly.analyzer import PolymarketProfitAnalyzer
    from analysis_poly.logging_config import configure_logging
    from analysis_poly.models import AnalysisRequest, RunStatus
    from analysis_poly.run_manager import build_analysis_result_payload
else:
    from .analyzer import PolymarketProfitAnalyzer
    from .logging_config import configure_logging
    from .models import AnalysisRequest, RunStatus
    from .run_manager import build_analysis_result_payload


def _parse_datetime_text(value: str) -> int:
    parsed = datetime.strptime(value.strip(), "%Y-%m-%d %H:%M")
    return int(parsed.timestamp())


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run analysis once and print the final JSON result")
    parser.add_argument("--address", required=True)
    parser.add_argument("--keywords", default="", help='Comma-separated slug keywords, e.g. "updown,15m"')
    parser.add_argument("--start-time", help='Local datetime, format "YYYY-MM-DD HH:MM"')
    parser.add_argument("--end-time", help='Local datetime, format "YYYY-MM-DD HH:MM"')
    parser.add_argument("--start-ts", type=int, help="Unix timestamp (seconds), used when --start-time is missing")
    parser.add_argument("--end-ts", type=int, help="Unix timestamp (seconds), used when --end-time is missing")
    parser.add_argument("--fee-rate-bps", type=float, default=1000.0)
    parser.add_argument("--missing-cost-warn-qty", type=float, default=0.5)
    parser.add_argument("--activity-window-sec", type=int, default=2 * 60 * 60)
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--page-limit", type=int, default=1000)
    parser.add_argument("--request-timeout-sec", type=float, default=20.0)
    parser.add_argument("--output-dir", default="reports")
    parser.add_argument("--indent", type=int, default=2, help="JSON indent level for stdout output")
    parser.add_argument("--log-level", default="WARNING", help="Logger level, default WARNING")
    return parser


def _request_from_args(args: argparse.Namespace) -> AnalysisRequest:
    if args.start_time:
        start_ts = _parse_datetime_text(args.start_time)
    elif args.start_ts is not None:
        start_ts = int(args.start_ts)
    else:
        raise ValueError("either --start-time or --start-ts is required")

    if args.end_time:
        end_ts = _parse_datetime_text(args.end_time)
    elif args.end_ts is not None:
        end_ts = int(args.end_ts)
    else:
        raise ValueError("either --end-time or --end-ts is required")

    return AnalysisRequest(
        address=args.address,
        start_ts=start_ts,
        end_ts=end_ts,
        keywords=[item.strip().lower() for item in str(args.keywords).split(",") if item.strip()],
        fee_rate_bps=args.fee_rate_bps,
        missing_cost_warn_qty=args.missing_cost_warn_qty,
        activity_window_sec=args.activity_window_sec,
        page_limit=args.page_limit,
        concurrency=args.concurrency,
        request_timeout_sec=args.request_timeout_sec,
        output_dir=args.output_dir,
    )


async def _run_once(req: AnalysisRequest) -> dict:
    analyzer = PolymarketProfitAnalyzer()
    report = await analyzer.run(req)

    output_dir = Path(req.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex
    suffix = f"{run_id}_{'partial' if report.is_partial else 'final'}"

    json_path = analyzer.save_json(report, str(output_dir / f"pnl_summary_{suffix}.json"))
    total_csv_path = analyzer.save_total_curve_csv(report, str(output_dir / f"pnl_total_curve_{suffix}.csv"))
    market_csv_path = analyzer.save_market_curve_csv(report, str(output_dir / f"pnl_market_curve_{suffix}.csv"))
    report.artifacts = {
        "json": json_path,
        "total_curve_csv": total_csv_path,
        "market_curve_csv": market_csv_path,
    }

    status = RunStatus.STOPPED if report.is_partial else RunStatus.COMPLETED
    return {
        "status": status,
        "result": build_analysis_result_payload(report, report.artifacts),
    }


def main() -> None:
    args = _build_arg_parser().parse_args()
    configure_logging(level=str(args.log_level).upper())
    req = _request_from_args(args)
    payload = asyncio.run(_run_once(req))
    print(json.dumps(payload, ensure_ascii=False, indent=args.indent))


if __name__ == "__main__":
    main()
