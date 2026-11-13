from __future__ import annotations

import json
from datetime import date, datetime, timezone

from pyspark.sql import SparkSession

from .api_client import FhirClient, daily_windows
from .control import get_watermark, log_api_calls, set_watermark
from .storage import raw_dir, raw_file_name, write_raw


def _utc(ts: datetime) -> datetime:
    return ts.astimezone(timezone.utc)


def ingest_resource(spark: SparkSession, cfg: dict, resource: str, run_id: str, run_date: date,
                    client: FhirClient | None = None) -> dict:
    client = client or FhirClient.from_config(cfg)
    ing = cfg["ingestion"]
    watermark = get_watermark(spark, cfg, resource) if ing.get("use_watermark", True) else None
    windows = daily_windows(run_date, ing["lookback_days"], watermark)
    out_dir = raw_dir(cfg, resource, run_date)

    stats = {"resource": resource, "windows": len(windows), "pages": 0, "entries": 0, "resumed": 0,
             "incomplete": [], "files": []}
    for start, end in windows:
        log_rows, cursor, next_page = [], start, 1
        while True:
            last = None
            for page in client.fetch_window(resource, cursor, end, run_id, next_page):
                last = page
                log_rows.append(_log_row(page, run_id, resource, start, end, out_dir, client.fmt, stats))
            if not (last and last.next_url):
                break
            if not last.max_last_updated or last.max_last_updated <= cursor:
                stats["incomplete"].append(start.date().isoformat())
                break
            cursor, next_page = last.max_last_updated, last.page_number + 1
            stats["resumed"] += 1
        log_api_calls(spark, cfg, log_rows)
        if stats["incomplete"]:
            raise RuntimeError(f"{resource} {start.date()} has more rows at one timestamp than "
                               f"api.max_pages_per_window allows; increase it and rerun")
        set_watermark(spark, cfg, resource, _utc(end), run_id)
    return stats


def _log_row(page, run_id: str, resource: str, start: datetime, end: datetime, out_dir, fmt: str, stats: dict) -> dict:
    path = write_raw(out_dir / raw_file_name(run_id, start.date(), page.page_number, fmt), page.body)
    stats["pages"] += 1
    stats["entries"] += page.entry_count
    stats["files"].append(str(path))
    return {
        "run_id": run_id,
        "resource": resource,
        "window_start": _utc(start),
        "window_end": _utc(end),
        "page_number": page.page_number,
        "request_url": page.request_url,
        "request_params": json.dumps(page.request_params),
        "status_code": page.status_code,
        "entry_count": page.entry_count,
        "api_called_at": _utc(page.called_at),
        "duration_ms": page.duration_ms,
        "raw_file_path": str(path),
        "saved_at": _utc(datetime.now(timezone.utc)),
    }
