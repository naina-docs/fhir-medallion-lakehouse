from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Iterator

import requests


@dataclass
class PageResult:
    resource: str
    page_number: int
    request_url: str
    request_params: dict
    status_code: int
    body: str
    entry_count: int
    next_url: str | None
    called_at: datetime
    duration_ms: int
    run_id: str = ""


@dataclass
class FhirClient:
    base_url: str
    page_size: int = 100
    max_pages: int = 50
    timeout: int = 60
    max_retries: int = 5
    backoff: float = 2.0
    fmt: str = "json"
    headers: dict = field(default_factory=dict)
    session: requests.Session = field(default_factory=requests.Session)

    @classmethod
    def from_config(cls, cfg: dict) -> "FhirClient":
        api = cfg["api"]
        return cls(
            base_url=api["base_url"].rstrip("/"),
            page_size=api["page_size"],
            max_pages=api["max_pages_per_window"],
            timeout=api["timeout_seconds"],
            max_retries=api["max_retries"],
            backoff=api["backoff_seconds"],
            fmt=api.get("format", "json"),
            headers=dict(api.get("headers", {})),
        )

    def _accept(self) -> str:
        return "application/fhir+xml" if self.fmt == "xml" else "application/fhir+json"

    def _get(self, url: str, params: dict | None) -> requests.Response:
        headers = {**self.headers, "Accept": self._accept()}
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
                resp.raise_for_status()
                return resp
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError):
                if attempt == self.max_retries:
                    raise
                time.sleep(self.backoff * 2 ** (attempt - 1))
        raise RuntimeError("unreachable")

    def window_params(self, start: datetime, end: datetime) -> dict:
        return {
            "_lastUpdated": [f"ge{start.isoformat()}", f"lt{end.isoformat()}"],
            "_count": self.page_size,
            "_sort": "_lastUpdated",
            "_format": self.fmt,
        }

    def fetch_window(self, resource: str, start: datetime, end: datetime, run_id: str | None = None) -> Iterator[PageResult]:
        run_id = run_id or uuid.uuid4().hex
        url: str | None = f"{self.base_url}/{resource}"
        params: dict | None = self.window_params(start, end)
        page = 0
        while url and page < self.max_pages:
            page += 1
            called_at = datetime.now(timezone.utc)
            t0 = time.monotonic()
            resp = self._get(url, params)
            entries, next_url = self._parse_page(resp.text)
            yield PageResult(
                resource=resource,
                page_number=page,
                request_url=resp.url,
                request_params=params or {},
                status_code=resp.status_code,
                body=resp.text,
                entry_count=entries,
                next_url=next_url,
                called_at=called_at,
                duration_ms=int((time.monotonic() - t0) * 1000),
                run_id=run_id,
            )
            url, params = next_url, None

    def _parse_page(self, body: str) -> tuple[int, str | None]:
        if self.fmt == "xml":
            import xml.etree.ElementTree as ET

            ns = {"f": "http://hl7.org/fhir"}
            root = ET.fromstring(body)
            entries = len(root.findall("f:entry", ns))
            nxt = None
            for link in root.findall("f:link", ns):
                if link.find("f:relation", ns).get("value") == "next":
                    nxt = link.find("f:url", ns).get("value")
            return entries, nxt

        import json

        bundle = json.loads(body)
        nxt = next((l["url"] for l in bundle.get("link", []) if l.get("relation") == "next"), None)
        return len(bundle.get("entry", [])), nxt


def daily_windows(end_date: date, lookback_days: int, start_after: datetime | None = None) -> list[tuple[datetime, datetime]]:
    windows = []
    for offset in range(lookback_days, 0, -1):
        day = end_date - timedelta(days=offset)
        start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        if start_after and end <= start_after:
            continue
        if start_after and start < start_after:
            start = start_after
        windows.append((start, end))
    return windows
