from datetime import date, datetime, timezone
from unittest import mock

import pytest
import requests

from conftest import FIXTURES
from fhir_lakehouse.api_client import FhirClient, daily_windows
from fhir_lakehouse.bronze import split_xml_bundle

UTC = timezone.utc


def response(code: int, body: str = "{}") -> requests.Response:
    r = requests.Response()
    r.status_code, r.url, r._content = code, "https://test", body.encode()
    return r


def test_daily_windows_cover_lookback_days():
    windows = daily_windows(date(2026, 11, 12), 3)
    assert [w[0].date() for w in windows] == [date(2026, 11, 9), date(2026, 11, 10), date(2026, 11, 11)]
    assert all((end - start).days == 1 for start, end in windows)


def test_daily_windows_resume_from_watermark():
    wm = datetime(2026, 11, 10, 6, 0, tzinfo=UTC)
    windows = daily_windows(date(2026, 11, 12), 3, wm)
    assert windows[0] == (wm, datetime(2026, 11, 11, tzinfo=UTC))
    assert len(windows) == 2
    assert daily_windows(date(2026, 11, 12), 3, datetime(2026, 11, 12, tzinfo=UTC)) == []


def test_window_params():
    client = FhirClient(base_url="https://test", page_size=25, sort_param="_lastUpdated")
    params = client.window_params(datetime(2026, 11, 9, tzinfo=UTC), datetime(2026, 11, 10, tzinfo=UTC))
    assert params["_lastUpdated"] == ["ge2026-11-09T00:00:00+00:00", "lt2026-11-10T00:00:00+00:00"]
    assert params["_count"] == 25 and params["_sort"] == "_lastUpdated"


@pytest.mark.parametrize("fmt", ["json", "xml"])
def test_parse_page(fmt):
    client = FhirClient(base_url="https://test", fmt=fmt)
    count, nxt, latest = client._parse_page((FIXTURES / f"patient_bundle.{fmt}").read_text())
    assert count == 2
    assert "_getpages=abc" in nxt
    assert latest == datetime(2026, 11, 10, 9, 30, tzinfo=UTC)


def test_split_xml_bundle():
    rows = split_xml_bundle((FIXTURES / "patient_bundle.xml").read_text())
    assert [(r[2], r[3], r[4]) for r in rows] == [("Patient", "p1", "1"), ("Patient", "p2", "3")]
    assert rows[0][6].startswith('<Patient xmlns="http://hl7.org/fhir">')


def test_client_errors_are_not_retried():
    client = FhirClient(base_url="https://test", max_retries=3, backoff=0)
    with mock.patch.object(client.session, "get", side_effect=[response(404)]) as get:
        with pytest.raises(requests.HTTPError):
            client._get("https://test", None)
    assert get.call_count == 1


def test_server_errors_are_retried():
    client = FhirClient(base_url="https://test", max_retries=3, backoff=0)
    with mock.patch.object(client.session, "get", side_effect=[response(503), response(429), response(200)]) as get:
        assert client._get("https://test", None).status_code == 200
    assert get.call_count == 3


def test_fetch_window_follows_next_links_up_to_cap():
    body = (FIXTURES / "patient_bundle.json").read_text()
    client = FhirClient(base_url="https://test", max_pages=3, backoff=0)
    with mock.patch.object(client.session, "get", side_effect=[response(200, body)] * 5) as get:
        pages = list(client.fetch_window("Patient", datetime(2026, 11, 9, tzinfo=UTC),
                                         datetime(2026, 11, 10, tzinfo=UTC), "r1", first_page=4))
    assert [p.page_number for p in pages] == [4, 5, 6]
    assert get.call_count == 3
    assert get.call_args_list[1].kwargs["params"] is None
