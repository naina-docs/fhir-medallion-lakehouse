import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures"
sys.path.insert(0, str(ROOT / "src"))

from fhir_lakehouse.api_client import PageResult  # noqa: E402
from fhir_lakehouse.config import load_config  # noqa: E402


def bundle(resources: list[dict], next_url: str | None = None) -> str:
    links = [{"relation": "next", "url": next_url}] if next_url else []
    return json.dumps({"resourceType": "Bundle", "link": links,
                       "entry": [{"fullUrl": f"{r['resourceType']}/{r['id']}", "resource": r} for r in resources]})


def resource(rtype: str, rid: str, version: int, updated: str, **fields) -> dict:
    return {"resourceType": rtype, "id": rid, "meta": {"versionId": str(version), "lastUpdated": updated}, **fields}


class FakeClient:
    fmt = "json"

    def __init__(self, pages: dict[str, list[str]]):
        self.pages = pages

    def fetch_window(self, resource_name, start, end, run_id=None, first_page=1):
        for i, body in enumerate(self.pages.get(resource_name, []), start=first_page):
            entries = json.loads(body)["entry"]
            inside = [e for e in entries
                      if start <= datetime.fromisoformat(e["resource"]["meta"]["lastUpdated"]) < end]
            if not inside:
                continue
            yield PageResult(resource=resource_name, page_number=i, request_url=f"https://test/{resource_name}",
                             request_params={"_count": 100}, status_code=200,
                             body=json.dumps({"resourceType": "Bundle", "entry": inside}),
                             entry_count=len(inside), next_url=None, called_at=datetime.now(timezone.utc),
                             duration_ms=1, run_id=run_id or "")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    delta = pytest.importorskip("delta")
    from pyspark.sql import SparkSession

    base = tmp_path_factory.mktemp("spark")
    builder = (SparkSession.builder.master("local[2]").appName("fhir-tests")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.warehouse.dir", str(base / "warehouse"))
               .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={base}")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false"))
    session = delta.configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    shutil.rmtree(base, ignore_errors=True)


@pytest.fixture()
def cfg(tmp_path):
    return load_config(overrides={"catalog": {"catalog": ""}, "storage": {"root_path": str(tmp_path / "lake")}})


@pytest.fixture()
def clean(spark, cfg):
    yield
    for schema in cfg["catalog"]["schemas"].values():
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
