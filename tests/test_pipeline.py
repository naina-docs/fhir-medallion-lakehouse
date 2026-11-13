from datetime import date, datetime

import pytest

from conftest import FIXTURES, FakeClient, bundle, resource

pytest.importorskip("pyspark")

from fhir_lakehouse.bronze import load_bronze  # noqa: E402
from fhir_lakehouse.config import resource_table  # noqa: E402
from fhir_lakehouse.control import control_table, create_control_objects, get_watermark, log_api_calls  # noqa: E402
from fhir_lakehouse.gold import create_gold_views  # noqa: E402
from fhir_lakehouse.quality import run_checks  # noqa: E402
from fhir_lakehouse.raw_ingest import ingest_resource  # noqa: E402
from fhir_lakehouse.silver import load_silver  # noqa: E402
from fhir_lakehouse.storage import raw_dir, write_raw  # noqa: E402

RUN_DATE = date(2026, 11, 12)
RESOURCES = ["Patient", "Encounter", "Observation", "Condition"]


def first_load():
    return {
        "Patient": [bundle([
            resource("Patient", "p1", 1, "2026-11-10T08:00:00+00:00", gender=" Female ", birthDate="1980-05-02",
                     name=[{"family": "Rao", "given": ["Asha"]}]),
            resource("Patient", "p2", 3, "2026-11-11T09:30:00+00:00", gender="male", birthDate="1975",
                     name=[{"family": ""}]),
        ])],
        "Encounter": [bundle([
            resource("Encounter", "e1", 1, "2026-11-10T10:00:00+00:00", status="finished",
                     subject={"reference": "Patient/p1"},
                     period={"start": "2026-11-10T09:00:00+00:00", "end": "2026-11-10T09:45:00+00:00"}),
        ])],
        "Observation": [bundle([
            resource("Observation", "o1", 1, "2026-11-10T11:00:00+00:00", status="final",
                     subject={"reference": "Patient/p1"}, encounter={"reference": "Encounter/e1"},
                     code={"coding": [{"code": "8867-4", "display": "Heart rate"}]},
                     effectiveDateTime="2026-11-10T09:10:00+00:00",
                     valueQuantity={"value": 72, "unit": "/min"},
                     referenceRange=[{"low": {"value": 60}, "high": {"value": 100}}]),
            resource("Observation", "o2", 1, "2026-11-10T11:05:00+00:00", status="final",
                     subject={"reference": "Patient/missing"}, valueQuantity={"value": "n/a"}),
        ])],
        "Condition": [bundle([
            resource("Condition", "c1", 1, "2026-11-09T12:00:00+00:00",
                     clinicalStatus={"coding": [{"code": "active"}]}, subject={"reference": "Patient/p2"},
                     code={"coding": [{"code": "I10", "display": "Hypertension"}]},
                     onsetDateTime="2020-01-01"),
        ])],
    }


def run_layers(spark, cfg, pages, run_id, run_date=RUN_DATE):
    client = FakeClient(pages)
    for r in RESOURCES:
        ingest_resource(spark, cfg, r, run_id, run_date, client)
        load_bronze(spark, cfg, r)
        load_silver(spark, cfg, r)


def test_end_to_end(spark, cfg, clean):
    create_control_objects(spark, cfg)
    run_layers(spark, cfg, first_load(), "run1")

    bronze = spark.table(resource_table(cfg, "bronze", "Patient"))
    assert bronze.count() == 2
    assert bronze.where("extraction_timestamp IS NULL OR api_url_or_params IS NULL OR raw_saved_at IS NULL").count() == 0

    p = {r.resource_id: r for r in spark.table(resource_table(cfg, "silver", "Patient")).collect()}
    assert p["p1"].gender == "female" and str(p["p1"].birth_date) == "1980-05-02"
    assert p["p2"].family_name is None and str(p["p2"].birth_date) == "1975-01-01"

    obs = {r.resource_id: r for r in spark.table(resource_table(cfg, "silver", "Observation")).collect()}
    assert obs["o1"].patient_id == "p1" and obs["o1"].encounter_id == "e1" and obs["o1"].value_number == 72.0
    assert obs["o2"].value_number is None

    assert str(get_watermark(spark, cfg, "Patient")) == "2026-11-12 00:00:00+00:00"
    assert ingest_resource(spark, cfg, "Patient", "rerun", RUN_DATE, FakeClient(first_load()))["windows"] == 0

    views = {v.split(".")[-1]: v for v in create_gold_views(spark, cfg)}
    for name in ["dim_patient_history", "fact_encounter", "fact_observation", "fact_condition"]:
        assert spark.table(views[name]).where("extraction_timestamp IS NULL OR api_url_or_params IS NULL").count() == 0
    for r in RESOURCES:
        assert {"extraction_timestamp", "api_url_or_params", "raw_saved_at"} <= set(
            spark.table(resource_table(cfg, "silver", r)).columns)
    fact_obs = {r.observation_id: r for r in spark.table(views["fact_observation"]).collect()}
    assert fact_obs["o1"].range_flag == "normal" and fact_obs["o1"].patient_key != cfg["gold"]["unknown_key"]
    assert fact_obs["o2"].patient_key == cfg["gold"]["unknown_key"]
    enc = spark.table(views["fact_encounter"]).first()
    assert enc.duration_minutes == 45 and enc.date_key == 20261110
    assert spark.table(views["dim_patient"]).count() == 3

    results = run_checks(spark, cfg, "run1")
    assert all(r["passed"] for r in results if r["severity"] == "error")
    assert spark.table(control_table(cfg, "dq_results")).count() == len(results)


def test_scd2_tracks_changes(spark, cfg, clean):
    create_control_objects(spark, cfg)
    run_layers(spark, cfg, first_load(), "run1")
    silver = resource_table(cfg, "silver", "Patient")

    changed = {"Patient": [bundle([
        resource("Patient", "p1", 2, "2026-11-12T08:00:00+00:00", gender="other", birthDate="1980-05-02",
                 name=[{"family": "Rao", "given": ["Asha"]}]),
        resource("Patient", "p1", 3, "2026-11-12T09:00:00+00:00", gender="other", birthDate="1980-05-02",
                 name=[{"family": "Rao", "given": ["Asha"]}]),
        resource("Patient", "p2", 4, "2026-11-12T10:00:00+00:00", gender="male", birthDate="1975",
                 name=[{"family": ""}]),
    ])]}
    client = FakeClient(changed)
    ingest_resource(spark, cfg, "Patient", "run2", date(2026, 11, 13), client)
    load_bronze(spark, cfg, "Patient")
    assert load_silver(spark, cfg, "Patient") == {"inserted": 1, "closed": 1}

    history = spark.table(silver).where("resource_id = 'p1'").orderBy("effective_from").collect()
    assert [(h.version_id, h.gender, h.is_current) for h in history] == [(1, "female", False), (2, "other", True)]
    assert history[0].effective_to == history[1].effective_from
    assert spark.table(silver).where("resource_id = 'p2'").count() == 1
    assert load_silver(spark, cfg, "Patient") == {"inserted": 0, "closed": 0}


def test_bronze_reads_xml(spark, cfg, clean):
    create_control_objects(spark, cfg)
    path = write_raw(raw_dir(cfg, "Patient", RUN_DATE) / "x_2026-11-10_page_0001.xml",
                     (FIXTURES / "patient_bundle.xml").read_text())
    now = datetime(2026, 11, 12)
    log_api_calls(spark, cfg, [{"run_id": "x", "resource": "Patient", "window_start": datetime(2026, 11, 10),
                                "window_end": datetime(2026, 11, 11), "page_number": 1, "request_url": "https://test",
                                "request_params": "{}", "status_code": 200, "entry_count": 2, "api_called_at": now,
                                "duration_ms": 1, "raw_file_path": str(path), "saved_at": now}])
    assert load_bronze(spark, cfg, "Patient") == 2
    rows = spark.table(resource_table(cfg, "bronze", "Patient")).orderBy("resource_id").collect()
    assert [(r.resource_id, r.version_id, r.source_format) for r in rows] == [("p1", "1", "xml"), ("p2", "3", "xml")]
    assert rows[0].resource_xml.startswith("<Patient") and rows[0].resource_json is None
