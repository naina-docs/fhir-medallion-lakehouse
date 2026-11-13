from datetime import date

from fhir_lakehouse.config import load_config, resource_table, schema_name
from fhir_lakehouse.storage import raw_dir, raw_file_name, write_raw


def test_names_follow_config():
    cfg = load_config()
    assert schema_name(cfg, "bronze") == "workspace.fhir_bronze"
    assert resource_table(cfg, "bronze", "Patient") == "workspace.fhir_bronze.bronze_patient"
    assert resource_table(cfg, "silver", "Observation") == "workspace.fhir_silver.observation"


def test_local_override_drops_catalog(cfg):
    assert resource_table(cfg, "silver", "Condition") == "fhir_silver.condition"


def test_silver_columns_cover_all_resources():
    cfg = load_config()
    assert {r["name"] for r in cfg["resources"]} <= set(cfg["silver_columns"])


def test_raw_layout(cfg, tmp_path):
    path = raw_dir(cfg, "Patient", date(2026, 11, 12)) / raw_file_name("run1", date(2026, 11, 9), 7, "json")
    assert path.as_posix().endswith("raw/Patient/load_date=2026-11-12/run1_2026-11-09_page_0007.json")
    write_raw(path, "{}")
    assert path.read_text() == "{}"
    assert [p.name for p in path.parent.iterdir()] == [path.name]
