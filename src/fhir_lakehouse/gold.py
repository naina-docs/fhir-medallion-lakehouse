from __future__ import annotations

from pyspark.sql import SparkSession

from .config import resource_table, schema_name, table_name


def gold_views(cfg: dict) -> dict[str, str]:
    g = {k: table_name(cfg, "gold", v) for k, v in cfg["tables"]["gold"].items()}
    s = {r: resource_table(cfg, "silver", r) for r in ("Patient", "Encounter", "Observation", "Condition")}
    unknown = cfg["gold"]["unknown_key"]
    date_start, future_days = cfg["gold"]["date_start"], int(cfg["gold"]["date_future_days"])
    patient_cols = """resource_id AS patient_id, gender, birth_date,
        floor(months_between(current_date(), birth_date) / 12) AS age_years,
        coalesce(deceased, deceased_at IS NOT NULL) AS is_deceased, deceased_at,
        family_name, given_name, city, state, postal_code, country, marital_status, language, active"""

    patient_key = f"coalesce(p.patient_key, '{unknown}') AS patient_key"

    return {
        "dim_patient_history": f"""
            SELECT sk AS patient_key, {patient_cols},
                   version_id, effective_from, effective_to, is_current,
                   extraction_timestamp, api_url_or_params
            FROM {s['Patient']}""",
        "dim_patient": f"""
            SELECT patient_key, patient_id, gender, birth_date, age_years, is_deceased, deceased_at,
                   family_name, given_name, city, state, postal_code, country, marital_status, language, active,
                   extraction_timestamp, api_url_or_params
            FROM {g['dim_patient_history']} WHERE is_current
            UNION ALL
            SELECT '{unknown}', '{unknown}', NULL, NULL, NULL, NULL, NULL,
                   NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL""",
        "fact_encounter": f"""
            SELECT e.sk AS encounter_key, e.resource_id AS encounter_id, e.patient_id, {patient_key},
                   CAST(date_format(e.period_start, 'yyyyMMdd') AS INT) AS date_key,
                   e.status, e.class_code, coalesce(e.class_display, e.class_code) AS class_name,
                   e.type_code, e.type_display, e.reason_code, e.reason_text,
                   e.practitioner_id, e.service_provider_id, e.period_start, e.period_end,
                   CASE WHEN e.period_end >= e.period_start
                        THEN (unix_timestamp(e.period_end) - unix_timestamp(e.period_start)) / 60 END AS duration_minutes,
                   e.last_updated, e.extraction_timestamp, e.api_url_or_params
            FROM {s['Encounter']} e
            LEFT JOIN {g['dim_patient']} p ON p.patient_id = e.patient_id
            WHERE e.is_current""",
        "fact_observation": f"""
            SELECT o.sk AS observation_key, o.resource_id AS observation_id, o.patient_id, {patient_key},
                   o.encounter_id, CAST(date_format(coalesce(o.effective_at, o.issued_at), 'yyyyMMdd') AS INT) AS date_key,
                   o.status, o.category_code, o.code_system, o.code,
                   coalesce(o.code_display, o.code_text, o.code) AS code_name,
                   o.value_number, o.value_unit, coalesce(o.value_text, o.value_string) AS value_text,
                   o.interpretation_code, o.ref_low, o.ref_high,
                   CASE WHEN o.value_number < o.ref_low THEN 'low'
                        WHEN o.value_number > o.ref_high THEN 'high'
                        WHEN o.ref_low IS NOT NULL OR o.ref_high IS NOT NULL THEN 'normal' END AS range_flag,
                   o.effective_at, o.issued_at, o.last_updated, o.extraction_timestamp, o.api_url_or_params
            FROM {s['Observation']} o
            LEFT JOIN {g['dim_patient']} p ON p.patient_id = o.patient_id
            WHERE o.is_current""",
        "fact_condition": f"""
            SELECT c.sk AS condition_key, c.resource_id AS condition_id, c.patient_id, {patient_key},
                   c.encounter_id, CAST(date_format(coalesce(c.onset_at, c.recorded_at), 'yyyyMMdd') AS INT) AS date_key,
                   c.clinical_status, c.verification_status, c.category_code, c.code_system, c.code,
                   coalesce(c.code_display, c.code_text, c.code) AS code_name, c.severity,
                   c.clinical_status IN ('active', 'recurrence', 'relapse') AS is_active,
                   c.onset_at, c.abatement_at, c.recorded_at, c.last_updated, c.extraction_timestamp, c.api_url_or_params
            FROM {s['Condition']} c
            LEFT JOIN {g['dim_patient']} p ON p.patient_id = c.patient_id
            WHERE c.is_current""",
        "dim_date": f"""
            WITH bounds AS (
                SELECT min(d) AS lo, max(d) AS hi FROM (
                    SELECT to_date(CAST(date_key AS STRING), 'yyyyMMdd') AS d FROM {g['fact_encounter']}
                    UNION ALL SELECT to_date(CAST(date_key AS STRING), 'yyyyMMdd') FROM {g['fact_observation']}
                    UNION ALL SELECT to_date(CAST(date_key AS STRING), 'yyyyMMdd') FROM {g['fact_condition']})
                WHERE d BETWEEN DATE'{date_start}' AND date_add(current_date(), {future_days}))
            SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key, d AS date, year(d) AS year,
                   quarter(d) AS quarter, month(d) AS month, date_format(d, 'MMMM') AS month_name,
                   day(d) AS day, dayofweek(d) AS day_of_week, date_format(d, 'EEEE') AS day_name,
                   dayofweek(d) IN (1, 7) AS is_weekend
            FROM bounds LATERAL VIEW explode(sequence(lo, hi, interval 1 day)) t AS d
            WHERE lo IS NOT NULL""",
        "agg_daily_activity": f"""
            SELECT d.date, d.date_key,
                   count(DISTINCT e.encounter_id) AS encounters,
                   count(DISTINCT e.patient_id) AS patients_seen,
                   avg(e.duration_minutes) AS avg_encounter_minutes
            FROM {g['dim_date']} d
            LEFT JOIN {g['fact_encounter']} e ON e.date_key = d.date_key
            GROUP BY d.date, d.date_key""",
        "agg_condition_prevalence": f"""
            SELECT c.code, c.code_name,
                   count(DISTINCT c.patient_id) AS patients,
                   count(*) AS conditions,
                   sum(CASE WHEN c.is_active THEN 1 ELSE 0 END) AS active_conditions,
                   round(avg(p.age_years), 1) AS avg_patient_age
            FROM {g['fact_condition']} c
            LEFT JOIN {g['dim_patient']} p ON p.patient_key = c.patient_key
            GROUP BY c.code, c.code_name""",
    }


def create_gold_views(spark: SparkSession, cfg: dict) -> list[str]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name(cfg, 'gold')}")
    created = []
    for key, sql in gold_views(cfg).items():
        name = table_name(cfg, "gold", cfg["tables"]["gold"][key])
        spark.sql(f"CREATE OR REPLACE VIEW {name} AS {sql}")
        created.append(name)
    return created
