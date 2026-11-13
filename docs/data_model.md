# Data Model

## Gold star schema

```mermaid
erDiagram
    dim_patient ||--o{ fact_encounter : patient_key
    dim_patient ||--o{ fact_observation : patient_key
    dim_patient ||--o{ fact_condition : patient_key
    dim_date ||--o{ fact_encounter : date_key
    dim_date ||--o{ fact_observation : date_key
    dim_date ||--o{ fact_condition : date_key
    fact_encounter ||--o{ fact_observation : encounter_id
    fact_encounter ||--o{ fact_condition : encounter_id

    dim_patient {
        string patient_key PK
        string patient_id
        string gender
        date birth_date
        int age_years
        boolean is_deceased
        string city
        string country
    }
    dim_date {
        int date_key PK
        date date
        int year
        int month
        boolean is_weekend
    }
    fact_encounter {
        string encounter_key PK
        string encounter_id
        string patient_key FK
        int date_key FK
        string status
        string class_name
        double duration_minutes
    }
    fact_observation {
        string observation_key PK
        string observation_id
        string patient_key FK
        string encounter_id
        int date_key FK
        string code_name
        double value_number
        string range_flag
    }
    fact_condition {
        string condition_key PK
        string condition_id
        string patient_key FK
        string encounter_id
        int date_key FK
        string code_name
        boolean is_active
    }
```

## Views

| View | Grain | Notes |
|---|---|---|
| `dim_patient` | current patient | includes an unknown member (`patient_key = '-1'`); phone numbers are not exposed |
| `dim_patient_history` | patient version | all SCD2 versions with validity dates |
| `dim_date` | day | continuous range over the fact dates, limited to `gold.date_start` … today + `gold.date_future_days` |
| `fact_encounter` | encounter | duration in minutes, date of `period.start` |
| `fact_observation` | observation | numeric value, unit, `range_flag` (low / normal / high) from reference range |
| `fact_condition` | condition | `is_active` from clinical status, date of onset or recorded date |
| `agg_daily_activity` | day | encounters, patients seen, average encounter length |
| `agg_condition_prevalence` | condition code | patients, conditions, active conditions, average age |

Facts whose patient is not in the loaded data point to the unknown member so they are not lost in joins.

All fact views and the patient dimensions carry `extraction_timestamp` and `api_url_or_params` from the source row.

## Silver columns

| Table | Columns |
|---|---|
| `patient` | active, gender, birth_date, deceased, deceased_at, family_name, given_name, phone, city, state, postal_code, country, marital_status, language |
| `encounter` | status, class_code, class_display, type_code, type_display, patient_id, practitioner_id, service_provider_id, reason_code, reason_text, period_start, period_end |
| `observation` | status, category_code, code_system, code, code_display, code_text, patient_id, encounter_id, effective_at, issued_at, value_number, value_unit, value_text, value_string, interpretation_code, ref_low, ref_high |
| `condition` | clinical_status, verification_status, category_code, code_system, code, code_display, code_text, severity, patient_id, encounter_id, onset_at, abatement_at, recorded_at |

Every Silver table also has `resource_id`, `version_id`, `last_updated`, `extraction_timestamp`, `api_url_or_params`, `raw_saved_at`, `source_run_id`, `source_loaded_at`, `row_hash`, `sk`, `effective_from`, `effective_to`, `is_current`, `silver_updated_at`.

Cleaning rules: strings are trimmed and blanks become null, codes such as gender and status are lower-cased, references like `Patient/123` keep only the id, and invalid dates or numbers become null instead of failing the load.
