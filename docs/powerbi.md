# Power BI Report

## Connect

1. Power BI Desktop → Get Data → **Azure Databricks**.
2. Server hostname and HTTP path: from a SQL warehouse (Connection details tab).
3. Catalog `workspace`, schema `fhir_gold`; select `dim_patient`, `dim_date`, `fact_encounter`, `fact_observation`, `fact_condition`, `agg_condition_prevalence`.
4. Use **DirectQuery** for always-current data, or Import with a scheduled refresh after the daily job.

## Model

| From | To | Cardinality |
|---|---|---|
| `fact_encounter[patient_key]` | `dim_patient[patient_key]` | many to one |
| `fact_observation[patient_key]` | `dim_patient[patient_key]` | many to one |
| `fact_condition[patient_key]` | `dim_patient[patient_key]` | many to one |
| `fact_encounter[date_key]` | `dim_date[date_key]` | many to one |
| `fact_observation[date_key]` | `dim_date[date_key]` | many to one |
| `fact_condition[date_key]` | `dim_date[date_key]` | many to one |

Mark `dim_date` as the date table (`date` column).

## Measures

```DAX
Patients = DISTINCTCOUNT ( dim_patient[patient_id] ) - 1
Encounters = COUNTROWS ( fact_encounter )
Avg Encounter Minutes = AVERAGE ( fact_encounter[duration_minutes] )
Observations = COUNTROWS ( fact_observation )
Abnormal Observations = CALCULATE ( [Observations], fact_observation[range_flag] IN { "low", "high" } )
Abnormal Rate = DIVIDE ( [Abnormal Observations], CALCULATE ( [Observations], NOT ISBLANK ( fact_observation[range_flag] ) ) )
Active Conditions = CALCULATE ( COUNTROWS ( fact_condition ), fact_condition[is_active] )
Unknown Patient Share = DIVIDE ( CALCULATE ( [Encounters], fact_encounter[patient_key] = "-1" ), [Encounters] )
```

`Patients` subtracts the unknown member row.

## Pages

1. **Overview**: cards for Patients, Encounters, Observations, Active Conditions; encounters by day (line, `dim_date[date]`); encounters by class (bar).
2. **Patients**: gender split (donut), age bands (column on `age_years`), patients by country/city (map or bar).
3. **Clinical**: top 10 conditions by patients (`agg_condition_prevalence`), abnormal rate by observation `code_name`, observation values over time for a selected code.
4. **Data quality**: table on `fhir_control.dq_results` and `pipeline_run_log` for the latest run, Unknown Patient Share card.

Slicers: date range, gender, encounter status, condition code.
