#!/usr/bin/env bash
set -euo pipefail

JOB_FILE="$(cd "$(dirname "$0")" && pwd)/fhir_medallion_job.json"
NAME=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["name"])' "$JOB_FILE")
JOB_ID=$(databricks jobs list --output json | python3 -c '
import json, sys
jobs = json.load(sys.stdin)
jobs = jobs.get("jobs", []) if isinstance(jobs, dict) else jobs
print(next((str(j["job_id"]) for j in jobs if j["settings"]["name"] == sys.argv[1]), ""))' "$NAME")

if [ -z "$JOB_ID" ]; then
  databricks jobs create --json @"$JOB_FILE"
else
  RESET_FILE=$(mktemp)
  trap 'rm -f "$RESET_FILE"' EXIT
  python3 -c 'import json,sys; print(json.dumps({"job_id": int(sys.argv[2]), "new_settings": json.load(open(sys.argv[1]))}))' \
    "$JOB_FILE" "$JOB_ID" > "$RESET_FILE"
  databricks jobs reset --json @"$RESET_FILE"
  echo "updated job $JOB_ID"
fi
