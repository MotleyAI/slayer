#!/usr/bin/env bash
# Boot SLayer against bigquery-public-data.thelook_ecommerce.
#
# Requires:
#   GCP_PROJECT_ID                  Billing project where BQ jobs run.
#   GOOGLE_APPLICATION_CREDENTIALS  Path to service-account JSON key.
#
# The JSON key file should grant the SA the roles/bigquery.jobUser role on
# $GCP_PROJECT_ID. The public dataset itself needs no extra grant — it is
# world-readable.
set -euo pipefail

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID is required (your billing project)}"
: "${GOOGLE_APPLICATION_CREDENTIALS:?GOOGLE_APPLICATION_CREDENTIALS must point at a service-account JSON key}"

if [ ! -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
  echo "GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS does not exist" >&2
  exit 1
fi

HERE="$(cd "$(dirname "$0")" && pwd)"
exec slayer serve --host 0.0.0.0 --port 5143 --storage "$HERE/slayer_data"
