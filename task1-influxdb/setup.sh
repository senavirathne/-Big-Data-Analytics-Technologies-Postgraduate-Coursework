#!/bin/sh
set -eu

bucket_json="$(
  influx bucket list \
    --host "$INFLUX_HOST" \
    --org "$INFLUX_ORG" \
    --token "$INFLUX_TOKEN" \
    --json
)"

if ! printf '%s\n' "$bucket_json" | grep -q '"name"[[:space:]]*:[[:space:]]*"'"$AUX_BUCKET"'"'; then
  influx bucket create \
    --host "$INFLUX_HOST" \
    --org "$INFLUX_ORG" \
    --token "$INFLUX_TOKEN" \
    --name "$AUX_BUCKET" \
    --retention 720h
fi
task_json="$(
  influx task list \
    --host "$INFLUX_HOST" \
    --org "$INFLUX_ORG" \
    --token "$INFLUX_TOKEN" \
    --json
)"

if ! printf '%s\n' "$task_json" | grep -q '"name"[[:space:]]*:[[:space:]]*"climate-hourly-downsample-30d"'; then
  influx task create \
    --host "$INFLUX_HOST" \
    --org "$INFLUX_ORG" \
    --token "$INFLUX_TOKEN" \
    --file /queries/03_continuous_downsample.flux
fi
