#!/bin/sh
set -eu

bucket_table="$(
  influx bucket list \
    --host "$INFLUX_HOST" \
    --org "$INFLUX_ORG" \
    --token "$INFLUX_TOKEN" \
    --name "$AUX_BUCKET" \
    --hide-headers
)"
bucket_id="$(printf '%s\n' "$bucket_table" | awk 'NR == 1 {print $1}')"

if [ -n "$bucket_id" ]; then
  influx bucket update \
    --host "$INFLUX_HOST" \
    --token "$INFLUX_TOKEN" \
    --id "$bucket_id" \
    --retention 720h
else
  influx bucket create \
    --host "$INFLUX_HOST" \
    --org "$INFLUX_ORG" \
    --token "$INFLUX_TOKEN" \
    --name "$AUX_BUCKET" \
    --retention 720h
fi

task_name=climate-hourly-downsample-30d
task_table="$(
  influx task list \
    --host "$INFLUX_HOST" \
    --org "$INFLUX_ORG" \
    --token "$INFLUX_TOKEN" \
    --hide-headers
)"
task_id="$(
  printf '%s\n' "$task_table" \
    | awk -v task_name="$task_name" '$2 == task_name {print $1; exit}'
)"

if [ -n "$task_id" ]; then
  influx task update \
    --host "$INFLUX_HOST" \
    --token "$INFLUX_TOKEN" \
    --id "$task_id" \
    --file /queries/03_continuous_downsample.flux \
    --status active
else
  influx task create \
    --host "$INFLUX_HOST" \
    --org "$INFLUX_ORG" \
    --token "$INFLUX_TOKEN" \
    --file /queries/03_continuous_downsample.flux
fi
