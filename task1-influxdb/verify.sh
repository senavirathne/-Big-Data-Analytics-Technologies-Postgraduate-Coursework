#!/bin/sh
set -eu

influx query \
  --host "$INFLUX_HOST" \
  --org "$INFLUX_ORG" \
  --token "$INFLUX_TOKEN" \
  --file /queries/01_sliding_hourly_average.flux \
  >/dev/null

influx query \
  --host "$INFLUX_HOST" \
  --org "$INFLUX_ORG" \
  --token "$INFLUX_TOKEN" \
  --file /queries/02_two_sigma_anomalies.flux \
  >/dev/null

# Execute the same operation once immediately. The registered task continues to execute it
# hourly after this verification container exits.
influx query \
  --host "$INFLUX_HOST" \
  --org "$INFLUX_ORG" \
  --token "$INFLUX_TOKEN" \
  --file /queries/03_continuous_downsample.flux \
  >/dev/null

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
if [ -z "$task_id" ]; then
  echo "the continuous downsampling task is not registered" >&2
  exit 1
fi

task_json="$(
  influx task list \
    --host "$INFLUX_HOST" \
    --org "$INFLUX_ORG" \
    --token "$INFLUX_TOKEN" \
    --id "$task_id" \
    --json
)"
compact_task_json="$(printf '%s' "$task_json" | tr -d '[:space:]')"
for required_fragment in \
  '"status":"active"' \
  'airport_wind' \
  'climate_raw' \
  'climate_30d'; do
  if ! printf '%s\n' "$compact_task_json" | grep -Fq "$required_fragment"; then
    echo "the continuous downsampling task is stale or incomplete" >&2
    exit 1
  fi
done

bucket_json="$(
  influx bucket list \
    --host "$INFLUX_HOST" \
    --org "$INFLUX_ORG" \
    --token "$INFLUX_TOKEN" \
    --name climate_30d \
    --json
)"
if ! printf '%s\n' "$bucket_json" | grep -q \
  '"everySeconds"[[:space:]]*:[[:space:]]*2592000'; then
  echo "climate_30d does not have the required 30-day retention rule" >&2
  exit 1
fi

echo "PASS: all three Flux operations executed; task and 30-day retention verified"
