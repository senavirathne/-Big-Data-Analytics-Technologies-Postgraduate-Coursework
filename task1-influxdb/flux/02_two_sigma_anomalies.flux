observations =
    from(bucket: "climate_raw")
        |> range(start: 1980-01-01T08:00:00Z, stop: 2020-01-01T00:00:00Z)
        |> filter(fn: (row) => row._measurement == "airport_wind" and row.station == "PAFA")
        |> group(columns: ["_field"])

means =
    observations
        |> mean(column: "_value")
        |> rename(columns: {_value: "dataset_mean"})

deviations =
    observations
        |> stddev(column: "_value", mode: "population")
        |> rename(columns: {_value: "dataset_stddev"})

thresholds =
    join(tables: {mean: means, deviation: deviations}, on: ["_field"])

values = observations |> rename(columns: {_value: "observation"})

join(tables: {value: values, threshold: thresholds}, on: ["_field"])
    |> filter(
        fn: (row) =>
            row.observation > row.dataset_mean + (2.0 * row.dataset_stddev) or
                row.observation < row.dataset_mean - (2.0 * row.dataset_stddev),
    )
    |> rename(columns: {observation: "_value"})
    |> yield(name: "observations_beyond_two_standard_deviations")
