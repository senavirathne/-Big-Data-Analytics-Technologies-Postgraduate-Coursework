observations =
    from(bucket: "climate_raw")
        |> range(start: 2005-12-31T23:00:00Z, stop: 2016-12-31T23:00:00Z)
        |> filter(
            fn: (r) =>
                r._measurement == "weather" and r.location == "Szeged" and
                    r._field == "temperature_c",
        )
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
        fn: (r) =>
            r.observation > r.dataset_mean + (2.0 * r.dataset_stddev) or
                r.observation < r.dataset_mean - (2.0 * r.dataset_stddev),
    )
    |> rename(columns: {observation: "_value"})
    |> yield(name: "observations_beyond_two_standard_deviations")
