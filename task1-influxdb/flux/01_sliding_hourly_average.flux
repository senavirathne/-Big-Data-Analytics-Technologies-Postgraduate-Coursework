from(bucket: "climate_raw")
    |> range(start: 2005-12-31T23:00:00Z, stop: 2016-12-31T23:00:00Z)
    |> filter(
        fn: (r) =>
            r._measurement == "weather" and r.location == "Szeged" and
                r._field == "temperature_c",
    )
    |> aggregateWindow(every: 15m, period: 1h, fn: mean, createEmpty: false)
    |> yield(name: "sliding_hourly_average")
