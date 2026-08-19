option task = {name: "climate-hourly-downsample-30d", every: 1h}

from(bucket: "climate_raw")
    |> range(start: -task.every)
    |> filter(
        fn: (r) =>
            r._measurement == "weather" and r.location == "Szeged" and
                r._field == "temperature_c",
    )
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
    |> to(bucket: "climate_30d", org: "big-data-coursework")
