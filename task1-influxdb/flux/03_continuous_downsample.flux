option task = {name: "climate-hourly-downsample-30d", every: 1h}

from(bucket: "climate_raw")
    |> range(start: 1900-01-01T00:00:00Z, stop: 2101-01-01T00:00:00Z)
    |> filter(fn: (row) => row._measurement == "fairbanks_climate")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
    |> to(bucket: "climate_30d", org: "big-data-coursework")
