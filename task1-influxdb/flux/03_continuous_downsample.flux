option task = {name: "climate-hourly-downsample-30d", every: 1h}

from(bucket: "climate_raw")
    |> range(start: -task.every)
    |> filter(fn: (row) => row._measurement == "airport_wind" and row.station == "PAFA")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
    |> to(bucket: "climate_30d", org: "big-data-coursework")
