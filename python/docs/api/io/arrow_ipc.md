# Arrow IPC

It's possible to read and write GeoArrow data to the [Arrow IPC format](https://arrow.apache.org/docs/python/ipc.html).

The Arrow IPC format is able to fully represent GeoArrow data. Loading such files back into memory will identically reproduce the prior data.

Arrow IPC generically supports GeoArrow data without any extra behavior, so the functionality to read and write Arrow IPC files lives in [`arro3`](https://github.com/kylebarron/arro3).

Refer to:

- [`arro3.io.read_ipc`][]
- [`arro3.io.read_ipc_stream`][]
- [`arro3.io.write_ipc`][]
- [`arro3.io.write_ipc_stream`][]

When saved without any internal compression, the Arrow IPC format can also be memory-mapped, enabling faster reading.
