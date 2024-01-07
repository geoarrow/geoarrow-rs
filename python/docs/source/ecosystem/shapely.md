# Shapely

For interoperability with Shapely, use WKB methods as shown below.

```py
import geoarrow.rust.core
import shapely

shapely_geoms = shapely.from_wkb(geoarrow.rust.core.to_wkb(geoarrow_array))
geoarrow_array = geoarrow.rust.core.from_wkb(
    shapely.to_wkb(shapely_geoms, flavor="iso")
)
```

!!! note

    `geoarrow.rust.core.from_wkb` and `geoarrow.rust.core.to_wkb` do not yet work on chunked arrays. To use with a chunked array,

    ```py
    import geoarrow.rust.core
    import shapely
    import pyarrow as pa

    table = geoarrow.rust.core.read_geojson("...")
    chunked_geoarrow_array = table.geometry
    shapely_geoms = shapely.from_wkb(
        pa.chunked_array(
            [
                geoarrow.rust.core.to_wkb(chunk)
                for chunk in chunked_geoarrow_array.chunks()
            ]
        )
    )
    ```

    It will be fixed to work with chunked arrays in the future.
