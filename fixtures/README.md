- `roads.geojson` from https://github.com/georust/gdal/blob/61d79f9e6c7c3c9dc7ba0206112ad8b03146fe59/fixtures/roads.geojson

### `nybb.arrow` (MultiPolygon)

```
wget https://www.nyc.gov/assets/planning/download/zip/data-maps/open-data/nybb_16a.zip
extract nybb_16a.zip
ogr2ogr nybb.arrow nybb_16a -lco GEOMETRY_ENCODING=GEOARROW -nlt PROMOTE_TO_MULTI
```

### `nz-building-outlines` (WKB, MultiPolygon)

This file is used for benchmarks. It's 400MB so it's not checked in to git.

```bash
wget https://storage.googleapis.com/open-geodata/linz-examples/nz-building-outlines.parquet -P geoparquet/
```

### `overture_infrastructure.parquet`

```py
import pyarrow.parquet as pq
path = "s3://overturemaps-us-west-2/release/2024-07-22.0/theme=base/type=infrastructure/part-00002-45813b04-c38e-4fcd-add8-9a16b9df42ad-c000.zstd.parquet"
file = pq.ParquetFile(path)
table = file.read_row_group(0)
pq.write_table(table.slice(0, 100), "geoparquet/overture_infrastructure.parquet")
```

### `overture_buildings.parquet`

```py
import pyarrow.parquet as pq
path = "s3://overturemaps-us-west-2/release/2024-07-22.0/theme=buildings/type=building/part-00166-ad3ba139-0181-4cec-a708-4d76675a32b0-c000.zstd.parquet"
file = pq.ParquetFile(path)
table = file.read_row_group(0)
pq.write_table(table.slice(0, 100), "geoparquet/overture_buildings.parquet")
```

Relevant characteristics:

- Float bbox columns
- WKB geometry
- Polygon/MultiPolygon

```json
{
  "version": "1.0.0",
  "primary_column": "geometry",
  "columns": {
    "geometry": {
      "encoding": "WKB",
      "geometry_types": ["Polygon", "MultiPolygon"],
      "bbox": [-0.0009639, 47.8116072, 11.2504289, 52.03137527066542]
    }
  }
}
```

Note the bbox of this extract is:

```json
[7.393789291381836, 50.34489440917969, 7.398535251617432, 50.34762954711914]
```
