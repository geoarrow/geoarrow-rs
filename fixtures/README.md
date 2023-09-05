
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

ogr2ogr \
    -select geometry \
    -limit 100000 \
    -lco ROW_GROUP_SIZE=100000 \
    geoparquet/nz-building-outlines-geometry.parquet \
    geoparquet/nz-building-outlines.parquet
```
