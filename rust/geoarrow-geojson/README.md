# geoarrow-geojson

This crate provides two types of writers.

- [`GeoJsonWriter`][crate::writer::GeoJsonWriter] streams Arrow record batches as a GeoJSON FeatureCollection, writing the collection header once and appending each batch of features. Use it when you need a single GeoJSON document containing all features.

  Example output (formatted for readability):

  ```json
  {
    "type": "FeatureCollection",
    "features": [
      {
        "type": "Feature",
        "geometry": { "type": "Point", "coordinates": [30, 10] },
        "properties": { "count": 10 }
      },
      {
        "type": "Feature",
        "geometry": { "type": "Point", "coordinates": [40, 20] },
        "properties": { "count": 20 }
      }
    ]
  }
  ```

- [`GeoJsonLinesWriter`][crate::writer::GeoJsonWriter] writes each feature as a standalone GeoJSON object separated by newlines, making it suitable for line-delimited sinks and streaming pipelines.

  Example output:

  ```json
  {"type":"Feature","geometry":{"type":"Point","coordinates":[30,10]},"properties":{"count":10}}
  {"type":"Feature","geometry":{"type":"Point","coordinates":[40,20]},"properties":{"count":20}}
  ```
