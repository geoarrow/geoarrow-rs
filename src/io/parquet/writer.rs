use std::collections::HashMap;
use std::io::Write;
use std::str::FromStr;

use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;

use crate::algorithm::native::TotalBounds;
use crate::chunked_array::ChunkedArray;
use crate::error::Result;
use crate::io::parquet::metadata::{
    get_geometry_types, GeoParquetColumnMetadata, GeoParquetMetadata,
};
use crate::io::wkb::ToWKB;
use crate::table::GeoTable;
use crate::GeometryArrayTrait;

pub fn write_geoparquet<W: Write + Send>(
    table: &mut GeoTable,
    writer: W,
    writer_properties: Option<WriterProperties>,
) -> Result<()> {
    // Create geo metadata before casting to WKB so that we can compute geometry types and bbox
    // more efficiently.
    let geo_meta = create_metadata(table)?;

    // Cast geometry column to WKB and update geometry column in table.
    let wkb_geometry = table.geometry()?.as_ref().to_wkb::<i32>();
    table.remove_column(table.geometry_column_index());
    let field = wkb_geometry.extension_field();
    table.append_column(
        field,
        ChunkedArray::new(
            wkb_geometry
                .chunks
                .into_iter()
                .map(|chunk| chunk.into_array_ref())
                .collect(),
        ),
    )?;

    let schema = table.schema();
    let mut writer = ArrowWriter::try_new(writer, schema.clone(), writer_properties)?;

    writer.append_key_value_metadata(geo_meta);

    for batch in table.batches() {
        writer.write(batch)?;
    }

    writer.close()?;

    Ok(())
}

fn create_metadata(table: &GeoTable) -> Result<KeyValue> {
    let bbox = table.geometry()?.as_ref().total_bounds();
    let geometry_types = get_geometry_types(table.geometry()?.as_ref());
    let array_metadata = table
        .geometry()?
        .geometry_chunks()
        .first()
        .unwrap()
        .metadata();
    let crs = array_metadata
        .as_ref()
        .crs
        .as_ref()
        .map(|crs_str| serde_json::Value::from_str(crs_str.as_str()))
        .transpose()?;

    let geometry_column_name = table.schema().field(table.geometry_column_index()).name();
    let column_meta = GeoParquetColumnMetadata {
        encoding: "WKB".to_string(),
        geometry_types,
        crs,
        orientation: None,
        edges: None,
        bbox: Some(vec![bbox.minx, bbox.miny, bbox.maxx, bbox.maxy]),
        epoch: None,
    };
    let mut columns = HashMap::with_capacity(1);
    columns.insert(geometry_column_name.clone(), column_meta);

    let meta = GeoParquetMetadata {
        version: "1.0.0".to_string(),
        primary_column: geometry_column_name.clone(),
        columns,
    };

    Ok(KeyValue::new(
        "geo".to_string(),
        serde_json::to_string(&meta)?,
    ))
}
