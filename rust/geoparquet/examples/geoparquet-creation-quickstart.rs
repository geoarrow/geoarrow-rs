//! This example demonstrates how to create a GeoParquet file using
//! arrow and geoarrow. It demonstrates how this crate interacts with
//! others in the ecosystem to create the file.
//!
//! ```sh
//! cargo run --example geoparquet-creation-quickstart
//! ```

use std::sync::Arc;

use arrow_array::{self, ArrayRef, Int32Array, RecordBatch};
use geo_types::Geometry;
use geoarrow_array::{GeoArrowArray, builder::GeometryBuilder};
use geoarrow_schema::GeometryType;
use wkt::TryFromWkt;

use arrow_schema::{DataType::Int32, Field, SchemaBuilder};
use geoarrow_schema::GeoArrowType;
use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptionsBuilder};
use parquet::arrow::ArrowWriter;

fn main() {
    const GEOMETRY_COLUMN_NAME: &str = "geometry";

    // For the sake of explicitness, we can specify the primary column
    let options = GeoParquetWriterOptionsBuilder::default()
        .set_primary_column(GEOMETRY_COLUMN_NAME.to_string())
        .build();

    // The schema builder defines the schema that
    // our arrow arrays must follow. If an array
    // deviates from this schema, an error will be returned
    let mut schema_builder = SchemaBuilder::new();

    // GeoArrow types are extension types to Arrow. As such,
    // we must create the proper GeoArrowType struct and
    // then convert them to an Arrow field so that they
    // can be used with the standard arrow schema builder.
    let geometry_type = GeoArrowType::Geometry(GeometryType::default());
    // The name of the field should match the column name
    // we set in the options
    let geometry_field = geometry_type.to_field(GEOMETRY_COLUMN_NAME, false);
    schema_builder.push(geometry_field);

    // We can still use the standard arrow `DataType` structs
    // as normal
    let feature_id = Field::new("id", Int32, false);
    schema_builder.push(feature_id);

    let schema = schema_builder.finish();

    let mut gpq_encoder = GeoParquetRecordBatchEncoder::try_new(&schema, &options).unwrap();

    let output_file = std::fs::File::create("geoparquet_creation_quickstart.parquet").unwrap();
    let mut parquet_writer =
        ArrowWriter::try_new(output_file, gpq_encoder.target_schema(), None).unwrap();

    // You must first construct the arrow array and then
    // cast it to the `ArrayRef` type so that it can be
    // used in a `RecordBatch`.
    let id_column = Arc::new(Int32Array::from_iter_values([1, 2, 3])) as ArrayRef;

    // `GeometryType` is the generic representation of a geometry.
    // There are also other more specific geometry types for more
    // specificity
    let generic_geometry_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(generic_geometry_type);
    let example_geometry1 = Geometry::try_from_wkt_str("POINT (1 2)").unwrap();
    let example_geometry2 = Geometry::Rect(geo_types::Rect::new((1.0, 3.0), (4.0, 5.0)));
    let example_geometry3 = Geometry::try_from_wkt_str("LINESTRING (30 10, 10 30, 40 40)").unwrap();
    builder.push_geometry(Some(&example_geometry1)).unwrap();
    builder.push_geometry(Some(&example_geometry2)).unwrap();
    builder.push_geometry(Some(&example_geometry3)).unwrap();
    // We can use the helper function `to_array_ref`
    // so we don't need to cast to `ArrayRef`
    let geometry_column = builder.finish().to_array_ref();

    // The columns in a RecordBatch must follow the schema.
    //The Nth item in the column vector must match the Nth item in the schema.
    // It is important that the schema is passed in properly to the `RecordBatch`.
    // For some functions like `RecordBatch::try_from_iter` the schema is inferred
    // and as such may drop informaton about our geometry types
    let batch = RecordBatch::try_new(Arc::new(schema), vec![geometry_column, id_column]).unwrap();

    for batch in [batch] {
        let encoded_batch = gpq_encoder.encode_record_batch(&batch).unwrap();
        parquet_writer.write(&encoded_batch).unwrap();
    }

    let kv_metadata = gpq_encoder.into_keyvalue().unwrap();
    parquet_writer.append_key_value_metadata(kv_metadata);
    parquet_writer.finish().unwrap();
}
