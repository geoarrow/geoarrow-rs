use std::io::Write;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringViewArray, StructArray};
use arrow_json::{ArrayWriter, LineDelimitedWriter, WriterBuilder};
use arrow_schema::{ArrowError, DataType, Field, Fields, Schema};
use geoarrow_schema::GeoArrowType;

use crate::encoder::GeoArrowEncoderFactory;

pub struct GeoJsonWriter<W: Write> {
    /// Underlying writer to use to write bytes
    writer: ArrayWriter<W>,
}

impl<W: Write> GeoJsonWriter<W> {
    /// Construct a new writer
    pub fn new(mut writer: W) -> std::io::Result<Self> {
        Self::write_header(&mut writer).unwrap();

        let array_writer = WriterBuilder::new()
            .with_encoder_factory(Arc::new(GeoArrowEncoderFactory))
            .build(writer);
        Ok(Self {
            writer: array_writer,
        })
    }

    fn write_header(w: &mut W) -> std::io::Result<()> {
        // Don't include the initial `[` because the ArrayWriter will write the open brace
        let s = br#"{"type":"FeatureCollection","features":"#;
        w.write_all(s)?;
        Ok(())
    }

    /// Serialize batch to GeoJSON output
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        let batch = transform_batch(batch)?;
        self.writer.write(&batch)
    }

    /// Serialize batches to GeoJSON output
    pub fn write_batches(&mut self, batches: &[&RecordBatch]) -> Result<(), ArrowError> {
        for batch in batches {
            self.write(&transform_batch(batch)?)?;
        }
        Ok(())
    }

    /// Finishes the output stream. This function must be called after
    /// all record batches have been produced. (e.g. producing the final `']'` if writing
    /// arrays.
    ///
    /// Consumes self and returns the underlying writer.
    pub fn finish(mut self) -> Result<W, ArrowError> {
        self.writer.finish()?;
        let mut w = self.writer.into_inner();
        // Write the closing brace
        w.write_all(b"}")?;
        Ok(w)
    }
}

pub struct GeoJsonLinesWriter<W: Write> {
    /// Underlying writer to use to write bytes
    writer: LineDelimitedWriter<W>,
}

impl<W: Write> GeoJsonLinesWriter<W> {
    /// Construct a new writer
    pub fn new(writer: W) -> Self {
        let line_writer = WriterBuilder::new()
            .with_encoder_factory(Arc::new(GeoArrowEncoderFactory))
            .build(writer);
        Self {
            writer: line_writer,
        }
    }

    /// Serialize batch to GeoJSON output
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        let batch = transform_batch(batch)?;
        self.writer.write(&batch)
    }

    /// Serialize batches to GeoJSON output
    pub fn write_batches(&mut self, batches: &[&RecordBatch]) -> Result<(), ArrowError> {
        for batch in batches {
            self.write(&transform_batch(batch)?)?;
        }
        Ok(())
    }

    /// Finishes the output stream. This function must be called after
    /// all record batches have been produced.
    ///
    /// Consumes self and returns the underlying writer.
    pub fn finish(mut self) -> Result<W, ArrowError> {
        self.writer.finish()?;
        Ok(self.writer.into_inner())
    }
}

/// Transform the batch to a format that can be written as GeoJSON
///
/// Steps:
/// - Find geometry column(s); error if there's more than one geometry column. Make sure the
///   geometry column is called `"geometry"`.
/// - For all non-geometry columns, wrap into a struct called "properties"
/// - Keep `id` column separate, if designated, as it's at the top-level in GeoJSON
/// - The custom encoders handle geometry types.
fn transform_batch(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    let schema = batch.schema_ref();

    let mut geometry_field_index = None;
    let mut id_field_index = None;
    let mut property_fields = Vec::new();
    let mut property_arrays = Vec::new();

    // Identify geometry, id, and property columns
    for (i, field) in schema.fields().iter().enumerate() {
        let array = batch.column(i);

        // Check if this is a geometry column by looking for GeoArrow extension metadata
        if GeoArrowType::from_extension_field(field).is_ok() {
            if geometry_field_index.is_some() {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Multiple geometry columns found in positions {} and {i}. GeoJSON requires exactly one geometry column.",
                    geometry_field_index.unwrap()
                )));
            }
            geometry_field_index = Some(i);
        } else if field.name() == "id" {
            id_field_index = Some(i);
        } else {
            // This is a property column
            property_fields.push(field.as_ref().clone());
            property_arrays.push(array.clone());
        }
    }

    let geometry_field_index = geometry_field_index.ok_or(ArrowError::InvalidArgumentError(
        "No geometry column found in the batch.".to_string(),
    ))?;

    // Build the new schema and arrays
    let mut new_fields = Vec::new();
    let mut new_arrays = Vec::new();

    // Add field for: "type: Feature"
    let type_field = Arc::new(Field::new("type", DataType::Utf8View, false));
    new_fields.push(type_field);
    let type_array = Arc::new(StringViewArray::from(vec!["Feature"; batch.num_rows()])) as ArrayRef;
    new_arrays.push(type_array);

    // Add id column if present
    if let Some(id_idx) = id_field_index {
        new_fields.push(schema.fields()[id_idx].clone());
        new_arrays.push(batch.column(id_idx).clone());
    }

    // Add geometry column (renamed to "geometry")
    let geometry_field = &schema.fields()[geometry_field_index];
    let renamed_geometry_field = Arc::new(
        Field::new(
            "geometry",
            geometry_field.data_type().clone(),
            geometry_field.is_nullable(),
        )
        .with_metadata(geometry_field.metadata().clone()),
    );
    new_fields.push(renamed_geometry_field);
    new_arrays.push(batch.column(geometry_field_index).clone());

    // Add properties struct if there are property columns
    if !property_fields.is_empty() {
        let properties_struct = StructArray::new(
            property_fields.into(),
            property_arrays,
            None, // No null buffer for the struct itself
        );

        let properties_field = Arc::new(Field::new(
            "properties",
            DataType::Struct(properties_struct.fields().clone()),
            false,
        ));

        new_fields.push(properties_field);
        new_arrays.push(Arc::new(properties_struct));
    } else {
        // If no properties, add an empty struct array
        let empty_struct = StructArray::new_empty_fields(batch.num_rows(), None);
        let properties_field = Arc::new(Field::new_struct("properties", Fields::empty(), true));
        new_fields.push(properties_field);
        new_arrays.push(Arc::new(empty_struct));
    }

    // Create the new schema and batch
    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_arrays)
}

#[cfg(test)]
mod test {
    use std::str::FromStr;
    use std::sync::Arc;
    use std::vec;

    use arrow_array::{Float64Array, Int32Array, StringArray};
    use arrow_schema::Schema;
    use geoarrow_array::test::point;
    use geoarrow_array::{GeoArrowArray, IntoArrow};
    use geoarrow_schema::{CoordType, Dimension};

    use super::*;

    #[test]
    fn test_geometry_encoder_factory() {
        let point_arr = point::array(CoordType::Interleaved, Dimension::XY);

        // Slice to avoid empty points
        let point_arr = point_arr.slice(0, 2);

        let field = point_arr.extension_type().to_field("geometry", true);
        let array = point_arr.to_array_ref();

        let schema = Schema::new(vec![Arc::new(field)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![array]).unwrap();

        let mut buffer = Vec::new();
        let mut geo_writer = GeoJsonWriter::new(&mut buffer).unwrap();
        geo_writer.write(&batch).unwrap();
        geo_writer.finish().unwrap();

        let s = String::from_utf8(buffer).unwrap();

        // Test against expected GeoJSON string
        let expected = r#"{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[30,10]},"properties":{}},{"type":"Feature","geometry":{"type":"Point","coordinates":[40,20]},"properties":{}}]}"#;

        assert_eq!(s, expected);
    }

    #[test]
    fn test_transform_batch_with_properties() {
        use arrow_array::{Int32Array, StringArray};
        use arrow_schema::{DataType, Field};

        // Create a point array
        let point_arr = point::array(CoordType::Interleaved, Dimension::XY);
        let point_arr = point_arr.slice(0, 2);
        let geometry_field = point_arr.extension_type().to_field("geom", true);
        let geometry_array = point_arr.to_array_ref();

        // Create property arrays
        let name_array = Arc::new(StringArray::from(vec!["Point A", "Point B"]));
        let value_array = Arc::new(Int32Array::from(vec![100, 200]));

        let schema = Schema::new(vec![
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(geometry_field),
            Arc::new(Field::new("value", DataType::Int32, false)),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![name_array, geometry_array, value_array],
        )
        .unwrap();

        // Transform the batch
        let transformed = transform_batch(&batch).unwrap();

        // Verify schema structure
        assert_eq!(transformed.schema().fields().len(), 3); // type, geometry + properties
        assert_eq!(transformed.schema().field(0).name(), "type");
        assert_eq!(transformed.schema().field(1).name(), "geometry");
        assert_eq!(transformed.schema().field(2).name(), "properties");

        // Properties should be a struct with name and value fields
        if let DataType::Struct(properties_fields) = transformed.schema().field(2).data_type() {
            assert_eq!(properties_fields.len(), 2);
            assert_eq!(properties_fields[0].name(), "name");
            assert_eq!(properties_fields[1].name(), "value");
        } else {
            panic!("Expected properties to be a struct");
        }
    }

    #[test]
    fn test_geojson_writer_with_properties() {
        // Create a point array with two points
        let point_arr = point::array(CoordType::default(), Dimension::XY);
        let point_arr = point_arr.slice(0, 2);
        let geometry_field = point_arr.extension_type().to_field("geometry", true);
        let geometry_array = point_arr.to_array_ref();

        // Create various property types
        let str_array = Arc::new(StringArray::from(vec!["A", "B"]));
        let count_array = Arc::new(Int32Array::from(vec![100, 200]));
        let value_array = Arc::new(Float64Array::from(vec![3.10, 2.71]));

        let schema = Schema::new(vec![
            Arc::new(geometry_field),
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("count", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Float64, false)),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![geometry_array, str_array, count_array, value_array],
        )
        .unwrap();

        // Write to GeoJSON
        let mut buffer = Vec::new();
        let mut writer = GeoJsonWriter::new(&mut buffer).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let geojson_string = String::from_utf8(buffer).unwrap();

        // Test against expected GeoJSON string
        let expected = r#"{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[30,10]},"properties":{"name":"A","count":100,"value":3.1}},{"type":"Feature","geometry":{"type":"Point","coordinates":[40,20]},"properties":{"name":"B","count":200,"value":2.71}}]}"#;

        assert_eq!(geojson_string, expected);

        // Also validate it's GeoJSON
        geojson::FeatureCollection::from_str(expected).expect("Expected GeoJSON to be valid");
    }

    #[test]
    fn test_geojson_writer_with_id_column() {
        use arrow_array::Int32Array;
        use arrow_schema::{DataType, Field};

        // Create a point array with id column
        let point_arr = point::array(CoordType::Interleaved, Dimension::XY);
        let point_arr = point_arr.slice(0, 2);
        let geometry_field = point_arr.extension_type().to_field("geom", true);
        let geometry_array = point_arr.to_array_ref();

        // Create id and property arrays
        let id_array = Arc::new(Int32Array::from(vec![101, 102]));
        let count_array = Arc::new(Int32Array::from(vec![10, 20]));

        let schema = Schema::new(vec![
            Arc::new(Field::new("id", DataType::Int32, false)),
            Arc::new(geometry_field),
            Arc::new(Field::new("count", DataType::Int32, false)),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![id_array, geometry_array, count_array],
        )
        .unwrap();

        // Write to GeoJSON
        let mut buffer = Vec::new();
        let mut writer = GeoJsonWriter::new(&mut buffer).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let geojson_string = String::from_utf8(buffer).unwrap();

        // Test against expected GeoJSON string with id field
        let expected = r#"{"type":"FeatureCollection","features":[{"type":"Feature","id":101,"geometry":{"type":"Point","coordinates":[30,10]},"properties":{"count":10}},{"type":"Feature","id":102,"geometry":{"type":"Point","coordinates":[40,20]},"properties":{"count":20}}]}"#;

        assert_eq!(geojson_string, expected);

        // Also validate it's GeoJSON
        geojson::FeatureCollection::from_str(expected).expect("Expected GeoJSON to be valid");
    }

    #[test]
    fn test_geojson_writer_no_properties() {
        // Create a simple point array with no properties
        let point_arr = point::array(CoordType::Interleaved, Dimension::XY);
        let point_arr = point_arr.slice(0, 2);
        let geometry_field = point_arr.extension_type().to_field("geometry", true);
        let geometry_array = point_arr.to_array_ref();

        let schema = Schema::new(vec![Arc::new(geometry_field)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![geometry_array]).unwrap();

        // Write to GeoJSON
        let mut buffer = Vec::new();
        let mut writer = GeoJsonWriter::new(&mut buffer).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let geojson_string = String::from_utf8(buffer).unwrap();

        // Test against expected GeoJSON string (no properties object when no properties exist)
        let expected = r#"{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[30,10]},"properties":{}},{"type":"Feature","geometry":{"type":"Point","coordinates":[40,20]},"properties":{}}]}"#;

        assert_eq!(geojson_string, expected);

        // Also validate it's GeoJSON
        geojson::FeatureCollection::from_str(expected).expect("Expected GeoJSON to be valid");
    }
}
