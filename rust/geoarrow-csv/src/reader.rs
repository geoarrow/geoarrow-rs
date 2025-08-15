//! Read from CSV files with a geometry column encoded as Well-Known Text.
//!
//! The CSV reader implements [`RecordBatchReader`], so you can iterate over the batches of the CSV
//! without materializing the entire file in memory.
//!
//! [`RecordBatchReader`]: arrow_array::RecordBatchReader

use std::io::Read;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::{LargeWktArray, WktArray, WktViewArray};
use geoarrow_array::cast::from_wkt;
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{GeoArrowType, WktType};

/// Options for the CSV reader.
#[derive(Debug, Clone)]
pub struct CsvReaderOptions {
    /// The name of the geometry column in the CSV
    ///
    /// Defaults to `"geometry"`
    pub geometry_column_name: Option<String>,

    /// The target geometry type to convert the WKT strings to.
    pub to_type: GeoArrowType,
}

/// A CSV reader that parses a WKT-encoded geometry column
pub struct CsvReader<R> {
    reader: arrow_csv::Reader<R>,
    output_schema: SchemaRef,
    geometry_column_index: usize,
    to_type: GeoArrowType,
}

impl<R> CsvReader<R> {
    /// Access the schema of this reader
    pub fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl<R: Read> CsvReader<R> {
    /// Wrap an upstream `arrow_csv::Reader` in an iterator that parses WKT geometries.
    pub fn try_new(
        reader: arrow_csv::Reader<R>,
        options: CsvReaderOptions,
    ) -> GeoArrowResult<Self> {
        let schema = reader.schema();
        let geometry_column_name =
            find_geometry_column(&schema, options.geometry_column_name.as_deref())?;
        let geometry_column_index = schema.index_of(&geometry_column_name)?;

        // Transform to output schema
        let mut output_fields = schema.fields().to_vec();
        output_fields[geometry_column_index] =
            options.to_type.to_field(geometry_column_name, true).into();

        let output_schema = Arc::new(Schema::new_with_metadata(
            output_fields,
            schema.metadata().clone(),
        ));

        Ok(Self {
            reader,
            output_schema,
            geometry_column_index,
            to_type: options.to_type,
        })
    }
}

impl<R: Read> Iterator for CsvReader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let reader = &mut self.reader;
        reader.next().map(move |batch| {
            parse_batch(
                batch,
                self.output_schema.clone(),
                self.geometry_column_index,
                self.to_type.clone(),
            )
        })
    }
}

impl<R: Read> arrow_array::RecordBatchReader for CsvReader<R> {
    fn schema(&self) -> SchemaRef {
        self.schema()
    }
}

fn parse_batch(
    batch: Result<RecordBatch, ArrowError>,
    output_schema: SchemaRef,
    geometry_column_index: usize,
    to_type: GeoArrowType,
) -> Result<RecordBatch, ArrowError> {
    let batch = batch?;
    let column = batch.column(geometry_column_index);

    let parsed_arr = match column.data_type() {
        DataType::Utf8 => {
            let arr = WktArray::try_from((column.as_ref(), WktType::default()))?;
            from_wkt(&arr, to_type)
        }
        DataType::LargeUtf8 => {
            let arr = LargeWktArray::try_from((column.as_ref(), WktType::default()))?;
            from_wkt(&arr, to_type)
        }
        DataType::Utf8View => {
            let arr = WktViewArray::try_from((column.as_ref(), WktType::default()))?;
            from_wkt(&arr, to_type)
        }
        _ => unreachable!(),
    }?;

    // Replace column in record batch
    let mut columns = batch.columns().to_vec();
    columns[geometry_column_index] = parsed_arr.into_array_ref();

    RecordBatch::try_new(output_schema, columns)
}

fn find_geometry_column(
    schema: &Schema,
    geometry_column_name: Option<&str>,
) -> GeoArrowResult<String> {
    if let Some(geometry_col_name) = geometry_column_name {
        if schema
            .fields()
            .iter()
            .any(|field| field.name() == geometry_col_name)
        {
            Ok(geometry_col_name.to_string())
        } else {
            Err(ArrowError::CsvError(format!(
                "CSV geometry column specified to have name '{}' but no such column found",
                geometry_col_name
            ))
            .into())
        }
    } else {
        let mut field_name: Option<String> = None;
        for field in schema.fields().iter() {
            if field.name().to_lowercase().as_str() == "geometry" {
                field_name = Some(field.name().clone());
            }
        }
        field_name.ok_or(
            ArrowError::CsvError(
                "No CSV geometry column name specified and no geometry column found.".to_string(),
            )
            .into(),
        )
    }
}

#[cfg(test)]
mod tests {

    use std::io::Cursor;

    use arrow_csv::ReaderBuilder;
    use arrow_csv::reader::Format;
    use geo_traits::{CoordTrait, PointTrait};
    use geoarrow_array::GeoArrowArrayAccessor;
    use geoarrow_array::array::PointArray;
    use geoarrow_schema::{Dimension, PointType};

    use super::*;

    #[test]
    fn read_csv() {
        let s = r#"
address,type,datetime,report location,incident number
904 7th Av,Car Fire,05/22/2019 12:55:00 PM,POINT (-122.329051 47.6069),F190051945
9610 53rd Av S,Aid Response,05/22/2019 12:55:00 PM,POINT (-122.266529 47.515984),F190051946"#;

        let format = Format::default().with_header(true);
        let (schema, _num_read_records) = format.infer_schema(Cursor::new(s), None).unwrap();
        let reader = ReaderBuilder::new(schema.into())
            .with_format(format)
            .build(Cursor::new(s))
            .unwrap();

        let point_type = PointType::new(Dimension::XY, Default::default());
        let to_type = GeoArrowType::Point(point_type.clone());
        let geo_options = CsvReaderOptions {
            geometry_column_name: Some("report location".to_string()),
            to_type: to_type.clone(),
        };
        let geo_reader = CsvReader::try_new(reader, geo_options).unwrap();

        let batches: Vec<_> = geo_reader.collect::<Result<Vec<_>, _>>().unwrap();
        let batch = batches.into_iter().next().unwrap();
        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 5);

        let geom_field = schema.field(3);
        let actual = GeoArrowType::from_extension_field(geom_field).unwrap();
        assert_eq!(actual, to_type);

        let geom_array = batch.column(3);
        let point_arr = PointArray::try_from((geom_array.as_ref(), point_type)).unwrap();
        assert_eq!(point_arr.len(), 2);
        let point1 = point_arr.value(0).unwrap();
        assert_eq!(point1.coord().unwrap().x(), -122.329051);
        assert_eq!(point1.coord().unwrap().y(), 47.6069);

        let point2 = point_arr.value(1).unwrap();
        assert_eq!(point2.coord().unwrap().x(), -122.266529);
        assert_eq!(point2.coord().unwrap().y(), 47.515984);

        // arrow_csv::reader::infer_schema_from_files(files, delimiter, max_read_records, has_header)
        //         infer_schema_from_files(files, delimiter, max_read_records, has_header)
    }
}
