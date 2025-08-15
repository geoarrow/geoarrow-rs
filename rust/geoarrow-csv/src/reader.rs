use std::io::Read;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::{LargeWktArray, WktArray, WktViewArray};
use geoarrow_array::cast::from_wkt;
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, GeoArrowType, WktType};

/// Options for the CSV reader.
#[derive(Debug, Clone)]
pub struct CsvReaderOptions {
    /// The GeoArrow coordinate type to use in the geometry arrays.
    pub coord_type: CoordType,

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
