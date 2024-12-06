use arrow::array::AsArray;
use arrow_array::RecordBatch;
use arrow_csv::reader::Format;
use arrow_csv::ReaderBuilder;
use arrow_schema::{Schema, SchemaRef};
use geozero::csv::CsvReader;
use geozero::GeozeroDatasource;
use std::io::Read;

use crate::array::{CoordType, WKTArray};
use crate::datatypes::Dimension;
use crate::error::{GeoArrowError, Result};
use crate::io::geozero::array::MixedGeometryStreamBuilder;
use crate::io::geozero::table::{GeoTableBuilder, GeoTableBuilderOptions};
use crate::io::wkt::read_wkt;
use crate::table::Table;

/// Options for the CSV reader.
#[derive(Debug, Clone)]
pub struct CSVReaderOptions {
    /// The GeoArrow coordinate type to use in the geometry arrays.
    pub coord_type: CoordType,

    /// The number of rows in each batch.
    pub batch_size: usize,

    /// The name of the geometry column in the CSV
    ///
    /// Defaults to `"geometry"`
    pub geometry_column_name: Option<String>,

    /// Specify whether the CSV file has a header, defaults to `true`
    ///
    /// When `true`, the first row of the CSV file is treated as a header row
    pub has_header: Option<bool>,

    pub max_records: Option<usize>,

    /// Specify a custom delimiter character, defaults to comma `','`
    pub delimiter: Option<char>,

    /// Specify an escape character, defaults to `None`
    pub escape: Option<char>,

    /// Specify a custom quote character, defaults to double quote `'"'`
    pub quote: Option<char>,

    /// Specify a custom terminator character, defaults to CRLF
    pub terminator: Option<char>,

    /// Specify a comment character, defaults to `None`
    ///
    /// Lines starting with this character will be ignored
    pub comment: Option<char>,
}

impl CSVReaderOptions {
    fn to_format(&self) -> Format {
        let mut format = Format::default();

        if let Some(has_header) = self.has_header {
            format = format.with_header(has_header);
        }
        if let Some(delimiter) = self.delimiter {
            format = format.with_delimiter(delimiter as u8);
        }
        if let Some(escape) = self.escape {
            format = format.with_escape(escape as u8);
        }
        if let Some(quote) = self.quote {
            format = format.with_quote(quote as u8);
        }
        if let Some(terminator) = self.terminator {
            format = format.with_terminator(terminator as u8);
        }
        if let Some(comment) = self.comment {
            format = format.with_comment(comment as u8);
        }

        format
    }
}

impl Default for CSVReaderOptions {
    fn default() -> Self {
        Self {
            coord_type: Default::default(),
            batch_size: 65_536,
            geometry_column_name: Default::default(),
            has_header: Default::default(),
            max_records: Default::default(),
            delimiter: Default::default(),
            escape: Default::default(),
            quote: Default::default(),
            terminator: Default::default(),
            comment: Default::default(),
        }
    }
}

/// Infer a CSV file's schema
/// Returns (Schema, records_read, geometry column name)
///
/// Note that the geometry column in the Schema is still left as a String.
pub fn infer_csv_schema(
    reader: impl Read,
    options: &CSVReaderOptions,
) -> Result<(Schema, usize, String)> {
    let format = options.to_format();
    let (schema, records_read) = format.infer_schema(reader, options.max_records)?;

    let geometry_col_name = find_geometry_column(
        &schema,
        options.geometry_column_name.as_ref().map(|x| x.as_str()),
    )?;

    Ok((schema, records_read, geometry_col_name))
}

/// Read a CSV file to a Table
///
/// Note that this is Read and not Read + Seek. This means that you must infer the schema yourself
/// before calling this function. This allows using with objects that are only `Read` in the case
/// when you already know the file's schema.
pub fn read_csv<R: Read>(
    mut reader: R,
    schema: SchemaRef,
    options: CSVReaderOptions,
) -> Result<Table> {
    let geometry_column_name = find_geometry_column(
        schema.as_ref(),
        options.geometry_column_name.as_ref().map(|x| x.as_str()),
    )?;
    let mut builder = ReaderBuilder::new(schema)
        .with_format(options.to_format())
        .with_batch_size(options.batch_size);

    // Transform to output schema
    // TODO:
    let output_schema = Schema::new(vec![]);

    // TODO:
    let geometry_column_index = 0;

    let reader = builder.build(reader)?;
    for record_batch in reader {
        let record_batch = record_batch?;
        let column = record_batch.column(geometry_column_index);
        let str_col = column.as_string::<i32>();
        let wkt_arr = WKTArray::new(str_col.clone(), Default::default());
        let geom_arr = read_wkt(&wkt_arr, options.coord_type, true)?;

        // Replace column in record batch
        let mut columns = record_batch.columns().to_vec();
        columns[geometry_column_index] = geom_arr.into_array_ref();

        let new_batch = RecordBatch::try_new(output_schema, columns).unwrap();
        // TODO: yield this record batch.
    }

    todo!();
}

fn find_geometry_column(schema: &Schema, geometry_column_name: Option<&str>) -> Result<String> {
    if let Some(geometry_col_name) = geometry_column_name {
        if schema
            .fields()
            .iter()
            .any(|field| field.name() == geometry_col_name)
        {
            Ok(geometry_col_name.to_string())
        } else {
            return Err(GeoArrowError::General(format!(
                "CSV geometry column specified to have name '{}' but no such column found",
                geometry_col_name
            )));
        }
    } else {
        let mut field_name: Option<String> = None;
        for field in schema.fields().iter() {
            if field.name().to_lowercase().as_str() == "geometry" {
                field_name = Some(field.name().clone());
            }
        }
        field_name.ok_or(GeoArrowError::General(
            "No CSV geometry column name specified and no geometry column found.".to_string(),
        ))
    }
}
