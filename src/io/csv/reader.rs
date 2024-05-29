use crate::{
    array::MixedGeometryArray,
    chunked_array::{ChunkedGeometryArrayTrait, ChunkedMixedGeometryArray},
    error::{GeoArrowError, Result},
    io::geozero::FromWKT,
    table::Table,
};
use arrow::array::StringArray;
use arrow_array::Array;
use arrow_csv::{reader::Format, ReaderBuilder};
use std::{
    io::{Read, Seek},
    sync::Arc,
};

/// Options for the CSV reader.
pub struct CSVReaderOptions {
    /// The number of records to read to infer the file's schema.
    pub max_records: Option<usize>,
}

impl CSVReaderOptions {
    pub fn new(max_records: Option<usize>) -> Self {
        Self { max_records }
    }
}

impl Default for CSVReaderOptions {
    fn default() -> Self {
        Self::new(None)
    }
}

/// Reads Comma-Separated Value (CSV) data into a [Table].
///
/// # Examples
///
/// ```
/// use std::io::Cursor;
///
/// let s = r#"address,type,datetime,report location,incident number
/// 904 7th Av,Car Fire,05/22/2019 12:55:00 PM,POINT (-122.329051 47.6069),F190051945
/// 9610 53rd Av S,Aid Response,05/22/2019 12:55:00 PM,POINT (-122.266529 47.515984),F190051946"#;
/// let cursor = Cursor::new(s);
/// # #[cfg(feature = "csv")]
/// let table = geoarrow::io::csv::read_csv(s, "report location", Default::default()).unwrap();
/// ```
///
/// By default, the reader will scan the entire CSV file to infer the data
/// schema. If your data are large, you can limit the number of records scanned
/// with the [CSVReaderOptions]:
///
/// ```
/// # #[cfg(feature = "csv")]
/// # {
/// use geoarrow::io::csv::CSVReaderOptions;
/// let options = CSVReaderOptions::new(100);
/// # }
/// ```
pub fn read_csv<R: Read + Seek>(
    mut reader: R,
    geometry_column_name: &str,
    options: CSVReaderOptions,
) -> Result<Table> {
    let format = Format::default().with_header(true);
    let (schema, _) = format.infer_schema(&mut reader, options.max_records)?;
    let schema = Arc::new(schema);
    reader.rewind()?;
    let geometry_column_index = schema.index_of(geometry_column_name)?;
    let geometry_field = schema.field(geometry_column_index);
    let mut batches = Vec::new();
    let mut geometry_chunks = Vec::new();
    for result in ReaderBuilder::new(schema.clone())
        .with_format(format)
        .build(reader)?
    {
        let batch = result?;
        let geometry_strings: &StringArray = batch
            .column(geometry_column_index)
            .as_any()
            .downcast_ref()
            .ok_or_else(|| {
                GeoArrowError::General(format!(
                    "could not downcast column '{}' to string array",
                    geometry_column_name
                ))
            })?;
        geometry_chunks.push(MixedGeometryArray::<i32>::from_wkt(
            geometry_strings,
            Default::default(),
            Default::default(),
            false,
        )?);
        batches.push(batch);
    }
    let mut table = Table::try_new(schema.clone(), batches)?;
    let geometries = ChunkedMixedGeometryArray::new(geometry_chunks);
    // FIXME this will panic if we have zero records
    let field = geometry_field
        .clone()
        .with_data_type(geometries.array_refs()[0].data_type().clone());
    // FIXME the field's data type is inferred from its contained data, so (e.g.
    // in the case of our test data) the union _only_ includes points, not all
    // the mixed types. This leads to a schema mismatch in `arrow_array::RecordBatch::try_new_impl`.
    table.set_column(geometry_column_index, field.into(), geometries.array_refs())?;
    Ok(table)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    #[test]
    fn read() {
        // From https://github.com/georust/geozero/blob/b680d124f72fa48f7ccdc8045dc1398aa091b764/geozero/src/csv/csv_reader.rs#L234C13-L236C95
        let s = r#"address,type,datetime,report location,incident number
904 7th Av,Car Fire,05/22/2019 12:55:00 PM,POINT (-122.329051 47.6069),F190051945
9610 53rd Av S,Aid Response,05/22/2019 12:55:00 PM,POINT (-122.266529 47.515984),F190051946"#;
        let cursor = Cursor::new(s);
        super::read_csv(cursor, "report location", Default::default()).unwrap();
    }
}
