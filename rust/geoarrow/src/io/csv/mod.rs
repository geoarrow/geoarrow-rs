//! Read from and write to CSV files.
//!
//! # Examples
//!
//! ```
//! use std::io::{Cursor, Seek};
//!
//! use arrow_array::RecordBatchReader;
//!
//! use geoarrow::array::CoordType;
//! use geoarrow::io::csv::{infer_csv_schema, read_csv, CSVReaderOptions};
//! use geoarrow::table::Table;
//!
//! let s = r#"
//! address,type,datetime,report location,incident number
//! 904 7th Av,Car Fire,05/22/2019 12:55:00 PM,POINT (-122.329051 47.6069),F190051945
//! 9610 53rd Av S,Aid Response,05/22/2019 12:55:00 PM,POINT (-122.266529 47.515984),F190051946"#;
//! let mut cursor = Cursor::new(s);
//!
//! let options = CSVReaderOptions {
//!     coord_type: CoordType::Separated,
//!     geometry_column_name: Some("report location".to_string()),
//!     has_header: Some(true),
//!     ..Default::default()
//! };
//!
//! // Note: this initial schema currently represents the CSV data _on disk_. That is, the
//! // geometry column is represented as a string. This may change in the future.
//! let (schema, _read_records, _geometry_column_name) =
//!     infer_csv_schema(&mut cursor, &options).unwrap();
//! cursor.rewind().unwrap();
//!
//! // `read_csv` returns a RecordBatchReader, which enables streaming the CSV without reading
//! // all of it.
//! let record_batch_reader = read_csv(cursor, schema, options).unwrap();
//! let geospatial_schema = record_batch_reader.schema();
//! let table = Table::try_new(
//!     record_batch_reader.collect::<Result<_, _>>().unwrap(),
//!     geospatial_schema,
//! )
//! .unwrap();
//! ```
//!

pub use reader::{infer_csv_schema, read_csv, CSVReaderOptions};
pub use writer::write_csv;

mod reader;
mod writer;
