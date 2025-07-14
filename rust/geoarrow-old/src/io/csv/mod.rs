//! Read from and write to CSV files.
//!
//! The CSV reader implements [`RecordBatchReader`], so you can iterate over the batches of the CSV
//! without materializing the entire file in memory.
//!
//! [`RecordBatchReader`]: arrow_array::RecordBatchReader
//!
//! Additionally, the CSV writer takes in a [`RecordBatchReader`], so you can write an Arrow
//! iterator to CSV without materializing all batches in memory at once.
//!
//! # Examples
//!
//! ```ignore
//! use std::io::{Cursor, Seek};
//!
//! use arrow_array::RecordBatchReader;
//!
//! use geoarrow_schema::CoordType;
//! use geoarrow::io::csv::{CSVReader, CSVReaderOptions};
//! use geoarrow::table::Table;
//!
//! let s = r#"
//! address,type,datetime,report location,incident number
//! 904 7th Av,Car Fire,05/22/2019 12:55:00 PM,POINT (-122.329051 47.6069),F190051945
//! 9610 53rd Av S,Aid Response,05/22/2019 12:55:00 PM,POINT (-122.266529 47.515984),F190051946"#;
//!
//! let options = CSVReaderOptions {
//!     coord_type: CoordType::Separated,
//!     geometry_column_name: Some("report location".to_string()),
//!     has_header: Some(true),
//!     ..Default::default()
//! };
//! let reader = CSVReader::try_new(Cursor::new(s), options).unwrap();
//!
//! // Now `reader` implements `arrow_array::RecordBatchReader`, so we can use TryFrom to convert
//! // it to a geoarrow Table
//! let table = Table::try_from(Box::new(reader) as Box<dyn arrow_array::RecordBatchReader>).unwrap();
//! ```

pub use reader::{CSVReader, CSVReaderOptions};
pub use writer::write_csv;

mod reader;
mod writer;
