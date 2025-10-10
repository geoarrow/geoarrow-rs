# geoarrow-csv

Read and write CSV files with a geometry column encoded as [Well-Known Text (WKT)](https://libgeos.org/specifications/wkt/).

This crate provides efficient streaming readers and writers for CSV files containing geospatial data, converting between WKT string representations and GeoArrow's columnar format.

## Reading CSV Files

Use `CsvReader` to read CSV files with WKT-encoded geometry columns. The reader implements `RecordBatchReader` for batched processing.

### Example

`example.csv` contains sample data:

```csv
address,type,datetime,report location,incident number
904 7th Ave,Car Fire,05/22/19,POINT (-122.329051 47.6069),F190051945
9610 53rd Av S,Aid Response,"05/22/2019 12:55:00 PM,",POINT (-122.266529 47.515984),F190051946
```

```rust
use std::fs::File;
use std::io::BufReader;
use arrow_csv::ReaderBuilder;
use geoarrow_csv::reader::{CsvReader, CsvReaderOptions};
use geoarrow_schema::{GeoArrowType, PointType, Dimension};

let file = File::open("example.csv").unwrap();
let mut buf_reader = BufReader::new(file);

// Create Arrow CSV reader with schema inference
let format = arrow_csv::reader::Format::default().with_header(true);
let (schema, _) = format.infer_schema(&mut buf_reader, None).unwrap();
let arrow_reader = ReaderBuilder::new(schema.into())
    .with_format(format)
    .build(buf_reader).unwrap();

let point_type = PointType::new(Dimension::XY, Default::default());
let options = CsvReaderOptions {
    geometry_column_name: Some("report location".to_string()),
    to_type: GeoArrowType::Point(point_type),
};

// Create the GeoArrow CSV reader
let mut geo_reader = CsvReader::try_new(arrow_reader, options).unwrap();

for batch_result in geo_reader {
    let batch = batch_result.unwrap();
    println!("Read {} rows", batch.num_rows());
}
```

## Writing CSV Files

Use `CsvWriter` to export GeoArrow data to CSV format with WKT-encoded geometries.

### Example

```rust
use std::fs::File;
use std::io::BufReader;

use arrow_csv::WriterBuilder;
use arrow_csv::ReaderBuilder;
use geoarrow_csv::writer::CsvWriter;
use geoarrow_csv::reader::{CsvReader, CsvReaderOptions};
use geoarrow_schema::{PointType, Dimension, GeoArrowType};

let in_file = File::open("example.csv").unwrap();
let out_file = File::create("output.csv").unwrap();

// Setting up a Reader for in_file to read batches to write to out_file
let mut buf_reader = BufReader::new(in_file);
let format = arrow_csv::reader::Format::default().with_header(true);
let (schema, _) = format.infer_schema(&mut buf_reader, None).unwrap();
let arrow_reader = ReaderBuilder::new(schema.into())
    .with_format(format)
    .build(buf_reader).unwrap();
let point_type = PointType::new(Dimension::XY, Default::default());
let options = CsvReaderOptions {
    geometry_column_name: Some("report location".to_string()),
    to_type: GeoArrowType::Point(point_type),
};
let mut geo_reader = CsvReader::try_new(arrow_reader, options).unwrap();

// Setting up our Writer
let arrow_writer = WriterBuilder::new().with_header(true).build(out_file);
let mut csv_writer = CsvWriter::new(arrow_writer);

for batch_result in geo_reader {
    let batch = batch_result.unwrap();
    csv_writer.write(&batch).unwrap();
    println!("Wrote {} rows ", batch.num_rows())
}
```

## Supported WKT Geometries

All geometry types allowed by the GeoArrow WKT specification are supported. This includes 2D, 3D, and 4D geometries, but does not include extended types like curves.
