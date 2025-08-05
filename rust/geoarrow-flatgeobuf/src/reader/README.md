Read from [FlatGeobuf](https://flatgeobuf.org/) files.

This module provides the ability to read FlatGeobuf files as synchronous
iterators or asynchronous streams of [`RecordBatch`][arrow_array::RecordBatch]es
with GeoArrow metadata.

The APIs in this crate are meant to be used in conjunction with the
[`flatgeobuf`] crate.

## Overview

The general overview of reading a GeoParquet file is as follows:

1. Open up a sync or async FlatGeobuf reader using [`flatgeobuf::FgbReader`] or [`flatgeobuf::HttpFgbReader`].
2. Use the [`FlatGeobufHeaderExt`] trait to infer a GeoArrow geometry type and an Arrow schema from the properties of the FlatGeobuf file.
3. If there's no property information stored in the FlatGeobuf file's header metadata, you'll need to scan some features of the file to infer a property schema. Open up a _new reader_ on the same file, then pass that reader to [`FlatGeobufSchemaScanner`][schema::FlatGeobufSchemaScanner] to infer a schema.
4. Create a [`FlatGeobufReaderOptions`] with the desired output schema. Note that you can apply a projection here (select specified columns) and change string/binary/timestamp data types if desired.
5. Pass the FlatGeobuf reader + the reader options to `FlatGeobufRecordBatchIterator` or `FlatGeobufRecordBatchStream` to create an iterator or stream of GeoArrow data.

    Now any [`RecordBatch`][arrow_array::RecordBatch]es emitted will have GeoArrow metadata on the geometry column.

## Synchronous reader

```rust
use std::fs::File;
use std::io::BufReader;

use arrow_array::RecordBatchReader;
use flatgeobuf::FgbReader;
use geoarrow_flatgeobuf::reader::schema::FlatGeobufSchemaScanner;
use geoarrow_flatgeobuf::reader::{
    FlatGeobufHeaderExt, FlatGeobufReaderOptions, FlatGeobufRecordBatchIterator,
};

let path = "../../fixtures/flatgeobuf/countries.fgb";
let filein = BufReader::new(File::open(path).unwrap());
let fgb_reader = FgbReader::open(filein).unwrap();
let fgb_header = fgb_reader.header();

let properties_schema = if let Some(properties_schema) = fgb_header.properties_schema(true) {
    properties_schema
} else {
    // If the file does not contain column information in metadata, we need to scan features to
    // infer a schema.
    let filein_for_scan = BufReader::new(File::open(path).unwrap());
    let fgb_reader_scan = FgbReader::open(filein_for_scan).unwrap();

    let mut scanner = FlatGeobufSchemaScanner::new(true);
    let max_read_records = Some(1000);
    scanner
        .process(fgb_reader_scan.select_all().unwrap(), max_read_records)
        .unwrap();
    scanner.finish()
};

let geometry_type = fgb_header.geoarrow_type(Default::default()).unwrap();

let selection = fgb_reader.select_all().unwrap();
let options = FlatGeobufReaderOptions::new(properties_schema, geometry_type);
let record_batch_reader = FlatGeobufRecordBatchIterator::try_new(selection, options).unwrap();
let schema = record_batch_reader.schema();
let batches = record_batch_reader.collect::<Result<Vec<_>, _>>().unwrap();

println!("Schema: {schema}");
println!("Num batches: {}", batches.len());
```

### Reading with spatial filter

To read with a spatial filter, just call [`select_bbox`][flatgeobuf::FgbReader::select_bbox] or [`select_bbox_seq`][flatgeobuf::FgbReader::select_bbox_seq] instead of `select_all`/`select_all_seq`. (Note that `select_bbox` should be faster than `select_bbox_seq`, and should be preferred when the underlying reader supports `Seek`.)

## Asynchronous reader

```rust
# #[cfg(all(feature = "async", feature = "object_store"))]
# {
use std::env::current_dir;
use std::sync::Arc;

use flatgeobuf::HttpFgbReader;
use futures::TryStreamExt;
use geoarrow_flatgeobuf::reader::object_store::ObjectStoreWrapper;
use geoarrow_flatgeobuf::reader::{
    FlatGeobufHeaderExt, FlatGeobufReaderOptions, FlatGeobufRecordBatchStream,
};
use http_range_client::AsyncBufferedHttpRangeClient;
use object_store::local::LocalFileSystem;

# tokio_test::block_on(async {
let store = Arc::new(
    LocalFileSystem::new_with_prefix(current_dir().unwrap().parent().unwrap().parent().unwrap()).unwrap()
);
let location = "fixtures/flatgeobuf/countries.fgb".into();
let object_store_wrapper = ObjectStoreWrapper::new(store, location);
let async_client = AsyncBufferedHttpRangeClient::with(object_store_wrapper, "");
let fgb_reader = HttpFgbReader::new(async_client).await.unwrap();
let fgb_header = fgb_reader.header();

// Follow the synchronous example for schema inference if needed
let properties_schema = fgb_header.properties_schema(true).unwrap();
let geometry_type = fgb_header.geoarrow_type(Default::default()).unwrap();

let selection = fgb_reader.select_all().await.unwrap();
let options = FlatGeobufReaderOptions::new(properties_schema, geometry_type);
let record_batch_stream = FlatGeobufRecordBatchStream::try_new(selection, options).unwrap();
let schema = record_batch_stream.schema();
let batches = record_batch_stream.try_collect::<Vec<_>>().await.unwrap();

println!("Schema: {schema}");
println!("Num batches: {}", batches.len());
# })
# }
```
