#![doc = include_str!("README.md")]

#[cfg(feature = "async")]
mod r#async;
mod common;
#[cfg(feature = "object_store")]
pub mod object_store;
pub mod schema;
mod sync;
mod table_builder;

#[cfg(feature = "async")]
pub use r#async::FlatGeobufRecordBatchStream;
pub use common::{FlatGeobufHeaderExt, FlatGeobufReaderOptions};
pub use sync::FlatGeobufRecordBatchIterator;

#[cfg(test)]
mod tests {
    use std::path::Path;

    use arrow_array::RecordBatchReader;
    use flatgeobuf::FgbReader;
    use geoarrow_schema::CoordType;

    use crate::reader::{
        FlatGeobufHeaderExt, FlatGeobufReaderOptions, FlatGeobufRecordBatchIterator,
    };

    #[test]
    fn test() {
        let use_view_types = true;
        let coord_type = CoordType::Separated;
        let batch_size = 65536;
        let read_geometry = true;

        let path = Path::new("/Users/kyle/Downloads/ns-water_water-line.fgb");
        let sync_reader = std::fs::File::open(path).unwrap();

        let fgb_reader = FgbReader::open(sync_reader.try_clone().unwrap()).unwrap();
        let fgb_header = fgb_reader.header();
        dbg!(&fgb_header);

        let properties_schema = fgb_header.properties_schema(use_view_types).unwrap();
        dbg!(&properties_schema);

        let geometry_type = fgb_header.geoarrow_type(coord_type).unwrap();
        let selection = fgb_reader.select_all().unwrap();

        let options = FlatGeobufReaderOptions::new(properties_schema, geometry_type)
            .with_batch_size(batch_size)
            .with_read_geometry(read_geometry);
        let record_batch_reader =
            FlatGeobufRecordBatchIterator::try_new(selection, options).unwrap();
        let schema = record_batch_reader.schema();
        let batches = record_batch_reader.collect::<Result<Vec<_>, _>>().unwrap();

        // Ok(Arro3Table::from(PyTable::try_new(batches, schema).unwrap()))
    }
}
