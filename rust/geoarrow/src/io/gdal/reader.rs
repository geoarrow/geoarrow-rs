use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::record_batch::RecordBatchReader;
use gdal::cpl::CslStringList;
use gdal::vector::Layer;
use gdal::vector::LayerAccess;

use crate::error::Result;

/// Read a GDAL layer to an Arrow [RecordBatchReader].
///
/// The output stream will contain geometries in WKB encoding.
///
/// Note that this expects GDAL 3.8 or later to propagate the CRS information correctly.
pub fn read_gdal(
    layer: &mut Layer,
    batch_size: Option<usize>,
) -> Result<Box<dyn RecordBatchReader + Send>> {
    // Instantiate an `ArrowArrayStream` for OGR to write into
    let mut output_stream = FFI_ArrowArrayStream::empty();

    // Take a pointer to it
    let output_stream_ptr = &mut output_stream as *mut FFI_ArrowArrayStream;

    // GDAL includes its own copy of the ArrowArrayStream struct definition. These are guaranteed
    // to be the same across implementations, but we need to manually cast between the two for Rust
    // to allow it.
    let gdal_pointer: *mut gdal::ArrowArrayStream = output_stream_ptr.cast();

    let mut options = CslStringList::new();
    if let Some(batch_size) = batch_size {
        options.set_name_value("MAX_FEATURES_IN_BATCH", batch_size.to_string().as_str())?;
    }
    options.set_name_value("GEOMETRY_METADATA_ENCODING", "GEOARROW")?;

    // Read the layer's data into our provisioned pointer
    unsafe { layer.read_arrow_stream(gdal_pointer, &options)? }

    let arrow_stream_reader = ArrowArrayStreamReader::try_new(output_stream)?;
    Ok(Box::new(arrow_stream_reader))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::schema::GeoSchemaExt;
    use gdal::Dataset;
    use std::path::Path;

    #[test]
    fn test_read_gdal() -> Result<()> {
        // Open a dataset and access a layer
        let dataset = Dataset::open(Path::new("fixtures/flatgeobuf/countries.fgb"))?;
        let mut layer = dataset.layer(0)?;
        let table = read_gdal(&mut layer, None)?;
        dbg!(table.schema().as_ref().geometry_columns());

        Ok(())
    }
}
