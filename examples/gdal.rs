use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use gdal::cpl::CslStringList;
use gdal::vector::*;
use gdal::Dataset;
use std::path::Path;

fn run() -> gdal::errors::Result<()> {
    // Open a dataset and access a layer
    let dataset_a = Dataset::open(Path::new("fixtures/roads.geojson"))?;
    let mut layer_a = dataset_a.layer(0)?;

    // Instantiate an `ArrowArrayStream` for OGR to write into
    let mut output_stream = Box::new(FFI_ArrowArrayStream::empty());

    // Access the unboxed pointer
    let output_stream_ptr = &mut *output_stream as *mut FFI_ArrowArrayStream;

    // gdal includes its own copy of the ArrowArrayStream struct definition. These are guaranteed
    // to be the same across implementations, but we need to manually cast between the two for Rust
    // to allow it.
    let gdal_pointer: *mut gdal::ArrowArrayStream = output_stream_ptr.cast();

    let mut options = CslStringList::new();
    options.set_name_value("INCLUDE_FID", "NO")?;

    // Read the layer's data into our provisioned pointer
    unsafe { layer_a.read_arrow_stream(gdal_pointer, &options).unwrap() }

    // The rest of this example is arrow2-specific.

    // `arrow2` has a helper class `ArrowArrayStreamReader` to assist with iterating over the raw
    // batches
    let arrow_stream_reader =
        unsafe { ArrowArrayStreamReader::from_raw(output_stream_ptr).unwrap() };

    // Iterate over the stream until it's finished
    for maybe_record_batch in arrow_stream_reader {
        // Access the contained array
        let _record_batch = maybe_record_batch.unwrap();

        // // The top-level array is a single logical "struct" array which includes all columns of the
        // // dataset inside it.
        // assert!(
        //     matches!(top_level_array.data_type(), DataType::Struct(..)),
        //     "Top-level arrays from OGR are expected to be of struct type"
        // );

        // // Downcast from the Box<dyn Array> to a concrete StructArray
        // let struct_array = top_level_array
        //     .as_any()
        //     .downcast_ref::<StructArray>()
        //     .unwrap();

        // // Access the underlying column metadata and data
        // // Clones are cheap because they do not copy the underlying data
        // let (fields, columns, _validity) = struct_array.clone().into_data();

        // // Find the index of the geometry column
        // let geom_column_index = fields
        //     .iter()
        //     .position(|field| field.name == "wkb_geometry")
        //     .unwrap();

        // // Pick that column and downcast to a BinaryArray
        // let geom_column = &columns[geom_column_index];
        // let binary_array = geom_column
        //     .as_any()
        //     .downcast_ref::<BinaryArray<i32>>()
        //     .unwrap();

        // let wkb_array = WKBArray::new(binary_array.clone());
        // let line_string_array: LineStringArray<i32> = wkb_array.try_into().unwrap();

        // let geodesic_length = line_string_array.geodesic_length();

        // println!("Number of geometries: {}", line_string_array.len());
        // println!("Geodesic Length: {:?}", geodesic_length);
    }

    Ok(())
}

fn main() {
    run().unwrap()
}
