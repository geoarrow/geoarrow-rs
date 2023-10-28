use std::sync::Arc;

use arrow_array::Array;

pub(super) fn read_geometry_column(_path: &str) -> Arc<dyn Array> {
    todo!()
    // let mut reader = File::open(path).unwrap();

    // // we can read its metadata:
    // let metadata = read::read_metadata(&mut reader).unwrap();

    // // and infer a [`Schema`] from the `metadata`.
    // let schema = read::infer_schema(&metadata).unwrap();

    // // we can filter the columns we need (here we select all)
    // let schema = schema.filter(|_index, _field| true);

    // // we can then read the row groups into chunks
    // let mut chunks = read::FileReader::new(
    //     reader,
    //     metadata.row_groups,
    //     schema,
    //     Some(1024 * 8 * 8),
    //     None,
    //     None,
    // );

    // let first_chunk = chunks.next().unwrap().unwrap();
    // first_chunk.arrays()[0].clone()
}
