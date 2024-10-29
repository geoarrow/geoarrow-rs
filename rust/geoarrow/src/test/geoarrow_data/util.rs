use std::fs::File;
use std::sync::Arc;

use arrow_array::Array;
use arrow_ipc::reader::FileReader;

pub(super) fn read_geometry_column(path: &str) -> Arc<dyn Array> {
    let file = File::open(path).unwrap();
    let reader = FileReader::try_new(file, None).unwrap();

    let mut arrays = vec![];
    for maybe_record_batch in reader {
        let record_batch = maybe_record_batch.unwrap();
        let geom_idx = record_batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == "geometry")
            .unwrap();
        let arr = record_batch.column(geom_idx).clone();
        arrays.push(arr);
    }

    assert_eq!(arrays.len(), 1);
    arrays.first().unwrap().clone()
}
