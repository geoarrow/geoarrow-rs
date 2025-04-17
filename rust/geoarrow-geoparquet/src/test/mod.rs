mod geoarrow_data;

use std::path::PathBuf;

pub(crate) fn fixture_dir() -> PathBuf {
    let p = PathBuf::from("../../fixtures");
    assert!(p.exists());
    p
}

pub(crate) fn geoarrow_data_example_files() -> PathBuf {
    fixture_dir().join("geoarrow-data/example/files")
}
