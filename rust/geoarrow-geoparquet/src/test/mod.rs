mod geoarrow_data;

use std::path::PathBuf;

pub(crate) fn fixture_dir() -> PathBuf {
    let p = PathBuf::from("../../fixtures");
    assert!(p.exists());
    p
}
