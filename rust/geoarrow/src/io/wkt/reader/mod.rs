use std::str::FromStr;
use std::sync::Arc;

use arrow_array::{Array, GenericStringArray, OffsetSizeTrait};

use crate::array::metadata::ArrayMetadata;
use crate::array::{CoordType, MixedGeometryBuilder};
use crate::NativeArray;

// mod wkt_trait;

// TODO: refactor this trait implementation once a WKTArray exists
pub trait ParseWKT {
    type Output;

    fn parse_wkt(&self, coord_type: CoordType, metadata: Arc<ArrayMetadata>) -> Self::Output;
}

impl<O: OffsetSizeTrait> ParseWKT for GenericStringArray<O> {
    type Output = Arc<dyn NativeArray>;

    fn parse_wkt(&self, coord_type: CoordType, metadata: Arc<ArrayMetadata>) -> Self::Output {
        // TODO: switch this prefer_multi to true when we use downcasting here.
        let mut builder = MixedGeometryBuilder::<2>::new_with_options(coord_type, metadata, false);
        for i in 0..self.len() {
            if self.is_valid(i) {
                let w = wkt::Wkt::<f64>::from_str(self.value(i)).unwrap();
                builder.push_geometry(Some(&w)).unwrap();
            } else {
                builder.push_null();
            }
        }
        Arc::new(builder.finish())
    }
}
