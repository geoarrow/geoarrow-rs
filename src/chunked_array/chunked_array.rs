use crate::GeometryArrayTrait;
use crate::array::PointArray;
use arrow2::datatypes::Field;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct ChunkedArray<'a, G: GeometryArrayTrait<'a>> {
    pub(crate) field: Arc<Field>,
    pub(crate) chunks: Vec<G>,
    length: usize,
    _phantom: &'a u8
}



// pub struct PointChunkedArray {
//     pub(crate) field: Arc<Field>,
//     pub(crate) chunks: Vec<PointArray>,
//     length: usize,
// }

// pub struct PointChunkedArray {
//     pub(crate) field: Arc<Field>,
//     pub(crate) chunks: Vec<PointArray>,
//     length: usize,
// }

// pub struct PointChunkedArray {
//     pub(crate) field: Arc<Field>,
//     pub(crate) chunks: Vec<PointArray>,
//     length: usize,
// }

// pub struct PointChunkedArray {
//     pub(crate) field: Arc<Field>,
//     pub(crate) chunks: Vec<PointArray>,
//     length: usize,
// }

// pub struct PointChunkedArray {
//     pub(crate) field: Arc<Field>,
//     pub(crate) chunks: Vec<PointArray>,
//     length: usize,
// }
