use arrow2::array::{Array, PrimitiveArray, StructArray};
use arrow2::buffer::Buffer;
use arrow2::datatypes::{DataType, Field};
use rstar::RTree;

use crate::array::CoordType;
use crate::error::GeoArrowError;
use crate::scalar::SeparatedCoord;
use crate::GeometryArrayTrait;

#[derive(Debug, Clone, PartialEq)]
pub struct SeparatedCoordBuffer {
    pub x: Buffer<f64>,
    pub y: Buffer<f64>,
}

impl SeparatedCoordBuffer {
    pub fn new(x: Buffer<f64>, y: Buffer<f64>) -> Self {
        Self { x, y }
    }

    pub fn values_array(&self) -> Vec<Box<dyn Array>> {
        vec![
            PrimitiveArray::new(DataType::Float64, self.x.clone(), None).boxed(),
            PrimitiveArray::new(DataType::Float64, self.y.clone(), None).boxed(),
        ]
    }

    pub fn values_field(&self) -> Vec<Field> {
        vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]
    }
}

impl<'a> GeometryArrayTrait<'a> for SeparatedCoordBuffer {
    type ArrowArray = StructArray;
    type Scalar = SeparatedCoord<'a>;
    type ScalarGeo = geo::Coord;
    type RTreeObject = Self::Scalar;

    fn value(&'a self, i: usize) -> Self::Scalar {
        SeparatedCoord {
            x: &self.x,
            y: &self.y,
            i,
        }
    }

    fn logical_type(&self) -> DataType {
        DataType::Struct(self.values_field())
    }

    fn extension_type(&self) -> DataType {
        panic!("Coordinate arrays do not have an extension name.")
    }

    fn into_arrow(self) -> Self::ArrowArray {
        StructArray::new(self.logical_type(), self.values_array(), None)
    }

    fn into_boxed_arrow(self) -> Box<dyn Array> {
        self.into_arrow().boxed()
    }

    fn with_coords(self, _coords: crate::array::CoordBuffer) -> Self {
        unimplemented!();
    }

    fn coord_type(&self) -> CoordType {
        CoordType::Separated
    }

    fn into_coord_type(self, _coord_type: CoordType) -> Self {
        panic!("into_coord_type only implemented on CoordBuffer");
    }

    fn rstar_tree(&'a self) -> RTree<Self::Scalar> {
        panic!("not implemented for coords");
    }

    fn len(&self) -> usize {
        self.x.len()
    }

    fn validity(&self) -> Option<&arrow2::bitmap::Bitmap> {
        panic!("coordinate arrays don't have their own validity arrays")
    }

    fn slice(&mut self, offset: usize, length: usize) {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        unsafe { self.slice_unchecked(offset, length) };
    }

    unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        self.x.slice_unchecked(offset, length);
        self.y.slice_unchecked(offset, length);
    }

    fn to_boxed(&self) -> Box<Self> {
        Box::new(self.clone())
    }
}

impl From<SeparatedCoordBuffer> for StructArray {
    fn from(value: SeparatedCoordBuffer) -> Self {
        value.into_arrow()
    }
}

impl TryFrom<&StructArray> for SeparatedCoordBuffer {
    type Error = GeoArrowError;

    fn try_from(value: &StructArray) -> Result<Self, Self::Error> {
        let arrays = value.values();

        if !arrays.len() == 2 {
            return Err(GeoArrowError::General(
                "Expected two child arrays of this StructArray.".to_string(),
            ));
        }

        let x_array_values = arrays[0]
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();
        let y_array_values = arrays[1]
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();

        Ok(SeparatedCoordBuffer::new(
            x_array_values.values().clone(),
            y_array_values.values().clone(),
        ))
    }
}
