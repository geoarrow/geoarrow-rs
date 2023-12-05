use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, FixedSizeListArray, Float64Array};
use arrow_buffer::bit_iterator::BitIterator;
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};

use crate::array::rect::RectBuilder;
use crate::array::zip_validity::ZipValidity;
use crate::array::{CoordBuffer, CoordType};
use crate::datatypes::GeoDataType;
use crate::geo_traits::RectTrait;
use crate::scalar::Rect;
use crate::trait_::{GeometryArrayAccessor, GeometryArraySelfMethods, IntoArrow};
use crate::util::owned_slice_validity;
use crate::GeometryArrayTrait;

/// An immutable array of Rect geometries.
///
/// This is **not** an array type defined by the GeoArrow specification (as of spec version 0.1)
/// but is included here for parity with georust/geo, and to save memory for the output of
/// `bounds()`.
///
/// Internally this is implemented as a FixedSizeList[4], laid out as minx, miny, maxx, maxy.
#[derive(Debug, Clone, PartialEq)]
pub struct RectArray {
    // Always GeoDataType::Rect
    data_type: GeoDataType,

    /// A Buffer of float values for the bounding rectangles
    /// Invariant: the length of values must always be a multiple of 4
    values: ScalarBuffer<f64>,
    validity: Option<NullBuffer>,
}

impl RectArray {
    pub fn new(values: ScalarBuffer<f64>, validity: Option<NullBuffer>) -> Self {
        Self {
            data_type: GeoDataType::Rect,
            values,
            validity,
        }
    }

    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = geo::Rect> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(
        &self,
    ) -> ZipValidity<geo::Rect, impl Iterator<Item = geo::Rect> + '_, BitIterator> {
        ZipValidity::new_with_validity(self.iter_geo_values(), self.nulls())
    }

    fn inner_field(&self) -> Arc<Field> {
        Field::new("rect", DataType::Float64, false).into()
    }

    fn outer_type(&self) -> DataType {
        DataType::FixedSizeList(self.inner_field(), 4)
    }
}

impl GeometryArrayTrait for RectArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> &GeoDataType {
        &self.data_type
    }

    fn storage_type(&self) -> DataType {
        self.outer_type()
    }

    fn extension_field(&self) -> Arc<Field> {
        let mut metadata = HashMap::new();
        metadata.insert(
            "ARROW:extension:name".to_string(),
            self.extension_name().to_string(),
        );
        Arc::new(Field::new("geometry", self.storage_type(), true).with_metadata(metadata))
    }

    fn extension_name(&self) -> &str {
        "geoarrow._rect"
    }

    fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    fn coord_type(&self) -> CoordType {
        unimplemented!()
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.values.len() / 4
    }

    /// Returns the optional validity.
    #[inline]
    fn validity(&self) -> Option<&NullBuffer> {
        self.validity.as_ref()
    }
}

impl GeometryArraySelfMethods for RectArray {
    fn with_coords(self, _coords: CoordBuffer) -> Self {
        unimplemented!()
    }

    fn into_coord_type(self, _coord_type: CoordType) -> Self {
        unimplemented!()
    }

    /// Slices this [`RectArray`] in place.
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        Self {
            data_type: self.data_type.clone(),
            values: self.values.slice(offset * 4, length * 4),
            validity: self.validity.as_ref().map(|v| v.slice(offset, length)),
        }
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Self {
        let values = self.values.slice(offset * 4, length * 4);

        let validity = owned_slice_validity(self.nulls(), offset, length);

        Self::new(values.to_vec().into(), validity)
    }
}

impl<'a> GeometryArrayAccessor<'a> for RectArray {
    type Item = Rect<'a>;
    type ItemGeo = geo::Rect;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        Rect::new_borrowed(&self.values, index)
    }
}

impl IntoArrow for RectArray {
    type ArrowArray = FixedSizeListArray;

    fn into_arrow(self) -> Self::ArrowArray {
        let inner_field = self.inner_field();
        let validity = self.validity;
        let values = Float64Array::new(self.values, None);
        FixedSizeListArray::new(inner_field, 2, Arc::new(values), validity)
    }
}

impl<G: RectTrait<T = f64>> From<&[G]> for RectArray {
    fn from(other: &[G]) -> Self {
        let mut_arr: RectBuilder = other.into();
        mut_arr.into()
    }
}

impl<G: RectTrait<T = f64>> From<Vec<Option<G>>> for RectArray {
    fn from(other: Vec<Option<G>>) -> Self {
        let mut_arr: RectBuilder = other.into();
        mut_arr.into()
    }
}
