use std::any::Any;
use std::sync::Arc;
use arrow_array::{ArrayRef, OffsetSizeTrait};
use arrow_buffer::bit_iterator::BitIterator;
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use crate::{GeometryArrayTrait};
use crate::array::{CoordType, LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray, PolygonArray, RectArray};
use crate::array::zip_validity::ZipValidity;
use crate::datatypes::GeoDataType;
use crate::scalar::geometry_scalar_ref::GeometryScalarRef;
use crate::trait_::{GeoArrayAccessor, GeometryScalarTrait};

type GeometryArrayRef = Arc<dyn GeometryArrayTrait>;

impl GeometryArrayTrait for GeometryArrayRef {
    fn as_any(&self) -> &dyn Any {
        self.as_any()
    }

    fn data_type(&self) -> &GeoDataType {
        self.data_type()
    }

    fn storage_type(&self) -> DataType {
        self.storage_type()
    }

    fn extension_field(&self) -> Arc<Field> {
        self.extension_field()
    }

    fn extension_name(&self) -> &str {
        self.extension_name()
    }

    fn into_array_ref(self) -> ArrayRef {
        self.into_array_ref()
    }

    fn coord_type(&self) -> CoordType {
        self.coord_type()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn validity(&self) -> Option<&NullBuffer> {
        self.validity()
    }
}

impl<'a> GeoArrayAccessor<'a> for GeometryArrayRef {
    type Item = GeometryScalarRef<'a>;
    type ItemGeo = geo::Geometry;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        todo!()
    }
}

impl dyn GeometryArrayTrait {
    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = geo::Geometry> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(
        &self,
    ) -> ZipValidity<geo::Geometry, impl Iterator<Item = geo::Geometry> + '_, BitIterator> {
        ZipValidity::new_with_validity(self.iter_geo_values(), self.nulls())
    }

    /// Returns the value at slot `i` as a GEOS geometry.
    #[cfg(feature = "geos")]
    pub fn value_as_geos(&self, i: usize) -> geos::Geometry {
        self.value(i).try_into().unwrap()
    }

    /// Gets the value at slot `i` as a GEOS geometry, additionally checking the validity bitmap
    #[cfg(feature = "geos")]
    pub fn get_as_geos(&self, i: usize) -> Option<geos::Geometry> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value_as_geos(i))
    }

    /// Iterator over GEOS geometry objects
    #[cfg(feature = "geos")]
    pub fn iter_geos_values(&self) -> impl Iterator<Item = geos::Geometry> + '_ {
        (0..self.len()).map(|i| self.value_as_geos(i))
    }

    /// Iterator over GEOS geometry objects, taking validity into account
    #[cfg(feature = "geos")]
    pub fn iter_geos(
        &self,
    ) -> ZipValidity<geos::Geometry, impl Iterator<Item = geos::Geometry> + '_, BitIterator> {
        ZipValidity::new_with_validity(self.iter_geos_values(), self.nulls())
    }
}


#[inline]
pub fn as_point_array(arr: &dyn GeometryArrayTrait) -> &PointArray {
    arr.as_any()
        .downcast_ref::<PointArray>()
        .expect("Unable to downcast to point array")
}

#[inline]
pub fn as_line_string_array<O: OffsetSizeTrait>(arr: &dyn GeometryArrayTrait) -> &LineStringArray<O> {
    arr.as_any()
        .downcast_ref::<LineStringArray<O>>()
        .expect("Unable to downcast to line string array")
}

#[inline]
pub fn as_polygon_array<O: OffsetSizeTrait>(arr: &dyn GeometryArrayTrait) -> &PolygonArray<O> {
    arr.as_any()
        .downcast_ref::<PolygonArray<O>>()
        .expect("Unable to downcast to polygon array")
}

#[inline]
pub fn as_multi_point_array<O: OffsetSizeTrait>(arr: &dyn GeometryArrayTrait) -> &MultiPointArray<O> {
    arr.as_any()
        .downcast_ref::<MultiPointArray<O>>()
        .expect("Unable to downcast to multi point array")
}

#[inline]
pub fn as_multi_line_string_array<O: OffsetSizeTrait>(arr: &dyn GeometryArrayTrait) -> &MultiLineStringArray<O> {
    arr.as_any()
        .downcast_ref::<MultiLineStringArray<O>>()
        .expect("Unable to downcast to multi line string array")
}

#[inline]
pub fn as_multi_polygon_array<O: OffsetSizeTrait>(arr: &dyn GeometryArrayTrait) -> &MultiPolygonArray<O> {
    arr.as_any()
        .downcast_ref::<MultiPolygonArray<O>>()
        .expect("Unable to downcast to multi polygon array")
}

#[inline]
pub fn as_rect_array(arr: &dyn GeometryArrayTrait) -> &RectArray {
    arr.as_any()
        .downcast_ref::<RectArray>()
        .expect("Unable to downcast to rect array")
}