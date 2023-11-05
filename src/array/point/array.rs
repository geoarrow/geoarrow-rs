use std::collections::HashMap;
use std::sync::Arc;

use crate::algorithm::native::eq::coord_eq_allow_nan;
use crate::array::zip_validity::ZipValidity;
use crate::array::{
    CoordBuffer, CoordType, InterleavedCoordBuffer, MutablePointArray, SeparatedCoordBuffer,
    WKBArray,
};
use crate::error::GeoArrowError;
use crate::scalar::Point;
use crate::util::owned_slice_validity;
use crate::GeometryArrayTrait;
use arrow_array::{Array, ArrayRef, FixedSizeListArray, OffsetSizeTrait, StructArray};
use arrow_buffer::bit_iterator::BitIterator;
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};

/// An immutable array of Point geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<Point>>` due to the internal validity bitmap.
#[derive(Debug, Clone)]
pub struct PointArray {
    pub coords: CoordBuffer,
    pub validity: Option<NullBuffer>,
}

pub(super) fn check(
    coords: &CoordBuffer,
    validity_len: Option<usize>,
) -> Result<(), GeoArrowError> {
    if validity_len.map_or(false, |len| len != coords.len()) {
        return Err(GeoArrowError::General(
            "validity mask length must match the number of values".to_string(),
        ));
    }

    Ok(())
}

impl PointArray {
    /// Create a new PointArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Panics
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    pub fn new(coords: CoordBuffer, validity: Option<NullBuffer>) -> Self {
        check(&coords, validity.as_ref().map(|v| v.len())).unwrap();
        Self { coords, validity }
    }

    /// Create a new PointArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    pub fn try_new(
        coords: CoordBuffer,
        validity: Option<NullBuffer>,
    ) -> Result<Self, GeoArrowError> {
        check(&coords, validity.as_ref().map(|v| v.len()))?;
        Ok(Self { coords, validity })
    }

    pub fn into_inner(self) -> (CoordBuffer, Option<NullBuffer>) {
        (self.coords, self.validity)
    }
}

impl<'a> GeometryArrayTrait<'a> for PointArray {
    type Scalar = Point<'a>;
    type ScalarGeo = geo::Point;
    type ArrowArray = Arc<dyn Array>;

    fn value(&'a self, i: usize) -> Option<Self::Scalar> {
        if i < self.len() {
            Some(Point::new_borrowed(&self.coords, i))
        } else {
            None
        }
    }

    fn storage_type(&self) -> DataType {
        self.coords.storage_type()
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
        "geoarrow.point"
    }

    fn into_arrow(self) -> Self::ArrowArray {
        let validity = self.validity;
        match self.coords {
            CoordBuffer::Interleaved(c) => Arc::new(FixedSizeListArray::new(
                c.values_field().into(),
                2,
                Arc::new(c.values_array()),
                validity,
            )),
            CoordBuffer::Separated(c) => {
                let fields = c.values_field();
                Arc::new(StructArray::new(fields.into(), c.values_array(), validity))
            }
        }
    }

    fn into_array_ref(self) -> ArrayRef {
        self.into_arrow()
    }

    fn with_coords(self, coords: CoordBuffer) -> Self {
        assert_eq!(coords.len(), self.coords.len());
        Self::new(coords, self.validity)
    }

    fn coord_type(&self) -> CoordType {
        self.coords.coord_type()
    }

    fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self::new(self.coords.into_coord_type(coord_type), self.validity)
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.coords.len()
    }

    /// Returns the optional validity.
    #[inline]
    fn validity(&self) -> Option<&NullBuffer> {
        self.validity.as_ref()
    }

    /// Slices this [`PointArray`] in place.
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        Self {
            coords: self.coords.slice(offset, length),
            validity: self.validity.as_ref().map(|v| v.slice(offset, length)),
        }
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        assert!(length >= 1, "length must be at least 1");

        let coords = self.coords.owned_slice(offset, length);

        let validity = owned_slice_validity(self.nulls(), offset, length);

        Self::new(coords, validity)
    }

    fn to_boxed(&self) -> Box<Self> {
        Box::new(self.clone())
    }
}

// Implement geometry accessors
impl PointArray {
    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = geo::Point> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(
        &self,
    ) -> ZipValidity<geo::Point, impl Iterator<Item = geo::Point> + '_, BitIterator> {
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

impl TryFrom<&FixedSizeListArray> for PointArray {
    type Error = GeoArrowError;

    fn try_from(value: &FixedSizeListArray) -> Result<Self, Self::Error> {
        let interleaved_coords: InterleavedCoordBuffer = value.try_into()?;

        Ok(Self::new(
            CoordBuffer::Interleaved(interleaved_coords),
            value.nulls().cloned(),
        ))
    }
}

impl TryFrom<&StructArray> for PointArray {
    type Error = GeoArrowError;

    fn try_from(value: &StructArray) -> Result<Self, Self::Error> {
        let validity = value.nulls();
        let separated_coords: SeparatedCoordBuffer = value.try_into()?;
        Ok(Self::new(
            CoordBuffer::Separated(separated_coords),
            validity.cloned(),
        ))
    }
}

impl TryFrom<&dyn Array> for PointArray {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type() {
            DataType::FixedSizeList(_, _) => {
                let arr = value.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                arr.try_into()
            }
            DataType::Struct(_) => {
                let arr = value.as_any().downcast_ref::<StructArray>().unwrap();
                arr.try_into()
            }
            _ => Err(GeoArrowError::General(
                "Invalid data type for PointArray".to_string(),
            )),
        }
    }
}

impl From<Vec<Option<geo::Point>>> for PointArray {
    fn from(other: Vec<Option<geo::Point>>) -> Self {
        let mut_arr: MutablePointArray = other.into();
        mut_arr.into()
    }
}

impl From<Vec<geo::Point>> for PointArray {
    fn from(other: Vec<geo::Point>) -> Self {
        let mut_arr: MutablePointArray = other.into();
        mut_arr.into()
    }
}

impl From<bumpalo::collections::Vec<'_, Option<geo::Point>>> for PointArray {
    fn from(other: bumpalo::collections::Vec<'_, Option<geo::Point>>) -> Self {
        let mut_arr: MutablePointArray = other.into();
        mut_arr.into()
    }
}

impl From<bumpalo::collections::Vec<'_, geo::Point>> for PointArray {
    fn from(other: bumpalo::collections::Vec<'_, geo::Point>) -> Self {
        let mut_arr: MutablePointArray = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for PointArray {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self, Self::Error> {
        let mut_arr: MutablePointArray = value.try_into()?;
        Ok(mut_arr.into())
    }
}

/// Default to an empty array
impl Default for PointArray {
    fn default() -> Self {
        MutablePointArray::default().into()
    }
}

// Implement a custom PartialEq for PointArray to allow Point(EMPTY) comparisons, which is stored
// as (NaN, NaN). By default, these resolve to false
impl PartialEq for PointArray {
    fn eq(&self, other: &Self) -> bool {
        if self.validity != other.validity {
            return false;
        }

        // If the coords are already true, don't check for NaNs
        // TODO: maybe only iterate once for perf?
        if self.coords == other.coords {
            return true;
        }

        if self.coords.len() != other.coords.len() {
            return false;
        }

        for coord_idx in 0..self.coords.len() {
            let c1 = self.coords.value_unchecked(coord_idx);
            let c2 = other.coords.value_unchecked(coord_idx);
            if !coord_eq_allow_nan(c1, c2) {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod test {
    use crate::test::geoarrow_data::{
        example_point_interleaved, example_point_separated, example_point_wkb,
    };
    use crate::test::point::{p0, p1, p2};

    use super::*;
    use geo::Point;

    #[test]
    fn geo_roundtrip_accurate() {
        let arr: PointArray = vec![p0(), p1(), p2()].into();
        assert_eq!(arr.value_as_geo(0), p0());
        assert_eq!(arr.value_as_geo(1), p1());
        assert_eq!(arr.value_as_geo(2), p2());
    }

    #[test]
    fn geo_roundtrip_accurate_option_vec() {
        let arr: PointArray = vec![Some(p0()), Some(p1()), Some(p2()), None].into();
        assert_eq!(arr.get_as_geo(0), Some(p0()));
        assert_eq!(arr.get_as_geo(1), Some(p1()));
        assert_eq!(arr.get_as_geo(2), Some(p2()));
        assert_eq!(arr.get_as_geo(3), None);
    }

    #[test]
    fn slice() {
        let points: Vec<Point> = vec![p0(), p1(), p2()];
        let point_array: PointArray = points.into();
        let sliced = point_array.slice(1, 1);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(p1()));
    }

    #[test]
    fn owned_slice() {
        let points: Vec<Point> = vec![p0(), p1(), p2()];
        let point_array: PointArray = points.into();
        let sliced = point_array.owned_slice(1, 1);

        assert_eq!(point_array.len(), 3);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(p1()));
    }

    #[ignore = "point file is invalid (https://github.com/geoarrow/geoarrow-data/issues/2)"]
    #[test]
    fn parse_wkb_geoarrow_interleaved_example() {
        let geom_arr = example_point_interleaved();

        let wkb_arr = example_point_wkb();
        let parsed_geom_arr: PointArray = wkb_arr.try_into().unwrap();

        // Comparisons on the point array directly currently fail because of NaN values in
        // coordinate 1.
        assert_eq!(geom_arr.get_as_geo(0), parsed_geom_arr.get_as_geo(0));
        assert_eq!(geom_arr.get_as_geo(2), parsed_geom_arr.get_as_geo(2));
    }

    #[test]
    fn parse_wkb_geoarrow_separated_example() {
        let geom_arr = example_point_separated();

        let wkb_arr = example_point_wkb();
        let parsed_geom_arr: PointArray = wkb_arr.try_into().unwrap();

        assert_eq!(geom_arr, parsed_geom_arr);
    }
}
