use crate::array::{
    CoordBuffer, CoordType, InterleavedCoordBuffer, MutablePointArray, SeparatedCoordBuffer,
    WKBArray,
};
use crate::error::GeoArrowError;
use crate::scalar::Point;
use crate::util::{owned_slice_validity, slice_validity_unchecked};
use crate::GeometryArrayTrait;
use arrow2::array::{Array, FixedSizeListArray, StructArray};
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::DataType;
use arrow2::types::Offset;
use rstar::RTree;

/// An immutable array of Point geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<Point>>` due to the internal validity bitmap.
#[derive(Debug, Clone, PartialEq)]
pub struct PointArray {
    pub coords: CoordBuffer,
    pub validity: Option<Bitmap>,
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
    pub fn new(coords: CoordBuffer, validity: Option<Bitmap>) -> Self {
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
    pub fn try_new(coords: CoordBuffer, validity: Option<Bitmap>) -> Result<Self, GeoArrowError> {
        check(&coords, validity.as_ref().map(|v| v.len()))?;
        Ok(Self { coords, validity })
    }
}

impl<'a> GeometryArrayTrait<'a> for PointArray {
    type Scalar = Point<'a>;
    type ScalarGeo = geo::Point;
    type ArrowArray = Box<dyn Array>;
    type RTreeObject = Self::Scalar;

    fn value(&'a self, i: usize) -> Self::Scalar {
        Point::new_borrowed(&self.coords, i)
    }

    fn logical_type(&self) -> DataType {
        self.coords.logical_type()
    }

    fn extension_type(&self) -> DataType {
        DataType::Extension(
            "geoarrow.point".to_string(),
            Box::new(self.logical_type()),
            None,
        )
    }

    fn into_arrow(self) -> Box<dyn Array> {
        let extension_type = self.extension_type();
        let validity = self.validity;
        match self.coords {
            CoordBuffer::Interleaved(c) => {
                FixedSizeListArray::new(extension_type, c.values_array().boxed(), validity).boxed()
            }
            CoordBuffer::Separated(c) => {
                StructArray::new(extension_type, c.values_array(), validity).boxed()
            }
        }
    }

    fn into_boxed_arrow(self) -> Box<dyn Array> {
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

    /// Build a spatial index containing this array's geometries
    fn rstar_tree(&'a self) -> RTree<Self::RTreeObject> {
        // Note: for points we don't memoize with CachedEnvelope
        RTree::bulk_load(self.iter().flatten().collect())
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.coords.len()
    }

    /// Returns the optional validity.
    #[inline]
    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Slices this [`PointArray`] in place.
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Examples
    /// ```
    /// use arrow2::array::PrimitiveArray;
    ///
    /// let array = PrimitiveArray::from_vec(vec![1, 2, 3]);
    /// assert_eq!(format!("{:?}", array), "Int32[1, 2, 3]");
    /// let sliced = array.slice(1, 1);
    /// assert_eq!(format!("{:?}", sliced), "Int32[2]");
    /// // note: `sliced` and `array` share the same memory region.
    /// ```
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    fn slice(&mut self, offset: usize, length: usize) {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Slices this [`PointArray`] in place.
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    #[inline]
    unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        slice_validity_unchecked(&mut self.validity, offset, length);
        self.coords.slice_unchecked(offset, length);
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        assert!(length >= 1, "length must be at least 1");

        let coords = self.coords.owned_slice(offset, length);

        let validity = owned_slice_validity(self.validity(), offset, length);

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
    ) -> ZipValidity<geo::Point, impl Iterator<Item = geo::Point> + '_, BitmapIter> {
        ZipValidity::new_with_validity(self.iter_geo_values(), self.validity())
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
    ) -> ZipValidity<geos::Geometry, impl Iterator<Item = geos::Geometry> + '_, BitmapIter> {
        ZipValidity::new_with_validity(self.iter_geos_values(), self.validity())
    }
}

impl TryFrom<&FixedSizeListArray> for PointArray {
    type Error = GeoArrowError;

    fn try_from(value: &FixedSizeListArray) -> Result<Self, Self::Error> {
        let interleaved_coords: InterleavedCoordBuffer = value.try_into()?;

        Ok(Self::new(
            CoordBuffer::Interleaved(interleaved_coords),
            value.validity().cloned(),
        ))
    }
}

impl TryFrom<&StructArray> for PointArray {
    type Error = GeoArrowError;

    fn try_from(value: &StructArray) -> Result<Self, Self::Error> {
        let validity = value.validity();
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
        match value.data_type().to_logical_type() {
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

impl<O: Offset> TryFrom<WKBArray<O>> for PointArray {
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
        let mut point_array: PointArray = points.into();
        point_array.slice(1, 1);
        assert_eq!(point_array.len(), 1);
        assert_eq!(point_array.get_as_geo(0), Some(p1()));
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

        // Comparisons on the point array directly currently fail because of NaN values in
        // coordinate 1.
        assert_eq!(geom_arr.get_as_geo(0), parsed_geom_arr.get_as_geo(0));
        assert_eq!(geom_arr.get_as_geo(2), parsed_geom_arr.get_as_geo(2));
    }
}
