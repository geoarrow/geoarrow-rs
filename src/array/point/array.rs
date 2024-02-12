use std::collections::HashMap;
use std::sync::Arc;

use crate::algorithm::native::eq::coord_eq_allow_nan;
use crate::array::metadata::ArrayMetadata;
use crate::array::{
    CoordBuffer, CoordType, InterleavedCoordBuffer, PointBuilder, SeparatedCoordBuffer, WKBArray,
};
use crate::datatypes::GeoDataType;
use crate::error::GeoArrowError;
use crate::geo_traits::PointTrait;
use crate::scalar::Point;
use crate::trait_::{GeometryArrayAccessor, GeometryArraySelfMethods, IntoArrow};
use crate::util::owned_slice_validity;
use crate::GeometryArrayTrait;
use arrow_array::{Array, ArrayRef, FixedSizeListArray, OffsetSizeTrait, StructArray};

use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};

/// An immutable array of Point geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<Point>>` due to the internal validity bitmap.
#[derive(Debug, Clone)]
pub struct PointArray {
    // Always GeoDataType::Point
    data_type: GeoDataType,
    pub(crate) metadata: Arc<ArrayMetadata>,
    pub(crate) coords: CoordBuffer,
    pub(crate) validity: Option<NullBuffer>,
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
    pub fn new(
        coords: CoordBuffer,
        validity: Option<NullBuffer>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        Self::try_new(coords, validity, metadata).unwrap()
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
        metadata: Arc<ArrayMetadata>,
    ) -> Result<Self, GeoArrowError> {
        check(&coords, validity.as_ref().map(|v| v.len()))?;
        let data_type = GeoDataType::Point(coords.coord_type());
        Ok(Self {
            data_type,
            coords,
            validity,
            metadata,
        })
    }

    pub fn coords(&self) -> &CoordBuffer {
        &self.coords
    }

    pub fn into_inner(self) -> (CoordBuffer, Option<NullBuffer>) {
        (self.coords, self.validity)
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> usize {
        self.len()
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self.validity().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths() * 2 * 8
    }
}

impl GeometryArrayTrait for PointArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> &GeoDataType {
        &self.data_type
    }

    fn storage_type(&self) -> DataType {
        self.coords.storage_type()
    }

    fn extension_field(&self) -> Arc<Field> {
        let mut metadata = HashMap::with_capacity(2);
        metadata.insert(
            "ARROW:extension:name".to_string(),
            self.extension_name().to_string(),
        );
        metadata.insert(
            "ARROW:extension:metadata".to_string(),
            serde_json::to_string(self.metadata.as_ref()).unwrap(),
        );
        Arc::new(Field::new("geometry", self.storage_type(), true).with_metadata(metadata))
    }

    fn extension_name(&self) -> &str {
        "geoarrow.point"
    }

    fn into_array_ref(self) -> ArrayRef {
        self.into_arrow()
    }

    fn to_array_ref(&self) -> arrow_array::ArrayRef {
        self.clone().into_array_ref()
    }

    fn coord_type(&self) -> CoordType {
        self.coords.coord_type()
    }

    fn metadata(&self) -> Arc<ArrayMetadata> {
        self.metadata.clone()
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

    fn as_ref(&self) -> &dyn GeometryArrayTrait {
        self
    }
}

impl GeometryArraySelfMethods for PointArray {
    fn with_coords(self, coords: CoordBuffer) -> Self {
        assert_eq!(coords.len(), self.coords.len());
        Self::new(coords, self.validity, self.metadata)
    }

    fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self::new(
            self.coords.into_coord_type(coord_type),
            self.validity,
            self.metadata,
        )
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
            data_type: self.data_type,
            coords: self.coords.slice(offset, length),
            validity: self.validity.as_ref().map(|v| v.slice(offset, length)),
            metadata: self.metadata(),
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

        Self::new(coords, validity, self.metadata())
    }
}

// Implement geometry accessors
impl<'a> GeometryArrayAccessor<'a> for PointArray {
    type Item = Point<'a>;
    type ItemGeo = geo::Point;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        Point::new_borrowed(&self.coords, index)
    }
}

impl IntoArrow for PointArray {
    type ArrowArray = Arc<dyn Array>;

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
}

impl TryFrom<&FixedSizeListArray> for PointArray {
    type Error = GeoArrowError;

    fn try_from(value: &FixedSizeListArray) -> Result<Self, Self::Error> {
        let interleaved_coords: InterleavedCoordBuffer = value.try_into()?;

        Ok(Self::new(
            CoordBuffer::Interleaved(interleaved_coords),
            value.nulls().cloned(),
            Default::default(),
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
            Default::default(),
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

impl<G: PointTrait<T = f64>> From<Vec<Option<G>>> for PointArray {
    fn from(other: Vec<Option<G>>) -> Self {
        let mut_arr: PointBuilder = other.into();
        mut_arr.into()
    }
}

impl<G: PointTrait<T = f64>> From<&[G]> for PointArray {
    fn from(other: &[G]) -> Self {
        let mut_arr: PointBuilder = other.into();
        mut_arr.into()
    }
}

impl<G: PointTrait<T = f64>> From<bumpalo::collections::Vec<'_, Option<G>>> for PointArray {
    fn from(other: bumpalo::collections::Vec<'_, Option<G>>) -> Self {
        let mut_arr: PointBuilder = other.into();
        mut_arr.into()
    }
}

impl<G: PointTrait<T = f64>> From<bumpalo::collections::Vec<'_, G>> for PointArray {
    fn from(other: bumpalo::collections::Vec<'_, G>) -> Self {
        let mut_arr: PointBuilder = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for PointArray {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self, Self::Error> {
        let mut_arr: PointBuilder = value.try_into()?;
        Ok(mut_arr.into())
    }
}

/// Default to an empty array
impl Default for PointArray {
    fn default() -> Self {
        PointBuilder::default().into()
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
            let c1 = self.coords.value(coord_idx);
            let c2 = other.coords.value(coord_idx);
            if !coord_eq_allow_nan(&c1, &c2) {
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
        let arr: PointArray = vec![p0(), p1(), p2()].as_slice().into();
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
        let point_array: PointArray = points.as_slice().into();
        let sliced = point_array.slice(1, 1);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(p1()));
    }

    #[test]
    fn owned_slice() {
        let points: Vec<Point> = vec![p0(), p1(), p2()];
        let point_array: PointArray = points.as_slice().into();
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
