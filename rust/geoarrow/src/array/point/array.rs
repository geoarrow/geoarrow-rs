use std::sync::Arc;

use arrow_array::{Array, ArrayRef, FixedSizeListArray, OffsetSizeTrait, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::extension::ExtensionType;
use arrow_schema::{DataType, Field};
use geo_traits::PointTrait;
use geoarrow_schema::{Dimension, Metadata, PointType};

use crate::algorithm::native::downcast::can_downcast_multi;
use crate::algorithm::native::eq::point_eq;
use crate::array::{
    CoordBuffer, CoordType, GeometryCollectionArray, InterleavedCoordBuffer, MixedGeometryArray,
    MultiPointArray, PointBuilder, SeparatedCoordBuffer, WKBArray,
};
use crate::datatypes::NativeType;
use crate::error::{GeoArrowError, Result};
use crate::scalar::{Geometry, Point};
use crate::trait_::{ArrayAccessor, GeometryArraySelfMethods, IntoArrow, NativeGeometryAccessor};
use crate::{ArrayBase, NativeArray};

/// An immutable array of Point geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<Point>>` due to the internal validity bitmap.
#[derive(Debug, Clone)]
pub struct PointArray {
    data_type: PointType,
    pub(crate) coords: CoordBuffer,
    pub(crate) validity: Option<NullBuffer>,
}

/// Perform checks:
///
/// - Validity mask must have the same length as the coordinates.
pub(super) fn check(coords: &CoordBuffer, validity_len: Option<usize>) -> Result<()> {
    if validity_len.is_some_and(|len| len != coords.len()) {
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
    pub fn new(coords: CoordBuffer, validity: Option<NullBuffer>, metadata: Arc<Metadata>) -> Self {
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
        metadata: Arc<Metadata>,
    ) -> Result<Self> {
        check(&coords, validity.as_ref().map(|v| v.len()))?;
        Ok(Self {
            data_type: PointType::new(coords.coord_type(), coords.dim(), metadata),
            coords,
            validity,
        })
    }

    /// Access the underlying coordinate buffer
    ///
    /// Note that some coordinates may be null, depending on the value of [`Self::nulls`]
    pub fn coords(&self) -> &CoordBuffer {
        &self.coords
    }

    /// Access the
    pub fn into_inner(self) -> (CoordBuffer, Option<NullBuffer>) {
        (self.coords, self.validity)
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> usize {
        self.len()
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self.nulls().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths() * self.dimension().size() * 8
    }

    /// Slices this [`PointArray`] in place.
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        Self {
            data_type: self.data_type.clone(),
            coords: self.coords.slice(offset, length),
            validity: self.validity.as_ref().map(|v| v.slice(offset, length)),
        }
    }

    /// Change the coordinate type of this array.
    pub fn to_coord_type(&self, coord_type: CoordType) -> Self {
        self.clone().into_coord_type(coord_type)
    }

    /// Change the coordinate type of this array.
    pub fn into_coord_type(self, coord_type: CoordType) -> Self {
        let metadata = self.metadata();
        Self::new(
            self.coords.into_coord_type(coord_type),
            self.validity,
            metadata,
        )
    }
}

impl ArrayBase for PointArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn storage_type(&self) -> DataType {
        self.data_type.data_type()
    }

    fn extension_field(&self) -> Arc<Field> {
        self.data_type.to_field("geometry", true).into()
    }

    fn extension_name(&self) -> &str {
        PointType::NAME
    }

    fn into_array_ref(self) -> ArrayRef {
        self.into_arrow()
    }

    fn to_array_ref(&self) -> ArrayRef {
        self.clone().into_array_ref()
    }

    fn metadata(&self) -> Arc<Metadata> {
        self.data_type.metadata().clone()
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.coords.len()
    }

    /// Returns the optional validity.
    #[inline]
    fn nulls(&self) -> Option<&NullBuffer> {
        self.validity.as_ref()
    }
}

impl NativeArray for PointArray {
    fn data_type(&self) -> NativeType {
        NativeType::Point(self.data_type.clone())
    }

    fn coord_type(&self) -> CoordType {
        self.coords.coord_type()
    }

    fn to_coord_type(&self, coord_type: CoordType) -> Arc<dyn NativeArray> {
        Arc::new(self.to_coord_type(coord_type))
    }

    fn with_metadata(&self, metadata: Arc<Metadata>) -> crate::trait_::NativeArrayRef {
        let mut arr = self.clone();
        arr.metadata = metadata;
        Arc::new(arr)
    }

    fn as_ref(&self) -> &dyn NativeArray {
        self
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn NativeArray> {
        Arc::new(self.slice(offset, length))
    }
}

impl GeometryArraySelfMethods for PointArray {
    fn with_coords(self, coords: CoordBuffer) -> Self {
        assert_eq!(coords.len(), self.coords.len());
        Self::new(coords, self.validity, self.metadata)
    }

    fn into_coord_type(self, coord_type: CoordType) -> Self {
        self.into_coord_type(coord_type)
    }
}

impl NativeGeometryAccessor for PointArray {
    unsafe fn value_as_geometry_unchecked(&self, index: usize) -> crate::scalar::Geometry {
        Geometry::Point(Point::new(&self.coords, index))
    }
}

#[cfg(feature = "geos")]
impl<'a> crate::trait_::NativeGEOSGeometryAccessor<'a> for PointArray {
    unsafe fn value_as_geometry_unchecked(
        &'a self,
        index: usize,
    ) -> std::result::Result<geos::Geometry, geos::Error> {
        let geom = Point::new(&self.coords, index);
        (&geom).try_into()
    }
}

impl<'a> ArrayAccessor<'a> for PointArray {
    type Item = Point<'a>;
    type ItemGeo = geo::Point;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        Point::new(&self.coords, index)
    }
}

impl IntoArrow for PointArray {
    type ArrowArray = ArrayRef;

    fn into_arrow(self) -> Self::ArrowArray {
        let validity = self.validity;
        let dim = self.coords.dim();
        match self.coords {
            CoordBuffer::Interleaved(c) => Arc::new(FixedSizeListArray::new(
                c.values_field().into(),
                dim.size() as i32,
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

impl TryFrom<(&FixedSizeListArray, Dimension)> for PointArray {
    type Error = GeoArrowError;

    fn try_from((value, dim): (&FixedSizeListArray, Dimension)) -> Result<Self> {
        let interleaved_coords = InterleavedCoordBuffer::from_arrow(value, dim)?;

        Ok(Self::new(
            CoordBuffer::Interleaved(interleaved_coords),
            value.nulls().cloned(),
            Default::default(),
        ))
    }
}

impl TryFrom<(&StructArray, Dimension)> for PointArray {
    type Error = GeoArrowError;

    fn try_from((value, dim): (&StructArray, Dimension)) -> Result<Self> {
        let validity = value.nulls();
        let separated_coords = SeparatedCoordBuffer::from_arrow(value, dim)?;
        Ok(Self::new(
            CoordBuffer::Separated(separated_coords),
            validity.cloned(),
            Default::default(),
        ))
    }
}

impl TryFrom<(&dyn Array, Dimension)> for PointArray {
    type Error = GeoArrowError;

    fn try_from((value, dim): (&dyn Array, Dimension)) -> Result<Self> {
        match value.data_type() {
            DataType::FixedSizeList(_, _) => {
                let arr = value.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                (arr, dim).try_into()
            }
            DataType::Struct(_) => {
                let arr = value.as_any().downcast_ref::<StructArray>().unwrap();
                (arr, dim).try_into()
            }
            _ => Err(GeoArrowError::General(
                "Invalid data type for PointArray".to_string(),
            )),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for PointArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let geom_type = NativeType::try_from(field)?;
        let dim = geom_type
            .dimension()
            .ok_or(GeoArrowError::General("Expected dimension".to_string()))?;
        let mut arr: Self = (arr, dim).try_into()?;
        arr.metadata = Arc::new(ArrayMetadata::try_from(field)?);
        Ok(arr)
    }
}

impl<G: PointTrait<T = f64>> From<(Vec<Option<G>>, Dimension)> for PointArray {
    fn from(other: (Vec<Option<G>>, Dimension)) -> Self {
        let mut_arr: PointBuilder = other.into();
        mut_arr.into()
    }
}

impl<G: PointTrait<T = f64>> From<(&[G], Dimension)> for PointArray {
    fn from(other: (&[G], Dimension)) -> Self {
        let mut_arr: PointBuilder = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WKBArray<O>, Dimension)> for PointArray {
    type Error = GeoArrowError;

    fn try_from(value: (WKBArray<O>, Dimension)) -> Result<Self> {
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

        // TODO: this should check for point equal.
        for point_idx in 0..self.len() {
            let c1 = self.value(point_idx);
            let c2 = other.value(point_idx);
            if !point_eq(&c1, &c2) {
                return false;
            }
        }

        true
    }
}

impl TryFrom<MultiPointArray> for PointArray {
    type Error = GeoArrowError;

    fn try_from(value: MultiPointArray) -> Result<Self> {
        if !can_downcast_multi(&value.geom_offsets) {
            return Err(GeoArrowError::General("Unable to cast".to_string()));
        }

        Ok(PointArray::new(
            value.coords,
            value.validity,
            value.metadata,
        ))
    }
}

impl TryFrom<MixedGeometryArray> for PointArray {
    type Error = GeoArrowError;

    fn try_from(value: MixedGeometryArray) -> Result<Self> {
        if value.has_line_strings()
            || value.has_polygons()
            || value.has_multi_line_strings()
            || value.has_multi_polygons()
        {
            return Err(GeoArrowError::General("Unable to cast".to_string()));
        }

        let (offset, length) = value.slice_offset_length();
        if value.has_only_points() {
            return Ok(value.points.slice(offset, length));
        }

        if value.has_only_multi_points() {
            return value.multi_points.slice(offset, length).try_into();
        }

        let mut builder = PointBuilder::with_capacity_and_options(
            value.dimension(),
            value.len(),
            value.coord_type(),
            value.metadata(),
        );
        value
            .iter()
            .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
        Ok(builder.finish())
    }
}

impl TryFrom<GeometryCollectionArray> for PointArray {
    type Error = GeoArrowError;

    fn try_from(value: GeometryCollectionArray) -> Result<Self> {
        MixedGeometryArray::try_from(value)?.try_into()
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
        let arr: PointArray = (vec![p0(), p1(), p2()].as_slice(), Dimension::XY).into();
        assert_eq!(arr.value_as_geo(0), p0());
        assert_eq!(arr.value_as_geo(1), p1());
        assert_eq!(arr.value_as_geo(2), p2());
    }

    #[test]
    fn geo_roundtrip_accurate_option_vec() {
        let arr: PointArray = (
            vec![Some(p0()), Some(p1()), Some(p2()), None],
            Dimension::XY,
        )
            .into();
        assert_eq!(arr.get_as_geo(0), Some(p0()));
        assert_eq!(arr.get_as_geo(1), Some(p1()));
        assert_eq!(arr.get_as_geo(2), Some(p2()));
        assert_eq!(arr.get_as_geo(3), None);
    }

    #[test]
    fn slice() {
        let points: Vec<Point> = vec![p0(), p1(), p2()];
        let point_array: PointArray = (points.as_slice(), Dimension::XY).into();
        let sliced = point_array.slice(1, 1);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(p1()));
    }

    #[ignore = "point file is invalid (https://github.com/geoarrow/geoarrow-data/issues/2)"]
    #[test]
    fn parse_wkb_geoarrow_interleaved_example() {
        let geom_arr = example_point_interleaved();

        let wkb_arr = example_point_wkb();
        let parsed_geom_arr: PointArray = (wkb_arr, Dimension::XY).try_into().unwrap();

        // Comparisons on the point array directly currently fail because of NaN values in
        // coordinate 1.
        assert_eq!(geom_arr.get_as_geo(0), parsed_geom_arr.get_as_geo(0));
        assert_eq!(geom_arr.get_as_geo(2), parsed_geom_arr.get_as_geo(2));
    }

    #[test]
    fn parse_wkb_geoarrow_separated_example() {
        let geom_arr = example_point_separated();

        let wkb_arr = example_point_wkb();
        let parsed_geom_arr: PointArray = (wkb_arr, Dimension::XY).try_into().unwrap();

        assert_eq!(geom_arr, parsed_geom_arr);
    }
}
