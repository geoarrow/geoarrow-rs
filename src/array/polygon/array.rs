use crate::array::{CoordBuffer, CoordType, MultiLineStringArray, WKBArray};
use crate::error::GeoArrowError;
use crate::scalar::Polygon;
use crate::util::{owned_slice_offsets, owned_slice_validity, slice_validity_unchecked};
use crate::GeometryArrayTrait;
use arrow2::array::Array;
use arrow2::array::ListArray;
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::{DataType, Field};
use arrow2::offset::OffsetsBuffer;
use arrow2::types::Offset;
use rstar::primitives::CachedEnvelope;
use rstar::RTree;

use super::MutablePolygonArray;

/// An immutable array of Polygon geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<Polygon>>` due to the internal validity bitmap.
#[derive(Debug, Clone, PartialEq)]
pub struct PolygonArray<O: Offset> {
    pub coords: CoordBuffer,

    /// Offsets into the ring array where each geometry starts
    pub geom_offsets: OffsetsBuffer<O>,

    /// Offsets into the coordinate array where each ring starts
    pub ring_offsets: OffsetsBuffer<O>,

    /// Validity bitmap
    pub validity: Option<Bitmap>,
}

pub(super) fn check<O: Offset>(
    coords: &CoordBuffer,
    geom_offsets: &OffsetsBuffer<O>,
    ring_offsets: &OffsetsBuffer<O>,
    validity_len: Option<usize>,
) -> Result<(), GeoArrowError> {
    if validity_len.map_or(false, |len| len != geom_offsets.len_proxy()) {
        return Err(GeoArrowError::General(
            "validity mask length must match the number of values".to_string(),
        ));
    }

    if ring_offsets.last().to_usize() != coords.len() {
        return Err(GeoArrowError::General(
            "largest ring offset must match coords length".to_string(),
        ));
    }

    if geom_offsets.last().to_usize() != ring_offsets.len_proxy() {
        return Err(GeoArrowError::General(
            "largest geometry offset must match ring offsets length".to_string(),
        ));
    }

    Ok(())
}

impl<O: Offset> PolygonArray<O> {
    /// Create a new PolygonArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Panics
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest ring offset does not match the number of coordinates
    /// - if the largest geometry offset does not match the size of ring offsets
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: OffsetsBuffer<O>,
        ring_offsets: OffsetsBuffer<O>,
        validity: Option<Bitmap>,
    ) -> Self {
        check(
            &coords,
            &geom_offsets,
            &ring_offsets,
            validity.as_ref().map(|v| v.len()),
        )
        .unwrap();
        Self {
            coords,
            geom_offsets,
            ring_offsets,
            validity,
        }
    }

    /// Create a new PolygonArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest ring offset does not match the number of coordinates
    /// - if the largest geometry offset does not match the size of ring offsets
    pub fn try_new(
        coords: CoordBuffer,
        geom_offsets: OffsetsBuffer<O>,
        ring_offsets: OffsetsBuffer<O>,
        validity: Option<Bitmap>,
    ) -> Result<Self, GeoArrowError> {
        check(
            &coords,
            &geom_offsets,
            &ring_offsets,
            validity.as_ref().map(|v| v.len()),
        )?;
        Ok(Self {
            coords,
            geom_offsets,
            ring_offsets,
            validity,
        })
    }

    fn vertices_type(&self) -> DataType {
        self.coords.logical_type()
    }

    fn rings_type(&self) -> DataType {
        let vertices_field = Field::new("vertices", self.vertices_type(), false);
        match O::IS_LARGE {
            true => DataType::LargeList(Box::new(vertices_field)),
            false => DataType::List(Box::new(vertices_field)),
        }
    }

    fn outer_type(&self) -> DataType {
        let rings_field = Field::new("rings", self.rings_type(), true);
        match O::IS_LARGE {
            true => DataType::LargeList(Box::new(rings_field)),
            false => DataType::List(Box::new(rings_field)),
        }
    }
}

impl<'a, O: Offset> GeometryArrayTrait<'a> for PolygonArray<O> {
    type Scalar = Polygon<'a, O>;
    type ScalarGeo = geo::Polygon;
    type ArrowArray = ListArray<O>;
    type RTreeObject = CachedEnvelope<Self::Scalar>;

    fn value(&'a self, i: usize) -> Self::Scalar {
        Polygon::new_borrowed(&self.coords, &self.geom_offsets, &self.ring_offsets, i)
    }

    fn logical_type(&self) -> DataType {
        self.outer_type()
    }

    fn extension_type(&self) -> DataType {
        DataType::Extension(
            "geoarrow.polygon".to_string(),
            Box::new(self.logical_type()),
            None,
        )
    }

    fn into_arrow(self) -> Self::ArrowArray {
        let rings_type = self.rings_type();
        let extension_type = self.extension_type();
        let validity = self.validity;
        let coord_array = self.coords.into_arrow();
        let ring_array = ListArray::new(rings_type, self.ring_offsets, coord_array, None).boxed();
        ListArray::new(extension_type, self.geom_offsets, ring_array, validity)
    }

    fn into_boxed_arrow(self) -> Box<dyn Array> {
        self.into_arrow().boxed()
    }

    fn with_coords(self, coords: CoordBuffer) -> Self {
        assert_eq!(coords.len(), self.coords.len());
        Self::new(coords, self.geom_offsets, self.ring_offsets, self.validity)
    }

    fn coord_type(&self) -> CoordType {
        self.coords.coord_type()
    }

    fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self::new(
            self.coords.into_coord_type(coord_type),
            self.geom_offsets,
            self.ring_offsets,
            self.validity,
        )
    }

    /// Build a spatial index containing this array's geometries
    fn rstar_tree(&'a self) -> RTree<Self::RTreeObject> {
        RTree::bulk_load(self.iter().flatten().map(CachedEnvelope::new).collect())
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.geom_offsets.len_proxy()
    }

    /// Returns the optional validity.
    #[inline]
    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Slices this [`PolygonArray`] in place.
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

    /// Slices this [`PolygonArray`] in place.
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    #[inline]
    unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        slice_validity_unchecked(&mut self.validity, offset, length);
        self.geom_offsets.slice_unchecked(offset, length + 1);
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        assert!(length >= 1, "length must be at least 1");

        // Find the start and end of the ring offsets
        let (start_ring_idx, _) = self.geom_offsets.start_end(offset);
        let (_, end_ring_idx) = self.geom_offsets.start_end(offset + length - 1);

        // Find the start and end of the coord buffer
        let (start_coord_idx, _) = self.ring_offsets.start_end(start_ring_idx);
        let (_, end_coord_idx) = self.ring_offsets.start_end(end_ring_idx - 1);

        // Slice the geom_offsets
        let geom_offsets = owned_slice_offsets(&self.geom_offsets, offset, length);
        let ring_offsets = owned_slice_offsets(
            &self.ring_offsets,
            start_ring_idx,
            end_ring_idx - start_ring_idx,
        );
        let coords = self
            .coords
            .owned_slice(start_coord_idx, end_coord_idx - start_coord_idx);

        let validity = owned_slice_validity(self.validity(), offset, length);

        Self::new(coords, geom_offsets, ring_offsets, validity)
    }

    fn to_boxed(&self) -> Box<Self> {
        Box::new(self.clone())
    }
}

// Implement geometry accessors
impl<O: Offset> PolygonArray<O> {
    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = geo::Polygon> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(
        &self,
    ) -> ZipValidity<geo::Polygon, impl Iterator<Item = geo::Polygon> + '_, BitmapIter> {
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

impl<O: Offset> TryFrom<&ListArray<O>> for PolygonArray<O> {
    type Error = GeoArrowError;

    fn try_from(geom_array: &ListArray<O>) -> Result<Self, Self::Error> {
        let geom_offsets = geom_array.offsets();
        let validity = geom_array.validity();

        let rings_dyn_array = geom_array.values();
        let rings_array = rings_dyn_array
            .as_any()
            .downcast_ref::<ListArray<O>>()
            .unwrap();

        let ring_offsets = rings_array.offsets();
        let coords: CoordBuffer = rings_array.values().as_ref().try_into()?;

        Ok(Self::new(
            coords,
            geom_offsets.clone(),
            ring_offsets.clone(),
            validity.cloned(),
        ))
    }
}

impl TryFrom<&dyn Array> for PolygonArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type().to_logical_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray<i32>>().unwrap();
                downcasted.try_into()
            }
            DataType::LargeList(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray<i64>>().unwrap();
                let geom_array: PolygonArray<i64> = downcasted.try_into()?;
                geom_array.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<&dyn Array> for PolygonArray<i64> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type().to_logical_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray<i32>>().unwrap();
                let geom_array: PolygonArray<i32> = downcasted.try_into()?;
                Ok(geom_array.into())
            }
            DataType::LargeList(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray<i64>>().unwrap();
                downcasted.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}
impl<O: Offset> From<Vec<Option<geo::Polygon>>> for PolygonArray<O> {
    fn from(other: Vec<Option<geo::Polygon>>) -> Self {
        let mut_arr: MutablePolygonArray<O> = other.into();
        mut_arr.into()
    }
}

impl<O: Offset> From<Vec<geo::Polygon>> for PolygonArray<O> {
    fn from(other: Vec<geo::Polygon>) -> Self {
        let mut_arr: MutablePolygonArray<O> = other.into();
        mut_arr.into()
    }
}

impl<O: Offset> From<bumpalo::collections::Vec<'_, geo::Polygon>> for PolygonArray<O> {
    fn from(value: bumpalo::collections::Vec<geo::Polygon>) -> Self {
        let mut_arr: MutablePolygonArray<O> = value.into();
        mut_arr.into()
    }
}

impl<O: Offset> From<bumpalo::collections::Vec<'_, Option<geo::Polygon>>> for PolygonArray<O> {
    fn from(value: bumpalo::collections::Vec<Option<geo::Polygon>>) -> Self {
        let mut_arr: MutablePolygonArray<O> = value.into();
        mut_arr.into()
    }
}

impl<O: Offset> TryFrom<WKBArray<O>> for PolygonArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self, Self::Error> {
        let mut_arr: MutablePolygonArray<O> = value.try_into()?;
        Ok(mut_arr.into())
    }
}

/// Polygon and MultiLineString have the same layout, so enable conversions between the two to
/// change the semantic type
impl<O: Offset> From<PolygonArray<O>> for MultiLineStringArray<O> {
    fn from(value: PolygonArray<O>) -> Self {
        Self::new(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            value.validity,
        )
    }
}

impl From<PolygonArray<i32>> for PolygonArray<i64> {
    fn from(value: PolygonArray<i32>) -> Self {
        Self::new(
            value.coords,
            (&value.geom_offsets).into(),
            (&value.ring_offsets).into(),
            value.validity,
        )
    }
}

impl TryFrom<PolygonArray<i64>> for PolygonArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: PolygonArray<i64>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            value.coords,
            (&value.geom_offsets).try_into()?,
            (&value.ring_offsets).try_into()?,
            value.validity,
        ))
    }
}

/// Default to an empty array
impl<O: Offset> Default for PolygonArray<O> {
    fn default() -> Self {
        MutablePolygonArray::default().into()
    }
}

#[cfg(test)]
mod test {
    use crate::test::geoarrow_data::{
        example_polygon_interleaved, example_polygon_separated, example_polygon_wkb,
    };
    use crate::test::polygon::{p0, p1};

    use super::*;

    #[test]
    fn geo_roundtrip_accurate() {
        let arr: PolygonArray<i64> = vec![p0(), p1()].into();
        assert_eq!(arr.value_as_geo(0), p0());
        assert_eq!(arr.value_as_geo(1), p1());
    }

    #[test]
    fn geo_roundtrip_accurate_option_vec() {
        let arr: PolygonArray<i64> = vec![Some(p0()), Some(p1()), None].into();
        assert_eq!(arr.get_as_geo(0), Some(p0()));
        assert_eq!(arr.get_as_geo(1), Some(p1()));
        assert_eq!(arr.get_as_geo(2), None);
    }

    #[test]
    fn slice() {
        let mut arr: PolygonArray<i64> = vec![p0(), p1()].into();
        arr.slice(1, 1);

        assert_eq!(arr.len(), 1);
        assert_eq!(arr.get_as_geo(0), Some(p1()));

        // Offset is 1 because it's sliced on another backing buffer
        assert_eq!(*arr.geom_offsets.first(), 1);
    }

    #[test]
    fn owned_slice() {
        let arr: PolygonArray<i64> = vec![p0(), p1()].into();
        let sliced = arr.owned_slice(1, 1);

        assert!(
            !sliced.geom_offsets.buffer().is_sliced(),
            "underlying offsets should not be sliced"
        );
        assert_eq!(arr.len(), 2);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(p1()));

        // Offset is 0 because it's copied to an owned buffer
        assert_eq!(*sliced.geom_offsets.first(), 0);
        assert_eq!(*sliced.ring_offsets.first(), 0);
    }

    #[test]
    fn parse_wkb_geoarrow_interleaved_example() {
        let geom_arr = example_polygon_interleaved();

        let wkb_arr = example_polygon_wkb();
        let parsed_geom_arr: PolygonArray<i64> = wkb_arr.try_into().unwrap();

        assert_eq!(geom_arr, parsed_geom_arr);
    }

    #[test]
    fn parse_wkb_geoarrow_separated_example() {
        // TODO: support checking equality of interleaved vs separated coords
        let geom_arr = example_polygon_separated().into_coord_type(CoordType::Interleaved);

        let wkb_arr = example_polygon_wkb();
        let parsed_geom_arr: PolygonArray<i64> = wkb_arr.try_into().unwrap();

        assert_eq!(geom_arr, parsed_geom_arr);
    }
}
