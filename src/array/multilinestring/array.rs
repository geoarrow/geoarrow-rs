use crate::array::{CoordBuffer, PolygonArray};
use crate::error::GeoArrowError;
use crate::GeometryArrayTrait;
use arrow2::array::{Array, ListArray};
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::{DataType, Field};
use arrow2::offset::OffsetsBuffer;

use super::MutableMultiLineStringArray;

/// A [`GeometryArrayTrait`] semantically equivalent to `Vec<Option<MultiLineString>>` using Arrow's
/// in-memory representation.
#[derive(Debug, Clone)]
pub struct MultiLineStringArray {
    pub coords: CoordBuffer,

    /// Offsets into the ring array where each geometry starts
    pub geom_offsets: OffsetsBuffer<i64>,

    /// Offsets into the coordinate array where each ring starts
    pub ring_offsets: OffsetsBuffer<i64>,

    /// Validity bitmap
    pub validity: Option<Bitmap>,
}

pub(super) fn _check(
    x: &[f64],
    y: &[f64],
    validity_len: Option<usize>,
    geom_offsets: &OffsetsBuffer<i64>,
) -> Result<(), GeoArrowError> {
    // TODO: check geom offsets and ring_offsets?
    if validity_len.map_or(false, |len| len != geom_offsets.len()) {
        return Err(GeoArrowError::General(
            "validity mask length must match the number of values".to_string(),
        ));
    }

    if x.len() != y.len() {
        return Err(GeoArrowError::General(
            "x and y arrays must have the same length".to_string(),
        ));
    }
    Ok(())
}

impl MultiLineStringArray {
    /// Create a new MultiLineStringArray from parts
    /// # Implementation
    /// This function is `O(1)`.
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: OffsetsBuffer<i64>,
        ring_offsets: OffsetsBuffer<i64>,
        validity: Option<Bitmap>,
    ) -> Self {
        // check(&x, &y, validity.as_ref().map(|v| v.len()), &geom_offsets).unwrap();
        Self {
            coords,
            geom_offsets,
            ring_offsets,
            validity,
        }
    }

    /// Create a new MultiLineStringArray from parts
    /// # Implementation
    /// This function is `O(1)`.
    pub fn try_new(
        coords: CoordBuffer,
        geom_offsets: OffsetsBuffer<i64>,
        ring_offsets: OffsetsBuffer<i64>,
        validity: Option<Bitmap>,
    ) -> Result<Self, GeoArrowError> {
        // check(&x, &y, validity.as_ref().map(|v| v.len()), &geom_offsets)?;
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

    fn linestrings_type(&self) -> DataType {
        let vertices_field = Field::new("vertices", self.vertices_type(), false);
        DataType::LargeList(Box::new(vertices_field))
    }

    fn outer_type(&self) -> DataType {
        let linestrings_field = Field::new("linestrings", self.linestrings_type(), true);
        DataType::LargeList(Box::new(linestrings_field))
    }
}

impl<'a> GeometryArrayTrait<'a> for MultiLineStringArray {
    type Scalar = crate::scalar::MultiLineString<'a>;
    type ScalarGeo = geo::MultiLineString;
    type ArrowArray = ListArray<i64>;

    fn value(&'a self, i: usize) -> Self::Scalar {
        crate::scalar::MultiLineString {
            coords: &self.coords,
            geom_offsets: &self.geom_offsets,
            ring_offsets: &self.ring_offsets,
            geom_index: i,
        }
    }

    fn logical_type(&self) -> DataType {
        self.outer_type()
    }

    fn extension_type(&self) -> DataType {
        DataType::Extension(
            "geoarrow.multilinestring".to_string(),
            Box::new(self.logical_type()),
            None,
        )
    }

    fn into_arrow(self) -> ListArray<i64> {
        let linestrings_type = self.linestrings_type();
        let extension_type = self.extension_type();

        let validity: Option<Bitmap> = if let Some(validity) = self.validity {
            validity.into()
        } else {
            None
        };

        let coord_array = self.coords.into_arrow();
        let ring_array =
            ListArray::new(linestrings_type, self.ring_offsets, coord_array, None).boxed();
        ListArray::new(extension_type, self.geom_offsets, ring_array, validity)
    }

    // /// Build a spatial index containing this array's geometries
    // fn rstar_tree(&'a self) -> RTree<Self::Scalar> {
    //     let mut tree = RTree::new();
    //     self.iter().flatten().for_each(|geom| tree.insert(geom));
    //     tree
    // }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.geom_offsets.len()
    }

    /// Returns the optional validity.
    #[inline]
    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Returns a clone of this [`PrimitiveArray`] sliced by an offset and length.
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
    #[must_use]
    fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Returns a clone of this [`PrimitiveArray`] sliced by an offset and length.
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    #[inline]
    #[must_use]
    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        let validity = self
            .validity
            .clone()
            .map(|bitmap| bitmap.slice_unchecked(offset, length))
            .and_then(|bitmap| (bitmap.unset_bits() > 0).then_some(bitmap));

        let geom_offsets = self
            .geom_offsets
            .clone()
            .slice_unchecked(offset, length + 1);

        Self {
            coords: self.coords.clone(),
            geom_offsets,
            ring_offsets: self.ring_offsets.clone(),
            validity,
        }
    }

    fn to_boxed(&self) -> Box<Self> {
        Box::new(self.clone())
    }
}

// Implement geometry accessors
impl MultiLineStringArray {
    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = geo::MultiLineString> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(
        &self,
    ) -> ZipValidity<
        geo::MultiLineString,
        impl Iterator<Item = geo::MultiLineString> + '_,
        BitmapIter,
    > {
        ZipValidity::new_with_validity(self.iter_geo_values(), self.validity())
    }

    // GEOS from not implemented for MultiLineString I suppose
    //
    // /// Returns the value at slot `i` as a GEOS geometry.
    // #[cfg(feature = "geos")]
    // pub fn value_as_geos(&self, i: usize) -> geos::Geometry {
    //     (&self.value_as_geo(i)).try_into().unwrap()
    // }

    // /// Gets the value at slot `i` as a GEOS geometry, additionally checking the validity bitmap
    // #[cfg(feature = "geos")]
    // pub fn get_as_geos(&self, i: usize) -> Option<geos::Geometry> {
    //     if self.is_null(i) {
    //         return None;
    //     }

    //     self.get_as_geo(i).as_ref().map(|g| g.try_into().unwrap())
    // }

    // /// Iterator over GEOS geometry objects
    // #[cfg(feature = "geos")]
    // pub fn iter_geos_values(&self) -> impl Iterator<Item = geos::Geometry> + '_ {
    //     (0..self.len()).map(|i| self.value_as_geos(i))
    // }

    // /// Iterator over GEOS geometry objects, taking validity into account
    // #[cfg(feature = "geos")]
    // pub fn iter_geos(
    //     &self,
    // ) -> ZipValidity<geos::Geometry, impl Iterator<Item = geos::Geometry> + '_, BitmapIter> {
    //     ZipValidity::new_with_validity(self.iter_geos_values(), self.validity())
    // }
}

impl TryFrom<ListArray<i64>> for MultiLineStringArray {
    type Error = GeoArrowError;

    fn try_from(_value: ListArray<i64>) -> Result<Self, Self::Error> {
        todo!();
        // let geom_offsets = value.offsets();
        // let validity = value.validity();

        // let inner_dyn_array = value.values();
        // let inner_array = inner_dyn_array
        //     .as_any()
        //     .downcast_ref::<ListArray<i64>>()
        //     .unwrap();

        // let ring_offsets = inner_array.offsets();
        // let coords_dyn_array = inner_array.values();
        // let coords_array = coords_dyn_array
        //     .as_any()
        //     .downcast_ref::<StructArray>()
        //     .unwrap();

        // let x_array_values = coords_array.values()[0]
        //     .as_any()
        //     .downcast_ref::<PrimitiveArray<f64>>()
        //     .unwrap();
        // let y_array_values = coords_array.values()[1]
        //     .as_any()
        //     .downcast_ref::<PrimitiveArray<f64>>()
        //     .unwrap();

        // Ok(Self::new(
        //     x_array_values.values().clone(),
        //     y_array_values.values().clone(),
        //     geom_offsets.clone(),
        //     ring_offsets.clone(),
        //     validity.cloned(),
        // ))
    }
}

impl TryFrom<Box<dyn Array>> for MultiLineStringArray {
    type Error = GeoArrowError;

    fn try_from(value: Box<dyn Array>) -> Result<Self, Self::Error> {
        let arr = value.as_any().downcast_ref::<ListArray<i64>>().unwrap();
        arr.clone().try_into()
    }
}

impl From<Vec<Option<geo::MultiLineString>>> for MultiLineStringArray {
    fn from(other: Vec<Option<geo::MultiLineString>>) -> Self {
        let mut_arr: MutableMultiLineStringArray = other.into();
        mut_arr.into()
    }
}

impl From<Vec<geo::MultiLineString>> for MultiLineStringArray {
    fn from(other: Vec<geo::MultiLineString>) -> Self {
        let mut_arr: MutableMultiLineStringArray = other.into();
        mut_arr.into()
    }
}

/// Polygon and MultiLineString have the same layout, so enable conversions between the two to
/// change the semantic type
impl From<MultiLineStringArray> for PolygonArray {
    fn from(value: MultiLineStringArray) -> Self {
        Self::new(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            value.validity,
        )
    }
}

#[cfg(test)]
mod test {
    use crate::test::multilinestring::{ml0, ml1};

    use super::*;

    #[test]
    fn geo_roundtrip_accurate() {
        let arr: MultiLineStringArray = vec![ml0(), ml1()].into();
        assert_eq!(arr.value_as_geo(0), ml0());
        assert_eq!(arr.value_as_geo(1), ml1());
    }

    #[test]
    fn geo_roundtrip_accurate_option_vec() {
        let arr: MultiLineStringArray = vec![Some(ml0()), Some(ml1()), None].into();
        assert_eq!(arr.get_as_geo(0), Some(ml0()));
        assert_eq!(arr.get_as_geo(1), Some(ml1()));
        assert_eq!(arr.get_as_geo(2), None);
    }

    #[test]
    fn slice() {
        let arr: MultiLineStringArray = vec![ml0(), ml1()].into();
        let sliced = arr.slice(1, 1);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(ml1()));
    }
}
