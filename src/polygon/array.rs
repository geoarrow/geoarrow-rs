use crate::error::GeoArrowError;
use crate::{CoordBuffer, GeometryArrayTrait, MultiLineStringArray};
use arrow2::array::Array;
use arrow2::array::ListArray;
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::{DataType, Field};
use arrow2::offset::OffsetsBuffer;
use geozero::{GeomProcessor, GeozeroGeometry};

use super::MutablePolygonArray;

/// A [`GeometryArrayTrait`] semantically equivalent to `Vec<Option<Polygon>>` using Arrow's
/// in-memory representation.
#[derive(Debug, Clone)]
pub struct PolygonArray {
    coords: CoordBuffer,

    /// Offsets into the ring array where each geometry starts
    geom_offsets: OffsetsBuffer<i64>,

    /// Offsets into the coordinate array where each ring starts
    ring_offsets: OffsetsBuffer<i64>,

    /// Validity bitmap
    validity: Option<Bitmap>,
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

impl PolygonArray {
    /// Create a new PolygonArray from parts
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

    /// Create a new PolygonArray from parts
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

    fn rings_type(&self) -> DataType {
        let vertices_field = Field::new("vertices", self.vertices_type(), false);
        DataType::LargeList(Box::new(vertices_field))
    }

    fn outer_type(&self) -> DataType {
        let rings_field = Field::new("rings", self.rings_type(), true);
        DataType::LargeList(Box::new(rings_field))
    }
}

impl<'a> GeometryArrayTrait<'a> for PolygonArray {
    type Scalar = crate::Polygon<'a>;
    type ScalarGeo = geo::Polygon;
    type ArrowArray = ListArray<i64>;

    fn value(&'a self, i: usize) -> Self::Scalar {
        crate::Polygon {
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
            "geoarrow.polygon".to_string(),
            Box::new(self.logical_type()),
            None,
        )
    }

    fn into_arrow(self) -> Self::ArrowArray {
        let rings_type = self.rings_type();
        let extension_type = self.extension_type();

        let validity: Option<Bitmap> = if let Some(validity) = self.validity {
            validity.into()
        } else {
            None
        };

        let coord_array = self.coords.into_arrow();
        let ring_array = ListArray::new(rings_type, self.ring_offsets, coord_array, None).boxed();
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
impl PolygonArray {
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
        (&self.value_as_geo(i)).try_into().unwrap()
    }

    /// Gets the value at slot `i` as a GEOS geometry, additionally checking the validity bitmap
    #[cfg(feature = "geos")]
    pub fn get_as_geos(&self, i: usize) -> Option<geos::Geometry> {
        if self.is_null(i) {
            return None;
        }

        self.get_as_geo(i).as_ref().map(|g| g.try_into().unwrap())
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

impl TryFrom<ListArray<i64>> for PolygonArray {
    type Error = GeoArrowError;

    fn try_from(_value: ListArray<i64>) -> Result<Self, Self::Error> {
        todo!()
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

impl TryFrom<Box<dyn Array>> for PolygonArray {
    type Error = GeoArrowError;

    fn try_from(value: Box<dyn Array>) -> Result<Self, Self::Error> {
        let arr = value.as_any().downcast_ref::<ListArray<i64>>().unwrap();
        arr.clone().try_into()
    }
}

impl From<Vec<Option<geo::Polygon>>> for PolygonArray {
    fn from(other: Vec<Option<geo::Polygon>>) -> Self {
        let mut_arr: MutablePolygonArray = other.into();
        mut_arr.into()
    }
}

impl From<Vec<geo::Polygon>> for PolygonArray {
    fn from(other: Vec<geo::Polygon>) -> Self {
        let mut_arr: MutablePolygonArray = other.into();
        mut_arr.into()
    }
}

/// Polygon and MultiLineString have the same layout, so enable conversions between the two to
/// change the semantic type
impl From<PolygonArray> for MultiLineStringArray {
    fn from(value: PolygonArray) -> Self {
        Self::new(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            value.validity,
        )
    }
}

impl GeozeroGeometry for PolygonArray {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            let (start_ring_idx, end_ring_idx) = self.geom_offsets.start_end(geom_idx);

            processor.polygon_begin(true, end_ring_idx - start_ring_idx, geom_idx)?;

            for ring_idx in start_ring_idx..end_ring_idx {
                let (start_coord_idx, end_coord_idx) = self.ring_offsets.start_end(ring_idx);

                processor.linestring_begin(
                    false,
                    end_coord_idx - start_coord_idx,
                    ring_idx - start_ring_idx,
                )?;

                for coord_idx in start_coord_idx..end_coord_idx {
                    processor.xy(
                        self.coords.get_x(coord_idx),
                        self.coords.get_y(coord_idx),
                        coord_idx - start_coord_idx,
                    )?;
                }

                processor.linestring_end(false, ring_idx - start_ring_idx)?;
            }

            processor.polygon_end(true, geom_idx)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use geo::{polygon, Polygon};
    use geozero::ToWkt;

    fn p0() -> Polygon {
        polygon![
            (x: -111., y: 45.),
            (x: -111., y: 41.),
            (x: -104., y: 41.),
            (x: -104., y: 45.),
        ]
    }

    fn p1() -> Polygon {
        polygon!(
            exterior: [
                (x: -111., y: 45.),
                (x: -111., y: 41.),
                (x: -104., y: 41.),
                (x: -104., y: 45.),
            ],
            interiors: [
                [
                    (x: -110., y: 44.),
                    (x: -110., y: 42.),
                    (x: -105., y: 42.),
                    (x: -105., y: 44.),
                ],
            ],
        )
    }

    #[test]
    fn geo_roundtrip_accurate() {
        let arr: PolygonArray = vec![p0(), p1()].into();
        assert_eq!(arr.value_as_geo(0), p0());
        assert_eq!(arr.value_as_geo(1), p1());
    }

    #[test]
    fn geo_roundtrip_accurate_option_vec() {
        let arr: PolygonArray = vec![Some(p0()), Some(p1()), None].into();
        assert_eq!(arr.get_as_geo(0), Some(p0()));
        assert_eq!(arr.get_as_geo(1), Some(p1()));
        assert_eq!(arr.get_as_geo(2), None);
    }

    #[test]
    fn geozero_process_geom() -> geozero::error::Result<()> {
        let arr: PolygonArray = vec![p0(), p1()].into();
        let wkt = arr.to_wkt()?;
        let expected = "GEOMETRYCOLLECTION(POLYGON((-111 45,-111 41,-104 41,-104 45,-111 45)),POLYGON((-111 45,-111 41,-104 41,-104 45,-111 45),(-110 44,-110 42,-105 42,-105 44,-110 44)))";
        assert_eq!(wkt, expected);
        Ok(())
    }

    #[test]
    fn slice() {
        let arr: PolygonArray = vec![p0(), p1()].into();
        let sliced = arr.slice(1, 1);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(p1()));
    }
}
