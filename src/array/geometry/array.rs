use arrow_buffer::NullBuffer;
use arrow_schema::DataType;
use arrow_array::{OffsetSizeTrait, Array};

// use crate::algorithm::native::type_id::TypeIds;
use crate::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
    PolygonArray, RectArray, WKBArray,
};
use crate::error::GeoArrowError;
use crate::scalar::Geometry;
use crate::GeometryArrayTrait;

/// A GeometryArray is an enum over the various underlying _zero copy_ GeoArrow array types.
///
/// Notably this does _not_ include [`WKBArray`] as a variant, because that is not zero-copy to
/// parse.
#[derive(Debug, Clone, PartialEq)]
pub enum GeometryArray<O: OffsetSizeTrait> {
    Point(PointArray),
    LineString(LineStringArray<O>),
    Polygon(PolygonArray<O>),
    MultiPoint(MultiPointArray<O>),
    MultiLineString(MultiLineStringArray<O>),
    MultiPolygon(MultiPolygonArray<O>),
    Rect(RectArray),
}

impl<'a, O: OffsetSizeTrait> GeometryArrayTrait<'a> for GeometryArray<O> {
    type Scalar = crate::scalar::Geometry<'a, O>;
    type ScalarGeo = geo::Geometry;
    type ArrowArray = Box<dyn Array>;

    fn value(&'a self, i: usize) -> Self::Scalar {
        match self {
            GeometryArray::Point(arr) => Geometry::Point(arr.value(i)),
            GeometryArray::LineString(arr) => Geometry::LineString(arr.value(i)),
            GeometryArray::Polygon(arr) => Geometry::Polygon(arr.value(i)),
            GeometryArray::MultiPoint(arr) => Geometry::MultiPoint(arr.value(i)),
            GeometryArray::MultiLineString(arr) => Geometry::MultiLineString(arr.value(i)),
            GeometryArray::MultiPolygon(arr) => Geometry::MultiPolygon(arr.value(i)),
            GeometryArray::Rect(arr) => Geometry::Rect(arr.value(i)),
        }
    }

    fn logical_type(&self) -> DataType {
        match self {
            GeometryArray::Point(arr) => arr.logical_type(),
            GeometryArray::LineString(arr) => arr.logical_type(),
            GeometryArray::Polygon(arr) => arr.logical_type(),
            GeometryArray::MultiPoint(arr) => arr.logical_type(),
            GeometryArray::MultiLineString(arr) => arr.logical_type(),
            GeometryArray::MultiPolygon(arr) => arr.logical_type(),
            GeometryArray::Rect(arr) => arr.logical_type(),
        }
    }

    fn extension_type(&self) -> DataType {
        match self {
            GeometryArray::Point(arr) => arr.extension_type(),
            GeometryArray::LineString(arr) => arr.extension_type(),
            GeometryArray::Polygon(arr) => arr.extension_type(),
            GeometryArray::MultiPoint(arr) => arr.extension_type(),
            GeometryArray::MultiLineString(arr) => arr.extension_type(),
            GeometryArray::MultiPolygon(arr) => arr.extension_type(),
            GeometryArray::Rect(arr) => arr.extension_type(),
        }
    }

    fn into_arrow(self) -> Self::ArrowArray {
        match self {
            GeometryArray::Point(arr) => arr.into_arrow(),
            GeometryArray::LineString(arr) => arr.into_arrow().boxed(),
            GeometryArray::Polygon(arr) => arr.into_arrow().boxed(),
            GeometryArray::MultiPoint(arr) => arr.into_arrow().boxed(),
            GeometryArray::MultiLineString(arr) => arr.into_arrow().boxed(),
            GeometryArray::MultiPolygon(arr) => arr.into_arrow().boxed(),
            GeometryArray::Rect(arr) => arr.into_arrow().boxed(),
        }
    }

    fn into_boxed_arrow(self) -> Box<dyn Array> {
        self.into_arrow()
    }

    fn with_coords(self, coords: crate::array::CoordBuffer) -> Self {
        match self {
            GeometryArray::Point(arr) => GeometryArray::Point(arr.with_coords(coords)),
            GeometryArray::LineString(arr) => GeometryArray::LineString(arr.with_coords(coords)),
            GeometryArray::Polygon(arr) => GeometryArray::Polygon(arr.with_coords(coords)),
            GeometryArray::MultiPoint(arr) => GeometryArray::MultiPoint(arr.with_coords(coords)),
            GeometryArray::MultiLineString(arr) => {
                GeometryArray::MultiLineString(arr.with_coords(coords))
            }
            GeometryArray::MultiPolygon(arr) => {
                GeometryArray::MultiPolygon(arr.with_coords(coords))
            }
            GeometryArray::Rect(arr) => GeometryArray::Rect(arr.with_coords(coords)),
        }
    }

    fn coord_type(&self) -> crate::array::CoordType {
        match self {
            GeometryArray::Point(arr) => arr.coord_type(),
            GeometryArray::LineString(arr) => arr.coord_type(),
            GeometryArray::Polygon(arr) => arr.coord_type(),
            GeometryArray::MultiPoint(arr) => arr.coord_type(),
            GeometryArray::MultiLineString(arr) => arr.coord_type(),
            GeometryArray::MultiPolygon(arr) => arr.coord_type(),
            GeometryArray::Rect(arr) => arr.coord_type(),
        }
    }

    fn into_coord_type(self, coord_type: crate::array::CoordType) -> Self {
        match self {
            GeometryArray::Point(arr) => GeometryArray::Point(arr.into_coord_type(coord_type)),
            GeometryArray::LineString(arr) => {
                GeometryArray::LineString(arr.into_coord_type(coord_type))
            }
            GeometryArray::Polygon(arr) => GeometryArray::Polygon(arr.into_coord_type(coord_type)),
            GeometryArray::MultiPoint(arr) => {
                GeometryArray::MultiPoint(arr.into_coord_type(coord_type))
            }
            GeometryArray::MultiLineString(arr) => {
                GeometryArray::MultiLineString(arr.into_coord_type(coord_type))
            }
            GeometryArray::MultiPolygon(arr) => {
                GeometryArray::MultiPolygon(arr.into_coord_type(coord_type))
            }
            GeometryArray::Rect(arr) => GeometryArray::Rect(arr.into_coord_type(coord_type)),
        }
    }

    /// The length of the [`GeometryArray`]. Every array has a length corresponding to the number
    /// of geometries it contains.
    fn len(&self) -> usize {
        match self {
            GeometryArray::Point(arr) => arr.len(),
            GeometryArray::LineString(arr) => arr.len(),
            GeometryArray::Polygon(arr) => arr.len(),
            GeometryArray::MultiPoint(arr) => arr.len(),
            GeometryArray::MultiLineString(arr) => arr.len(),
            GeometryArray::MultiPolygon(arr) => arr.len(),
            GeometryArray::Rect(arr) => arr.len(),
        }
    }

    /// The validity of the [`GeometryArray`]: every array has an optional [`Bitmap`] that, when
    /// available specifies whether the geometry at a given slot is valid or not (null). When the
    /// validity is [`None`], all slots are valid.
    fn validity(&self) -> Option<&NullBuffer> {
        match self {
            GeometryArray::Point(arr) => arr.validity(),
            GeometryArray::LineString(arr) => arr.validity(),
            GeometryArray::Polygon(arr) => arr.validity(),
            GeometryArray::MultiPoint(arr) => arr.validity(),
            GeometryArray::MultiLineString(arr) => arr.validity(),
            GeometryArray::MultiPolygon(arr) => arr.validity(),
            GeometryArray::Rect(arr) => arr.validity(),
        }
    }

    /// Slices the [`GeometryArray`] in place
    /// # Implementation
    /// This operation is `O(1)` over `len`, as it amounts to increase two ref counts
    /// and moving the struct to the heap.
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    fn slice(&mut self, offset: usize, length: usize) {
        match self {
            GeometryArray::Point(arr) => arr.slice(offset, length),
            GeometryArray::LineString(arr) => arr.slice(offset, length),
            GeometryArray::Polygon(arr) => arr.slice(offset, length),
            GeometryArray::MultiPoint(arr) => arr.slice(offset, length),
            GeometryArray::MultiLineString(arr) => arr.slice(offset, length),
            GeometryArray::MultiPolygon(arr) => arr.slice(offset, length),
            GeometryArray::Rect(arr) => arr.slice(offset, length),
        }
    }

    /// Slices the [`GeometryArray`] in place
    /// # Implementation
    /// This operation is `O(1)` over `len`, as it amounts to increase two ref counts
    /// and moving the struct to the heap.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`
    unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        match self {
            GeometryArray::Point(arr) => arr.slice_unchecked(offset, length),
            GeometryArray::LineString(arr) => arr.slice_unchecked(offset, length),
            GeometryArray::Polygon(arr) => arr.slice_unchecked(offset, length),
            GeometryArray::MultiPoint(arr) => arr.slice_unchecked(offset, length),
            GeometryArray::MultiLineString(arr) => arr.slice_unchecked(offset, length),
            GeometryArray::MultiPolygon(arr) => arr.slice_unchecked(offset, length),
            GeometryArray::Rect(arr) => arr.slice_unchecked(offset, length),
        }
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Self {
        match self {
            GeometryArray::Point(arr) => GeometryArray::Point(arr.owned_slice(offset, length)),
            GeometryArray::LineString(arr) => {
                GeometryArray::LineString(arr.owned_slice(offset, length))
            }
            GeometryArray::Polygon(arr) => GeometryArray::Polygon(arr.owned_slice(offset, length)),
            GeometryArray::MultiPoint(arr) => {
                GeometryArray::MultiPoint(arr.owned_slice(offset, length))
            }
            GeometryArray::MultiLineString(arr) => {
                GeometryArray::MultiLineString(arr.owned_slice(offset, length))
            }
            GeometryArray::MultiPolygon(arr) => {
                GeometryArray::MultiPolygon(arr.owned_slice(offset, length))
            }
            GeometryArray::Rect(arr) => GeometryArray::Rect(arr.owned_slice(offset, length)),
        }
    }

    // /// Clones this [`GeometryArray`] with a new assigned bitmap.
    // /// # Panic
    // /// This function panics iff `validity.len() != self.len()`.
    // pub fn with_validity(&self, validity: Option<NullBuffer>) -> Box<GeometryArrayTrait>;

    /// Clone a [`GeometryArray`] to an owned `Box<GeometryArray>`.
    fn to_boxed(&self) -> Box<GeometryArray<O>> {
        Box::new(match self {
            GeometryArray::Point(arr) => GeometryArray::Point(arr.clone()),
            GeometryArray::LineString(arr) => GeometryArray::LineString(arr.clone()),
            GeometryArray::Polygon(arr) => GeometryArray::Polygon(arr.clone()),
            GeometryArray::MultiPoint(arr) => GeometryArray::MultiPoint(arr.clone()),
            GeometryArray::MultiLineString(arr) => GeometryArray::MultiLineString(arr.clone()),
            GeometryArray::MultiPolygon(arr) => GeometryArray::MultiPolygon(arr.clone()),
            GeometryArray::Rect(arr) => GeometryArray::Rect(arr.clone()),
        })
    }
}

impl TryFrom<&dyn Array> for GeometryArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type() {
            DataType::Extension(extension_name, _field, _extension_meta) => {
                match extension_name.as_str() {
                    "geoarrow.point" => Ok(GeometryArray::Point(value.try_into()?)),
                    "geoarrow.linestring" => Ok(GeometryArray::LineString(value.try_into()?)),
                    "geoarrow.polygon" => Ok(GeometryArray::Polygon(value.try_into()?)),
                    "geoarrow.multipoint" => Ok(GeometryArray::MultiPoint(value.try_into()?)),
                    "geoarrow.multilinestring" => {
                        Ok(GeometryArray::MultiLineString(value.try_into()?))
                    }
                    "geoarrow.multipolygon" => Ok(GeometryArray::MultiPolygon(value.try_into()?)),
                    // TODO: create a top-level API that parses any named geoarrow array?
                    // "geoarrow.wkb" => Ok(GeometryArray::WKB(value.try_into()?)),
                    _ => Err(GeoArrowError::General(format!(
                        "Unknown geoarrow type {}",
                        extension_name
                    ))),
                }
            }
            // TODO: better error here, and document that arrays without geoarrow extension
            // metadata should use TryFrom for a specific geometry type directly, instead of using
            // GeometryArray
            _ => todo!(),
        }
    }
}

impl TryFrom<&dyn Array> for GeometryArray<i64> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type() {
            DataType::Extension(extension_name, _field, _extension_meta) => {
                match extension_name.as_str() {
                    "geoarrow.point" => Ok(GeometryArray::Point(value.try_into()?)),
                    "geoarrow.linestring" => Ok(GeometryArray::LineString(value.try_into()?)),
                    "geoarrow.polygon" => Ok(GeometryArray::Polygon(value.try_into()?)),
                    "geoarrow.multipoint" => Ok(GeometryArray::MultiPoint(value.try_into()?)),
                    "geoarrow.multilinestring" => {
                        Ok(GeometryArray::MultiLineString(value.try_into()?))
                    }
                    "geoarrow.multipolygon" => Ok(GeometryArray::MultiPolygon(value.try_into()?)),
                    // "geoarrow.wkb" => Ok(GeometryArray::WKB(value.try_into()?)),
                    _ => Err(GeoArrowError::General(format!(
                        "Unknown geoarrow type {}",
                        extension_name
                    ))),
                }
            }
            // TODO: better error here, and document that arrays without geoarrow extension
            // metadata should use TryFrom for a specific geometry type directly, instead of using
            // GeometryArray
            _ => todo!(),
        }
    }
}

// TODO: write a macro to dedupe these `From`s
impl<O: OffsetSizeTrait> From<PointArray> for GeometryArray<O> {
    fn from(value: PointArray) -> Self {
        GeometryArray::Point(value)
    }
}

impl<O: OffsetSizeTrait> From<LineStringArray<O>> for GeometryArray<O> {
    fn from(value: LineStringArray<O>) -> Self {
        GeometryArray::LineString(value)
    }
}

impl<O: OffsetSizeTrait> From<PolygonArray<O>> for GeometryArray<O> {
    fn from(value: PolygonArray<O>) -> Self {
        GeometryArray::Polygon(value)
    }
}

impl<O: OffsetSizeTrait> From<MultiPointArray<O>> for GeometryArray<O> {
    fn from(value: MultiPointArray<O>) -> Self {
        GeometryArray::MultiPoint(value)
    }
}

impl<O: OffsetSizeTrait> From<MultiLineStringArray<O>> for GeometryArray<O> {
    fn from(value: MultiLineStringArray<O>) -> Self {
        GeometryArray::MultiLineString(value)
    }
}

impl<O: OffsetSizeTrait> From<MultiPolygonArray<O>> for GeometryArray<O> {
    fn from(value: MultiPolygonArray<O>) -> Self {
        GeometryArray::MultiPolygon(value)
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for GeometryArray<O> {
    type Error = GeoArrowError;
    fn try_from(value: WKBArray<O>) -> Result<Self, Self::Error> {
        let type_ids = value.get_unique_type_ids();

        if type_ids.is_empty() {
            return Err(GeoArrowError::General(
                "Input WKB array is empty.".to_string(),
            ));
        }

        if type_ids.len() == 1 {
            if type_ids.contains(&0) {
                return Ok(GeometryArray::Point(value.try_into()?));
            }

            if type_ids.contains(&1) {
                return Ok(GeometryArray::LineString(value.try_into()?));
            }

            if type_ids.contains(&3) {
                return Ok(GeometryArray::Polygon(value.try_into()?));
            }

            if type_ids.contains(&4) {
                return Ok(GeometryArray::MultiPoint(value.try_into()?));
            }

            if type_ids.contains(&5) {
                return Ok(GeometryArray::MultiLineString(value.try_into()?));
            }

            if type_ids.contains(&6) {
                return Ok(GeometryArray::MultiPolygon(value.try_into()?));
            }
        }

        if type_ids.len() == 3 {
            if type_ids.contains(&0) && type_ids.contains(&4) {
                return Ok(GeometryArray::MultiPoint(value.try_into()?));
            }

            if type_ids.contains(&1) && type_ids.contains(&5) {
                return Ok(GeometryArray::MultiLineString(value.try_into()?));
            }

            if type_ids.contains(&3) && type_ids.contains(&6) {
                return Ok(GeometryArray::MultiPolygon(value.try_into()?));
            }
        }

        Err(GeoArrowError::General(
            "Mixed WKB parsing not yet implemented".to_string(),
        ))
    }
}

impl From<GeometryArray<i32>> for GeometryArray<i64> {
    fn from(value: GeometryArray<i32>) -> Self {
        match value {
            GeometryArray::Point(arr) => GeometryArray::Point(arr),
            GeometryArray::LineString(arr) => GeometryArray::LineString(arr.into()),
            GeometryArray::Polygon(arr) => GeometryArray::Polygon(arr.into()),
            GeometryArray::MultiPoint(arr) => GeometryArray::MultiPoint(arr.into()),
            GeometryArray::MultiLineString(arr) => GeometryArray::MultiLineString(arr.into()),
            GeometryArray::MultiPolygon(arr) => GeometryArray::MultiPolygon(arr.into()),
            GeometryArray::Rect(arr) => GeometryArray::Rect(arr),
        }
    }
}

impl TryFrom<GeometryArray<i64>> for GeometryArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: GeometryArray<i64>) -> Result<Self, Self::Error> {
        Ok(match value {
            GeometryArray::Point(arr) => GeometryArray::Point(arr),
            GeometryArray::LineString(arr) => GeometryArray::LineString(arr.try_into()?),
            GeometryArray::Polygon(arr) => GeometryArray::Polygon(arr.try_into()?),
            GeometryArray::MultiPoint(arr) => GeometryArray::MultiPoint(arr.try_into()?),
            GeometryArray::MultiLineString(arr) => GeometryArray::MultiLineString(arr.try_into()?),
            GeometryArray::MultiPolygon(arr) => GeometryArray::MultiPolygon(arr.try_into()?),
            GeometryArray::Rect(arr) => GeometryArray::Rect(arr),
        })
    }
}
