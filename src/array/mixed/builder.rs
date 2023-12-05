use crate::array::linestring::LineStringCapacity;
use crate::array::mixed::array::GeometryType;
use crate::array::multilinestring::MultiLineStringCapacity;
use crate::array::multipoint::MultiPointCapacity;
use crate::array::multipolygon::MultiPolygonCapacity;
use crate::array::polygon::PolygonCapacity;
use crate::array::{
    CoordType, LineStringBuilder, MixedGeometryArray, MultiLineStringBuilder, MultiPointBuilder,
    MultiPolygonBuilder, PointBuilder, PolygonBuilder, WKBArray,
};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::*;
use crate::io::wkb::reader::geometry::WKBGeometry;
use crate::scalar::WKB;
use crate::trait_::IntoArrow;
use crate::GeometryArrayTrait;
use arrow_array::{OffsetSizeTrait, UnionArray};

/// The Arrow equivalent to a `Vec<Option<Geometry>>` with the caveat that these geometries must be
/// a _primitive_ geometry type. That means this does not support Geometry::GeometryCollection.
///
/// # Invariants
///
/// - All arrays must have the same dimension
/// - All arrays must have the same coordinate layout (interleaved or separated)
#[derive(Debug)]
pub struct MixedGeometryBuilder<O: OffsetSizeTrait> {
    // Invariant: every item in `types` is `> 0 && < fields.len()`
    // - 0: PointArray
    // - 1: LineStringArray
    // - 2: PolygonArray
    // - 3: MultiPointArray
    // - 4: MultiLineStringArray
    // - 5: MultiPolygonArray
    types: Vec<i8>,

    /// Note that we include an ordering so that exporting this array to Arrow is O(1). If we used
    /// another ordering like always Point, LineString, etc. then we'd either have to always export
    /// all arrays (including some zero-length arrays) or have to reorder the `types` buffer when
    /// exporting.
    // ordering: Vec<>,
    points: PointBuilder,
    line_strings: LineStringBuilder<O>,
    polygons: PolygonBuilder<O>,
    multi_points: MultiPointBuilder<O>,
    multi_line_strings: MultiLineStringBuilder<O>,
    multi_polygons: MultiPolygonBuilder<O>,

    // The offset of the _next_ geometry to be pushed into these arrays
    // This is necessary to maintain so that we can efficiently update `offsets` below
    point_counter: i32,
    line_string_counter: i32,
    polygon_counter: i32,
    multi_point_counter: i32,
    multi_line_string_counter: i32,
    multi_polygon_counter: i32,

    // Invariant: `offsets.len() == types.len()`
    offsets: Vec<i32>,
}

impl<'a, O: OffsetSizeTrait> MixedGeometryBuilder<O> {
    /// Creates a new empty [`MixedGeometryBuilder`].
    pub fn new() -> Self {
        Self {
            types: vec![],
            points: PointBuilder::new(),
            line_strings: LineStringBuilder::new(),
            polygons: PolygonBuilder::new(),
            multi_points: MultiPointBuilder::new(),
            multi_line_strings: MultiLineStringBuilder::new(),
            multi_polygons: MultiPolygonBuilder::new(),
            point_counter: 0,
            line_string_counter: 0,
            polygon_counter: 0,
            multi_point_counter: 0,
            multi_line_string_counter: 0,
            multi_polygon_counter: 0,
            offsets: vec![],
        }
    }

    /// Reserve capacity for at least `additional` more geometries.
    pub fn reserve_geometries(&mut self, additional: usize) {
        self.types.reserve(additional);
        self.offsets.reserve(additional);
    }

    /// Reserve capacity for at least `additional` more Points.
    pub fn reserve_points(&mut self, additional: usize) {
        self.points.reserve(additional);
    }

    /// Reserve capacity for at least `additional` more LineStrings.
    pub fn reserve_line_strings(&mut self, additional: LineStringCapacity) {
        self.line_strings.reserve(additional);
    }

    /// Reserve capacity for at least `additional` more Polygons.
    pub fn reserve_polygons(&mut self, additional: PolygonCapacity) {
        self.polygons.reserve(additional)
    }

    /// Reserve capacity for at least `additional` more MultiPoints.
    pub fn reserve_multi_points(&mut self, additional: MultiPointCapacity) {
        self.multi_points.reserve(additional)
    }

    /// Reserve capacity for at least `additional` more MultiLineStrings.
    pub fn reserve_multi_line_strings(&mut self, additional: MultiLineStringCapacity) {
        self.multi_line_strings.reserve(additional)
    }

    /// Reserve capacity for at least `additional` more MultiPolygons.
    pub fn reserve_multi_polygons(&mut self, additional: MultiPolygonCapacity) {
        self.multi_polygons.reserve(additional)
    }

    // /// The canonical method to create a [`MixedGeometryBuilder`] out of its internal
    // /// components.
    // ///
    // /// # Implementation
    // ///
    // /// This function is `O(1)`.
    // ///
    // /// # Errors
    // ///
    // pub fn try_new(
    //     coords: CoordBufferBuilder,
    //     geom_offsets: BufferBuilder<O>,
    //     ring_offsets: BufferBuilder<O>,
    //     validity: Option<MutableBitmap>,
    // ) -> Result<Self> {
    //     check(
    //         &coords.clone().into(),
    //         &geom_offsets.clone().into(),
    //         &ring_offsets.clone().into(),
    //         validity.as_ref().map(|x| x.len()),
    //     )?;
    //     Ok(Self {
    //         coords,
    //         geom_offsets,
    //         ring_offsets,
    //         validity,
    //     })
    // }

    /// Add a new Point to the end of this array, storing it in the PointBuilder child array.
    #[inline]
    pub fn push_point(&mut self, value: Option<&impl PointTrait<T = f64>>) {
        self.offsets.push(self.point_counter);
        self.point_counter += 1;

        self.types.push(GeometryType::Point.default_ordering());
        self.points.push_point(value)
    }

    /// Add a new Point to the end of this array, storing it in the MultiPointBuilder child
    /// array.
    #[inline]
    pub fn push_point_as_multi_point(
        &mut self,
        value: Option<&impl PointTrait<T = f64>>,
    ) -> Result<()> {
        self.offsets.push(self.multi_point_counter);
        self.multi_point_counter += 1;

        self.types.push(GeometryType::MultiPoint.default_ordering());
        self.multi_points.push_point(value)
    }

    /// Add a new LineString to the end of this array, storing it in the LineStringBuilder
    /// child array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_line_string(
        &mut self,
        value: Option<&impl LineStringTrait<T = f64>>,
    ) -> Result<()> {
        self.offsets.push(self.line_string_counter);
        self.line_string_counter += 1;

        self.types.push(GeometryType::LineString.default_ordering());
        self.line_strings.push_line_string(value)
    }

    /// Add a new LineString to the end of this array, storing it in the
    /// MultiLineStringBuilder child array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_line_string_as_multi_line_string(
        &mut self,
        value: Option<&impl LineStringTrait<T = f64>>,
    ) -> Result<()> {
        self.offsets.push(self.multi_line_string_counter);
        self.multi_line_string_counter += 1;

        self.types
            .push(GeometryType::MultiLineString.default_ordering());
        self.multi_line_strings.push_line_string(value)
    }

    /// Add a new Polygon to the end of this array, storing it in the PolygonBuilder
    /// child array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_polygon(&mut self, value: Option<&impl PolygonTrait<T = f64>>) -> Result<()> {
        self.offsets.push(self.polygon_counter);
        self.polygon_counter += 1;

        self.types.push(GeometryType::Polygon.default_ordering());
        self.polygons.push_polygon(value)
    }

    /// Add a new Polygon to the end of this array, storing it in the MultiPolygonBuilder
    /// child array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_polygon_as_multi_polygon(
        &mut self,
        value: Option<&impl PolygonTrait<T = f64>>,
    ) -> Result<()> {
        self.offsets.push(self.multi_polygon_counter);
        self.multi_polygon_counter += 1;

        self.types
            .push(GeometryType::MultiPolygon.default_ordering());
        self.multi_polygons.push_polygon(value)
    }

    /// Add a new MultiPoint to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_multi_point(
        &mut self,
        value: Option<&impl MultiPointTrait<T = f64>>,
    ) -> Result<()> {
        self.offsets.push(self.multi_point_counter);
        self.multi_point_counter += 1;

        self.types.push(GeometryType::MultiPoint.default_ordering());
        self.multi_points.push_multi_point(value)
    }

    /// Add a new MultiLineString to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_multi_line_string(
        &mut self,
        value: Option<&impl MultiLineStringTrait<T = f64>>,
    ) -> Result<()> {
        self.offsets.push(self.multi_line_string_counter);
        self.multi_line_string_counter += 1;

        self.types
            .push(GeometryType::MultiLineString.default_ordering());
        self.multi_line_strings.push_multi_line_string(value)
    }

    /// Add a new MultiPolygon to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_multi_polygon(
        &mut self,
        value: Option<&impl MultiPolygonTrait<T = f64>>,
    ) -> Result<()> {
        self.offsets.push(self.multi_polygon_counter);
        self.multi_polygon_counter += 1;

        self.types
            .push(GeometryType::MultiPolygon.default_ordering());
        self.multi_polygons.push_multi_polygon(value)
    }

    pub fn push_geometry(&mut self, value: &'a impl GeometryTrait<T = f64>) -> Result<()> {
        match value.as_type() {
            crate::geo_traits::GeometryType::Point(g) => self.push_point(Some(g)),
            crate::geo_traits::GeometryType::LineString(g) => self.push_line_string(Some(g))?,
            crate::geo_traits::GeometryType::Polygon(g) => self.push_polygon(Some(g))?,
            crate::geo_traits::GeometryType::MultiPoint(p) => self.push_multi_point(Some(p))?,
            crate::geo_traits::GeometryType::MultiLineString(p) => {
                self.push_multi_line_string(Some(p))?
            }
            crate::geo_traits::GeometryType::MultiPolygon(p) => self.push_multi_polygon(Some(p))?,
            crate::geo_traits::GeometryType::GeometryCollection(_) => {
                panic!("nested geometry collections not supported")
            }
            _ => todo!(),
        };
        Ok(())
    }

    pub fn from_wkb<W: OffsetSizeTrait>(
        wkb_objects: &[Option<WKB<'_, W>>],
        coord_type: Option<CoordType>,
    ) -> Result<Self> {
        // let wkb_objects2: Vec<WKBGeometry> =
        //     wkb_objects.iter().map(|wkb| wkb.to_wkb_object()).collect();

        let wkb_objects2: Vec<Option<WKBGeometry>> = wkb_objects
            .iter()
            .map(|maybe_wkb| maybe_wkb.as_ref().map(|wkb| wkb.to_wkb_object()))
            .collect();
        todo!()
        // let mut array = Self::new();
        // Ok(Self::from_nullable_line_strings(&wkb_objects2, coord_type))
    }

    pub fn finish(self) -> MixedGeometryArray<O> {
        self.into()
    }
}

impl<O: OffsetSizeTrait> Default for MixedGeometryBuilder<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: OffsetSizeTrait> IntoArrow for MixedGeometryBuilder<O> {
    type ArrowArray = UnionArray;

    fn into_arrow(self) -> Self::ArrowArray {
        todo!()
    }
}

impl<O: OffsetSizeTrait> From<MixedGeometryBuilder<O>> for MixedGeometryArray<O> {
    fn from(other: MixedGeometryBuilder<O>) -> Self {
        Self::new(
            other.types.into(),
            other.offsets.into(),
            other.points.into(),
            other.line_strings.into(),
            other.polygons.into(),
            other.multi_points.into(),
            other.multi_line_strings.into(),
            other.multi_polygons.into(),
        )
    }
}

// TODO: figure out these trait impl errors
// fn from_geometry_trait_iterator<'a, O: OffsetSizeTrait>(
//     geoms: impl Iterator<Item = impl GeometryTrait<T = f64> + 'a>,
//     prefer_multi: bool
// ) -> MixedGeometryBuilder<O> {
//     let mut array = MixedGeometryBuilder::new();

//     for geom in geoms.into_iter() {
//         match geom.as_type() {
//             GeometryType::Point(point) => {
//                 array.push_valid_point(point);
//                 // if prefer_multi {
//                 //     array.push_point_as_multi_point(Some(point));
//                 // } else {
//                 //     array.push_point(Some(point));
//                 // }
//             }
//             _ => todo!(),
//         };
//         // maybe_geom.
//     }

//     array
// }

fn from_geo_iterator<'a, O: OffsetSizeTrait>(
    geoms: impl Iterator<Item = &'a geo::Geometry>,
    prefer_multi: bool,
) -> Result<MixedGeometryBuilder<O>> {
    let mut array = MixedGeometryBuilder::new();

    for geom in geoms.into_iter() {
        match geom {
            geo::Geometry::Point(point) => {
                if prefer_multi {
                    array.push_point_as_multi_point(Some(point))?;
                } else {
                    array.push_point(Some(point));
                }
            }
            geo::Geometry::LineString(line_string) => {
                if prefer_multi {
                    array.push_line_string_as_multi_line_string(Some(line_string))?;
                } else {
                    array.push_line_string(Some(line_string))?;
                }
            }
            geo::Geometry::Polygon(polygon) => {
                if prefer_multi {
                    array.push_polygon_as_multi_polygon(Some(polygon))?;
                } else {
                    array.push_polygon(Some(polygon))?;
                }
            }
            geo::Geometry::MultiPoint(multi_point) => {
                array.push_multi_point(Some(multi_point))?;
            }
            geo::Geometry::MultiLineString(multi_line_string) => {
                array.push_multi_line_string(Some(multi_line_string))?;
            }
            geo::Geometry::MultiPolygon(multi_polygon) => {
                array.push_multi_polygon(Some(multi_polygon))?;
            }
            _ => todo!(),
        }
    }

    Ok(array)
}

impl<O: OffsetSizeTrait> TryFrom<Vec<geo::Geometry>> for MixedGeometryBuilder<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<geo::Geometry>) -> std::result::Result<Self, Self::Error> {
        from_geo_iterator(value.iter(), true)
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for MixedGeometryBuilder<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> std::result::Result<Self, Self::Error> {
        assert_eq!(
            value.nulls().map_or(0, |validity| validity.null_count()),
            0,
            "Parsing a WKBArray with null elements not supported",
        );

        // TODO: do a first pass over WKB array to compute sizes for each geometry type
        let mut result_arr = MixedGeometryBuilder::new();

        let wkb_objects: Vec<WKB<'_, O>> =
            value.iter().map(|maybe_wkb| maybe_wkb.unwrap()).collect();
        let wkb_objects2: Vec<WKBGeometry> =
            wkb_objects.iter().map(|wkb| wkb.to_wkb_object()).collect();
        for wkb in wkb_objects2 {
            result_arr.push_geometry(&wkb)?;
        }

        Ok(result_arr)
    }
}
