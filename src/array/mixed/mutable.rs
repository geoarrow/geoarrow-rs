use crate::array::mixed::array::GeometryType;
use crate::array::{
    MixedGeometryArray, MutableLineStringArray, MutableMultiLineStringArray,
    MutableMultiPointArray, MutableMultiPolygonArray, MutablePointArray, MutablePolygonArray,
};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::*;
use arrow2::types::Offset;

/// The Arrow equivalent to a `Vec<Option<Geometry>>` with the caveat that these geometries must be
/// a _primitive_ geometry type. That means this does not support Geometry::GeometryCollection.
///
/// # Invariants
///
/// - All arrays must have the same dimension
/// - All arrays must have the same coordinate layout (interleaved or separated)
#[derive(Debug, Clone)]
pub struct MutableMixedGeometryArray<O: Offset> {
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
    points: MutablePointArray,
    line_strings: MutableLineStringArray<O>,
    polygons: MutablePolygonArray<O>,
    multi_points: MutableMultiPointArray<O>,
    multi_line_strings: MutableMultiLineStringArray<O>,
    multi_polygons: MutableMultiPolygonArray<O>,

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

impl<'a, O: Offset> MutableMixedGeometryArray<O> {
    /// Creates a new empty [`MutableMixedGeometryArray`].
    pub fn new() -> Self {
        Self {
            types: vec![],
            points: MutablePointArray::new(),
            line_strings: MutableLineStringArray::new(),
            polygons: MutablePolygonArray::new(),
            multi_points: MutableMultiPointArray::new(),
            multi_line_strings: MutableMultiLineStringArray::new(),
            multi_polygons: MutableMultiPolygonArray::new(),
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
    pub fn reserve_line_strings(&mut self, coord_additional: usize, geom_additional: usize) {
        self.line_strings.reserve(coord_additional, geom_additional);
    }

    /// Reserve capacity for at least `additional` more Polygons.
    pub fn reserve_polygons(
        &mut self,
        coord_additional: usize,
        ring_additional: usize,
        geom_additional: usize,
    ) {
        self.polygons
            .reserve(coord_additional, ring_additional, geom_additional)
    }

    /// Reserve capacity for at least `additional` more MultiPoints.
    pub fn reserve_multi_points(&mut self, coord_additional: usize, geom_additional: usize) {
        self.multi_points.reserve(coord_additional, geom_additional)
    }

    /// Reserve capacity for at least `additional` more MultiLineStrings.
    pub fn reserve_multi_line_strings(
        &mut self,
        coord_additional: usize,
        ring_additional: usize,
        geom_additional: usize,
    ) {
        self.multi_line_strings
            .reserve(coord_additional, ring_additional, geom_additional)
    }

    /// Reserve capacity for at least `additional` more MultiPolygons.
    pub fn reserve_multi_polygons(
        &mut self,
        coord_additional: usize,
        ring_additional: usize,
        polygon_additional: usize,
        geom_additional: usize,
    ) {
        self.multi_polygons.reserve(
            coord_additional,
            ring_additional,
            polygon_additional,
            geom_additional,
        )
    }

    // /// The canonical method to create a [`MutableMixedGeometryArray`] out of its internal
    // /// components.
    // ///
    // /// # Implementation
    // ///
    // /// This function is `O(1)`.
    // ///
    // /// # Errors
    // ///
    // pub fn try_new(
    //     coords: MutableCoordBuffer,
    //     geom_offsets: Offsets<O>,
    //     ring_offsets: Offsets<O>,
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

    /// Add a new Point to the end of this array, storing it in the MutablePointArray child array.
    #[inline]
    pub fn push_point(&mut self, value: Option<impl PointTrait<T = f64>>) {
        self.offsets.push(self.point_counter);
        self.point_counter += 1;

        self.types.push(GeometryType::Point.default_ordering());
        self.points.push_point(value)
    }

    /// Add a new Point to the end of this array, storing it in the MutableMultiPointArray child
    /// array.
    #[inline]
    pub fn push_point_as_multi_point(
        &mut self,
        value: Option<impl PointTrait<T = f64>>,
    ) -> Result<()> {
        self.offsets.push(self.multi_point_counter);
        self.multi_point_counter += 1;

        self.types.push(GeometryType::MultiPoint.default_ordering());
        self.multi_points.push_point(value)
    }

    /// Add a new LineString to the end of this array, storing it in the MutableLineStringArray
    /// child array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_line_string(
        &mut self,
        value: Option<impl LineStringTrait<'a, T = f64>>,
    ) -> Result<()> {
        self.offsets.push(self.line_string_counter);
        self.line_string_counter += 1;

        self.types.push(GeometryType::LineString.default_ordering());
        self.line_strings.push_line_string(value)
    }

    /// Add a new LineString to the end of this array, storing it in the
    /// MutableMultiLineStringArray child array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_line_string_as_multi_line_string(
        &mut self,
        value: Option<impl LineStringTrait<'a, T = f64>>,
    ) -> Result<()> {
        self.offsets.push(self.multi_line_string_counter);
        self.multi_line_string_counter += 1;

        self.types
            .push(GeometryType::MultiLineString.default_ordering());
        self.multi_line_strings.push_line_string(value)
    }

    /// Add a new Polygon to the end of this array, storing it in the MutablePolygonArray
    /// child array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_polygon(&mut self, value: Option<impl PolygonTrait<'a, T = f64>>) -> Result<()> {
        self.offsets.push(self.polygon_counter);
        self.polygon_counter += 1;

        self.types.push(GeometryType::Polygon.default_ordering());
        self.polygons.push_polygon(value)
    }

    /// Add a new Polygon to the end of this array, storing it in the MutableMultiPolygonArray
    /// child array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_polygon_as_multi_polygon(
        &mut self,
        value: Option<impl PolygonTrait<'a, T = f64>>,
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
        value: Option<impl MultiPointTrait<'a, T = f64>>,
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
        value: Option<impl MultiLineStringTrait<'a, T = f64>>,
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
        value: Option<impl MultiPolygonTrait<'a, T = f64>>,
    ) -> Result<()> {
        self.offsets.push(self.multi_polygon_counter);
        self.multi_polygon_counter += 1;

        self.types
            .push(GeometryType::MultiPolygon.default_ordering());
        self.multi_polygons.push_multi_polygon(value)
    }
}

impl<C: CoordBuffer, O: Offset> Default for MutableMixedGeometryArray<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: CoordBuffer, O: Offset> From<MutableMixedGeometryArray<O>> for MixedGeometryArray<O> {
    fn from(other: MutableMixedGeometryArray<O>) -> Self {
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
// fn from_geometry_trait_iterator<'a, O: Offset>(
//     geoms: impl Iterator<Item = impl GeometryTrait<'a, T = f64> + 'a>,
//     prefer_multi: bool
// ) -> MutableMixedGeometryArray<O> {
//     let mut array = MutableMixedGeometryArray::new();

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

fn from_geo_iterator<'a, O: Offset>(
    geoms: impl Iterator<Item = &'a geo::Geometry>,
    prefer_multi: bool,
) -> Result<MutableMixedGeometryArray<O>> {
    let mut array = MutableMixedGeometryArray::new();

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

impl<C: CoordBuffer, O: Offset> TryFrom<Vec<geo::Geometry>> for MutableMixedGeometryArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<geo::Geometry>) -> std::result::Result<Self, Self::Error> {
        from_geo_iterator(value.iter(), true)
    }
}
