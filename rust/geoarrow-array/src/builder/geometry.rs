use std::sync::Arc;

use arrow_array::OffsetSizeTrait;
use geo_traits::*;
use geoarrow_schema::{
    Dimension, GeometryCollectionType, GeometryType, LineStringType, Metadata, MultiLineStringType,
    MultiPointType, MultiPolygonType, PointType, PolygonType,
};

use crate::array::{DimensionIndex, GeometryArray, WkbArray};
use crate::builder::{
    GeometryCollectionBuilder, LineStringBuilder, MultiLineStringBuilder, MultiPointBuilder,
    MultiPolygonBuilder, PointBuilder, PolygonBuilder,
};
use crate::capacity::GeometryCapacity;
use crate::error::{GeoArrowError, Result};
use crate::trait_::{ArrayAccessor, GeoArrowArray, GeometryArrayBuilder};

pub(crate) const DEFAULT_PREFER_MULTI: bool = false;

/// The GeoArrow equivalent to a `Vec<Option<Geometry>>`: a mutable collection of Geometries.
///
/// Each Geometry can have a different dimension. All geometries must have the same coordinate
/// type.
///
/// This currently has the caveat that these geometries must be a _primitive_ geometry type. This
/// does not currently support nested GeometryCollection objects.
///
/// Converting an [`GeometryBuilder`] into a [`GeometryArray`] is `O(1)`.
///
/// # Invariants
///
/// - All arrays must have the same coordinate layout (interleaved or separated)
#[derive(Debug)]
pub struct GeometryBuilder {
    metadata: Arc<Metadata>,

    // Invariant: every item in `types` is `> 0 && < fields.len()`
    types: Vec<i8>,

    /// An array of PointArray, ordered XY, XYZ, XYM, XYZM
    points: [PointBuilder; 4],
    line_strings: [LineStringBuilder; 4],
    polygons: [PolygonBuilder; 4],
    mpoints: [MultiPointBuilder; 4],
    mline_strings: [MultiLineStringBuilder; 4],
    mpolygons: [MultiPolygonBuilder; 4],
    gcs: [GeometryCollectionBuilder; 4],

    // Invariant: `offsets.len() == types.len()`
    offsets: Vec<i32>,

    /// Whether to prefer multi or single arrays for new geometries.
    ///
    /// E.g. if this is `true` and a Point geometry is added, it will be added to the
    /// MultiPointBuilder. If this is `false`, the Point geometry will be added to the
    /// PointBuilder.
    ///
    /// The idea is that always adding multi-geometries will make it easier to downcast later.
    pub(crate) prefer_multi: bool,

    /// The number of nulls that has been deferred and are still to be written.
    ///
    /// Adding nulls is tricky. We often want to use this builder as a generic builder for data
    /// from unknown sources, which then gets downcasted to an array of a specific type.
    ///
    /// In a large majority of the time, this builder will have only data of a single type, which
    /// can then get downcasted to a simple array of a single geometry type and dimension. But in
    /// order for this process to be easy, we want the nulls to be assigned to the same array type
    /// as the actual data.
    ///
    /// When there's a valid geometry pushed before the null, we can add the null to an existing
    /// non-null array type, but if there are no valid geometries yet, we don't know which array to
    /// push the null to. This `deferred_nulls` is the number of initial null values that haven't
    /// yet been written to an array, because we don't know which array to write them to.
    deferred_nulls: usize,
}

impl<'a> GeometryBuilder {
    /// Creates a new empty [`GeometryBuilder`].
    pub fn new(typ: GeometryType, prefer_multi: bool) -> Self {
        Self::with_capacity(typ, Default::default(), prefer_multi)
    }

    /// Creates a new [`GeometryBuilder`] with given capacity and no validity.
    pub fn with_capacity(
        typ: GeometryType,
        capacity: GeometryCapacity,
        prefer_multi: bool,
    ) -> Self {
        use Dimension::*;

        let metadata = typ.metadata().clone();
        let coord_type = typ.coord_type();

        let points = core::array::from_fn(|i| {
            PointBuilder::with_capacity(
                PointType::new(coord_type, Dimension::from_order(i), Default::default()),
                capacity.points()[i],
            )
        });
        let line_strings = core::array::from_fn(|i| {
            LineStringBuilder::with_capacity(
                LineStringType::new(coord_type, Dimension::from_order(i), Default::default()),
                capacity.line_strings()[i],
            )
        });
        let polygons = core::array::from_fn(|i| {
            PolygonBuilder::with_capacity(
                PolygonType::new(coord_type, Dimension::from_order(i), Default::default()),
                capacity.polygons()[i],
            )
        });
        let mpoints = core::array::from_fn(|i| {
            MultiPointBuilder::with_capacity(
                MultiPointType::new(coord_type, Dimension::from_order(i), Default::default()),
                capacity.multi_points()[i],
            )
        });
        let mline_strings = core::array::from_fn(|i| {
            MultiLineStringBuilder::with_capacity(
                MultiLineStringType::new(coord_type, Dimension::from_order(i), Default::default()),
                capacity.multi_line_strings()[i],
            )
        });
        let mpolygons = core::array::from_fn(|i| {
            MultiPolygonBuilder::with_capacity(
                MultiPolygonType::new(coord_type, Dimension::from_order(i), Default::default()),
                capacity.multi_polygons()[i],
            )
        });
        let gcs = core::array::from_fn(|i| {
            GeometryCollectionBuilder::with_capacity(
                GeometryCollectionType::new(coord_type, XY, Default::default()),
                capacity.gcs()[i],
                prefer_multi,
            )
        });

        // Don't store array metadata on child arrays
        Self {
            metadata,
            types: vec![],
            points,
            line_strings,
            polygons,
            mpoints,
            mline_strings,
            mpolygons,
            gcs,
            offsets: vec![],
            prefer_multi,
            deferred_nulls: 0,
        }
    }

    /// Reserves capacity for at least `additional` more geometries.
    ///
    /// The collection may reserve more space to speculatively avoid frequent reallocations. After
    /// calling `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, capacity: GeometryCapacity) {
        let total_num_geoms = capacity.total_num_geoms();
        self.types.reserve(total_num_geoms);
        self.offsets.reserve(total_num_geoms);

        capacity.points().iter().enumerate().for_each(|(i, cap)| {
            self.points[i].reserve(*cap);
        });
        capacity
            .line_strings()
            .iter()
            .enumerate()
            .for_each(|(i, cap)| {
                self.line_strings[i].reserve(*cap);
            });
        capacity.polygons().iter().enumerate().for_each(|(i, cap)| {
            self.polygons[i].reserve(*cap);
        });
        capacity
            .multi_points()
            .iter()
            .enumerate()
            .for_each(|(i, cap)| {
                self.mpoints[i].reserve(*cap);
            });
        capacity
            .multi_line_strings()
            .iter()
            .enumerate()
            .for_each(|(i, cap)| {
                self.mline_strings[i].reserve(*cap);
            });
        capacity
            .multi_polygons()
            .iter()
            .enumerate()
            .for_each(|(i, cap)| {
                self.mpolygons[i].reserve(*cap);
            });
        capacity.gcs().iter().enumerate().for_each(|(i, cap)| {
            self.gcs[i].reserve(*cap);
        });
    }

    /// Reserves the minimum capacity for at least `additional` more Geometries.
    ///
    /// Unlike [`reserve`], this will not deliberately over-allocate to speculatively avoid
    /// frequent allocations. After calling `reserve_exact`, capacity will be greater than or equal
    /// to `self.len() + additional`. Does nothing if the capacity is already sufficient.
    ///
    /// Note that the allocator may give the collection more space than it
    /// requests. Therefore, capacity can not be relied upon to be precisely
    /// minimal. Prefer [`reserve`] if future insertions are expected.
    ///
    /// [`reserve`]: Self::reserve
    pub fn reserve_exact(&mut self, capacity: GeometryCapacity) {
        let total_num_geoms = capacity.total_num_geoms();

        self.types.reserve_exact(total_num_geoms);
        self.offsets.reserve_exact(total_num_geoms);

        capacity.points().iter().enumerate().for_each(|(i, cap)| {
            self.points[i].reserve_exact(*cap);
        });
        capacity
            .line_strings()
            .iter()
            .enumerate()
            .for_each(|(i, cap)| {
                self.line_strings[i].reserve_exact(*cap);
            });
        capacity.polygons().iter().enumerate().for_each(|(i, cap)| {
            self.polygons[i].reserve_exact(*cap);
        });
        capacity
            .multi_points()
            .iter()
            .enumerate()
            .for_each(|(i, cap)| {
                self.mpoints[i].reserve_exact(*cap);
            });
        capacity
            .multi_line_strings()
            .iter()
            .enumerate()
            .for_each(|(i, cap)| {
                self.mline_strings[i].reserve_exact(*cap);
            });
        capacity
            .multi_polygons()
            .iter()
            .enumerate()
            .for_each(|(i, cap)| {
                self.mpolygons[i].reserve_exact(*cap);
            });
        capacity.gcs().iter().enumerate().for_each(|(i, cap)| {
            self.gcs[i].reserve_exact(*cap);
        });
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

    /// Consume the builder and convert to an immutable [`GeometryArray`]
    pub fn finish(self) -> GeometryArray {
        GeometryArray::new(
            self.types.into(),
            self.offsets.into(),
            self.points.map(|arr| arr.finish()),
            self.line_strings.map(|arr| arr.finish()),
            self.polygons.map(|arr| arr.finish()),
            self.mpoints.map(|arr| arr.finish()),
            self.mline_strings.map(|arr| arr.finish()),
            self.mpolygons.map(|arr| arr.finish()),
            self.gcs.map(|arr| arr.finish()),
            self.metadata,
        )
    }

    /// Creates a new builder with a capacity inferred by the provided iterator.
    pub fn with_capacity_from_iter(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
        typ: GeometryType,
        prefer_multi: bool,
    ) -> Result<Self> {
        let counter = GeometryCapacity::from_geometries(geoms, prefer_multi)?;
        Ok(Self::with_capacity(typ, counter, prefer_multi))
    }

    /// Reserve more space in the underlying buffers with the capacity inferred from the provided
    /// geometries.
    pub fn reserve_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
        prefer_multi: bool,
    ) -> Result<()> {
        let counter = GeometryCapacity::from_geometries(geoms, prefer_multi)?;
        self.reserve(counter);
        Ok(())
    }

    /// Reserve more space in the underlying buffers with the capacity inferred from the provided
    /// geometries.
    pub fn reserve_exact_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
        prefer_multi: bool,
    ) -> Result<()> {
        let counter = GeometryCapacity::from_geometries(geoms, prefer_multi)?;
        self.reserve_exact(counter);
        Ok(())
    }

    /// Add a new Point to the end of this array.
    ///
    /// If `self.prefer_multi` is `true`, it will be stored in the `MultiPointBuilder` child
    /// array. Otherwise, it will be stored in the `PointBuilder` child array.
    #[inline]
    pub fn push_point(&mut self, value: Option<&impl PointTrait<T = f64>>) -> Result<()> {
        if let Some(point) = value {
            let dim = point.dim().try_into().unwrap();
            if self.prefer_multi {
                self.add_multi_point_type(dim);
                // Flush deferred nulls
                (0..self.deferred_nulls).for_each(|_| self.mpoints[dim.order()].push_null());
                self.deferred_nulls = 0;

                self.mpoints[dim.order()].push_point(Some(point))?;
            } else {
                self.add_point_type(dim);

                // Flush deferred nulls
                (0..self.deferred_nulls).for_each(|_| self.points[dim.order()].push_null());
                self.deferred_nulls = 0;

                self.points[dim.order()].push_point(Some(point));
            }
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    fn add_point_type(&mut self, dim: Dimension) {
        // let child =
        self.offsets.push(self.point_xy.len().try_into().unwrap());
        match dim {
            Dimension::XY => {
                self.offsets.push(self.point_xy.len().try_into().unwrap());
                self.types.push(1)
            }
            Dimension::XYZ => {
                self.offsets.push(self.point_xyz.len().try_into().unwrap());
                self.types.push(11)
            }
            _ => todo!("Handle M and ZM dimensions"),
        }
    }

    /// Add a new LineString to the end of this array.
    ///
    /// If `self.prefer_multi` is `true`, it will be stored in the `MultiLineStringBuilder` child
    /// array. Otherwise, it will be stored in the `LineStringBuilder` child array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_line_string(
        &mut self,
        value: Option<&impl LineStringTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(line_string) = value {
            if self.prefer_multi {
                self.add_multi_line_string_type(line_string.dim().try_into().unwrap());
                match line_string.dim() {
                    Dimensions::Xy | Dimensions::Unknown(2) => {
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.mline_string_xy.push_null());
                        self.deferred_nulls = 0;

                        self.mline_string_xy.push_line_string(Some(line_string))?;
                    }
                    Dimensions::Xyz | Dimensions::Unknown(3) => {
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.mline_string_xyz.push_null());
                        self.deferred_nulls = 0;

                        self.mline_string_xyz.push_line_string(Some(line_string))?;
                    }
                    dim => {
                        return Err(GeoArrowError::General(format!(
                            "Unsupported dimension {dim:?}"
                        )));
                    }
                }
            } else {
                self.add_line_string_type(line_string.dim().try_into().unwrap());
                match line_string.dim() {
                    Dimensions::Xy | Dimensions::Unknown(2) => {
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.line_string_xy.push_null());
                        self.deferred_nulls = 0;

                        self.line_string_xy.push_line_string(Some(line_string))?;
                    }
                    Dimensions::Xyz | Dimensions::Unknown(3) => {
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.line_string_xyz.push_null());
                        self.deferred_nulls = 0;

                        self.line_string_xyz.push_line_string(Some(line_string))?;
                    }
                    dim => {
                        return Err(GeoArrowError::General(format!(
                            "Unsupported dimension {dim:?}"
                        )));
                    }
                }
            }
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    fn add_line_string_type(&mut self, dim: Dimension) {
        match dim {
            Dimension::XY => {
                self.offsets
                    .push(self.line_string_xy.len().try_into().unwrap());
                self.types.push(2)
            }
            Dimension::XYZ => {
                self.offsets
                    .push(self.line_string_xyz.len().try_into().unwrap());
                self.types.push(12)
            }
            _ => todo!("Handle M and ZM dimensions"),
        }
    }

    /// Add a new Polygon to the end of this array.
    ///
    /// If `self.prefer_multi` is `true`, it will be stored in the `MultiPolygonBuilder` child
    /// array. Otherwise, it will be stored in the `PolygonBuilder` child array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_polygon(&mut self, value: Option<&impl PolygonTrait<T = f64>>) -> Result<()> {
        if let Some(polygon) = value {
            if self.prefer_multi {
                self.add_multi_polygon_type(polygon.dim().try_into().unwrap());
                match polygon.dim() {
                    Dimensions::Xy | Dimensions::Unknown(2) => {
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.mpolygon_xy.push_null());
                        self.deferred_nulls = 0;

                        self.mpolygon_xy.push_polygon(Some(polygon))?;
                    }
                    Dimensions::Xyz | Dimensions::Unknown(3) => {
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.mpolygon_xyz.push_null());
                        self.deferred_nulls = 0;

                        self.mpolygon_xyz.push_polygon(Some(polygon))?;
                    }
                    dim => {
                        return Err(GeoArrowError::General(format!(
                            "Unsupported dimension {dim:?}"
                        )));
                    }
                }
            } else {
                self.add_polygon_type(polygon.dim().try_into().unwrap());
                match polygon.dim() {
                    Dimensions::Xy | Dimensions::Unknown(2) => {
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.polygon_xy.push_null());
                        self.deferred_nulls = 0;

                        self.polygon_xy.push_polygon(Some(polygon))?;
                    }
                    Dimensions::Xyz | Dimensions::Unknown(3) => {
                        // Flush deferred nulls
                        (0..self.deferred_nulls).for_each(|_| self.polygon_xyz.push_null());
                        self.deferred_nulls = 0;

                        self.polygon_xyz.push_polygon(Some(polygon))?;
                    }
                    dim => {
                        return Err(GeoArrowError::General(format!(
                            "Unsupported dimension {dim:?}"
                        )));
                    }
                }
            }
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    fn add_polygon_type(&mut self, dim: Dimension) {
        match dim {
            Dimension::XY => {
                self.offsets.push(self.polygon_xy.len().try_into().unwrap());
                self.types.push(3)
            }
            Dimension::XYZ => {
                self.offsets
                    .push(self.polygon_xyz.len().try_into().unwrap());
                self.types.push(13)
            }
            _ => todo!("Handle M and ZM dimensions"),
        }
    }

    /// Add a new MultiPoint to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_multi_point(
        &mut self,
        value: Option<&impl MultiPointTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(multi_point) = value {
            self.add_multi_point_type(multi_point.dim().try_into().unwrap());
            match multi_point.dim() {
                Dimensions::Xy | Dimensions::Unknown(2) => {
                    // Flush deferred nulls
                    (0..self.deferred_nulls).for_each(|_| self.mpoint_xy.push_null());
                    self.deferred_nulls = 0;

                    self.mpoint_xy.push_multi_point(Some(multi_point))?;
                }
                Dimensions::Xyz | Dimensions::Unknown(3) => {
                    // Flush deferred nulls
                    (0..self.deferred_nulls).for_each(|_| self.mpoint_xyz.push_null());
                    self.deferred_nulls = 0;

                    self.mpoint_xyz.push_multi_point(Some(multi_point))?;
                }
                dim => {
                    return Err(GeoArrowError::General(format!(
                        "Unsupported dimension {dim:?}"
                    )));
                }
            }
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    fn add_multi_point_type(&mut self, dim: Dimension) {
        match dim {
            Dimension::XY => {
                self.offsets.push(self.mpoint_xy.len().try_into().unwrap());
                self.types.push(4)
            }
            Dimension::XYZ => {
                self.offsets.push(self.mpoint_xyz.len().try_into().unwrap());
                self.types.push(14)
            }
            _ => todo!("Handle M and ZM dimensions"),
        }
    }

    /// Add a new MultiLineString to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_multi_line_string(
        &mut self,
        value: Option<&impl MultiLineStringTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(multi_line_string) = value {
            self.add_multi_line_string_type(multi_line_string.dim().try_into().unwrap());
            match multi_line_string.dim() {
                Dimensions::Xy | Dimensions::Unknown(2) => {
                    // Flush deferred nulls
                    (0..self.deferred_nulls).for_each(|_| self.mline_string_xy.push_null());
                    self.deferred_nulls = 0;

                    self.mline_string_xy
                        .push_multi_line_string(Some(multi_line_string))?;
                }
                Dimensions::Xyz | Dimensions::Unknown(3) => {
                    // Flush deferred nulls
                    (0..self.deferred_nulls).for_each(|_| self.mline_string_xyz.push_null());
                    self.deferred_nulls = 0;

                    self.mline_string_xyz
                        .push_multi_line_string(Some(multi_line_string))?;
                }
                dim => {
                    return Err(GeoArrowError::General(format!(
                        "Unsupported dimension {dim:?}"
                    )));
                }
            }
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    fn add_multi_line_string_type(&mut self, dim: Dimension) {
        match dim {
            Dimension::XY => {
                self.offsets
                    .push(self.mline_string_xy.len().try_into().unwrap());
                self.types.push(5)
            }
            Dimension::XYZ => {
                self.offsets
                    .push(self.mline_string_xyz.len().try_into().unwrap());
                self.types.push(15)
            }
            _ => todo!("Handle M and ZM dimensions"),
        }
    }

    /// Add a new MultiPolygon to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_multi_polygon(
        &mut self,
        value: Option<&impl MultiPolygonTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(multi_polygon) = value {
            self.add_multi_polygon_type(multi_polygon.dim().try_into().unwrap());
            match multi_polygon.dim() {
                Dimensions::Xy | Dimensions::Unknown(2) => {
                    // Flush deferred nulls
                    (0..self.deferred_nulls).for_each(|_| self.mpolygon_xy.push_null());
                    self.deferred_nulls = 0;

                    self.mpolygon_xy.push_multi_polygon(Some(multi_polygon))?;
                }
                Dimensions::Xyz | Dimensions::Unknown(3) => {
                    // Flush deferred nulls
                    (0..self.deferred_nulls).for_each(|_| self.mpolygon_xyz.push_null());
                    self.deferred_nulls = 0;

                    self.mpolygon_xyz.push_multi_polygon(Some(multi_polygon))?;
                }
                dim => {
                    return Err(GeoArrowError::General(format!(
                        "Unsupported dimension {dim:?}"
                    )));
                }
            }
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    fn add_multi_polygon_type(&mut self, dim: Dimension) {
        match dim {
            Dimension::XY => {
                self.offsets
                    .push(self.mpolygon_xy.len().try_into().unwrap());
                self.types.push(6)
            }
            Dimension::XYZ => {
                self.offsets
                    .push(self.mpolygon_xyz.len().try_into().unwrap());
                self.types.push(16)
            }
            _ => todo!("Handle M and ZM dimensions"),
        }
    }

    /// Add a new geometry to this builder
    #[inline]
    pub fn push_geometry(&mut self, value: Option<&'a impl GeometryTrait<T = f64>>) -> Result<()> {
        use geo_traits::GeometryType::*;

        if let Some(geom) = value {
            match geom.as_type() {
                Point(g) => {
                    self.push_point(Some(g))?;
                }
                LineString(g) => {
                    self.push_line_string(Some(g))?;
                }
                Polygon(g) => {
                    self.push_polygon(Some(g))?;
                }
                MultiPoint(p) => self.push_multi_point(Some(p))?,
                MultiLineString(p) => self.push_multi_line_string(Some(p))?,
                MultiPolygon(p) => self.push_multi_polygon(Some(p))?,
                GeometryCollection(gc) => {
                    if gc.num_geometries() == 1 {
                        self.push_geometry(Some(&gc.geometry(0).unwrap()))?
                    } else {
                        self.push_geometry_collection(Some(gc))?
                    }
                }
                Rect(_) | Triangle(_) | Line(_) => todo!(),
            };
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Add a new GeometryCollection to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_geometry_collection(
        &mut self,
        value: Option<&impl GeometryCollectionTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(gc) = value {
            self.add_geometry_collection_type(gc.dim().try_into().unwrap());

            match gc.dim() {
                Dimensions::Xy | Dimensions::Unknown(2) => {
                    // Flush deferred nulls
                    (0..self.deferred_nulls).for_each(|_| self.gc_xy.push_null());
                    self.deferred_nulls = 0;

                    self.gc_xy.push_geometry_collection(Some(gc))?;
                }
                Dimensions::Xyz | Dimensions::Unknown(3) => {
                    // Flush deferred nulls
                    (0..self.deferred_nulls).for_each(|_| self.gc_xyz.push_null());
                    self.deferred_nulls = 0;

                    self.gc_xyz.push_geometry_collection(Some(gc))?;
                }
                dim => {
                    return Err(GeoArrowError::General(format!(
                        "Unsupported dimension {dim:?}"
                    )));
                }
            }
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    fn add_geometry_collection_type(&mut self, dim: Dimension) {
        match dim {
            Dimension::XY => {
                self.offsets.push(self.gc_xy.len().try_into().unwrap());
                self.types.push(7)
            }
            Dimension::XYZ => {
                self.offsets.push(self.gc_xyz.len().try_into().unwrap());
                self.types.push(17)
            }
            _ => todo!("Handle M and ZM dimensions"),
        }
    }

    /// Push a null to this builder.
    ///
    /// Adding null values to a union array is tricky, because you don't want to add a null to a
    /// child that would otherwise be totally empty. Ideally, as few children as possible exist and
    /// are non-empty.
    ///
    /// We handle that by pushing nulls to the first non-empty child we find. If no underlying
    /// arrays are non-empty, we add to an internal counter instead. Once the first non-empty
    /// geometry is pushed, then we flush all the "deferred nulls" to that child.
    ///
    // TODO: test building an array of all nulls. Make sure we flush deferred nulls if we've never
    // added any valid geometries.
    #[inline]
    pub fn push_null(&mut self) {
        for arr in &mut self.points {
            if !arr.is_empty() {
                arr.push_null();
                return;
            }
        }
        for arr in &mut self.line_strings {
            if !arr.is_empty() {
                arr.push_null();
                return;
            }
        }
        for arr in &mut self.polygons {
            if !arr.is_empty() {
                arr.push_null();
                return;
            }
        }
        for arr in &mut self.mpoints {
            if !arr.is_empty() {
                arr.push_null();
                return;
            }
        }
        for arr in &mut self.mline_strings {
            if !arr.is_empty() {
                arr.push_null();
                return;
            }
        }
        for arr in &mut self.mpolygons {
            if !arr.is_empty() {
                arr.push_null();
                return;
            }
        }
        for arr in &mut self.gcs {
            if !arr.is_empty() {
                arr.push_null();
                return;
            }
        }

        self.deferred_nulls += 1;
    }

    /// Extend this builder with the given geometries
    pub fn extend_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_geom| self.push_geometry(maybe_geom))
            .unwrap();
    }

    /// Create this builder from a slice of Geometries.
    pub fn from_geometries(
        geoms: &[impl GeometryTrait<T = f64>],
        typ: GeometryType,
        prefer_multi: bool,
    ) -> Result<Self> {
        let mut array = Self::with_capacity_from_iter(geoms.iter().map(Some), typ, prefer_multi)?;
        array.extend_from_iter(geoms.iter().map(Some));
        Ok(array)
    }

    /// Create this builder from a slice of nullable Geometries.
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        typ: GeometryType,
        prefer_multi: bool,
    ) -> Result<Self> {
        let mut array =
            Self::with_capacity_from_iter(geoms.iter().map(|x| x.as_ref()), typ, prefer_multi)?;
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        Ok(array)
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WkbArray<O>, GeometryType)> for GeometryBuilder {
    type Error = GeoArrowError;

    fn try_from(
        (value, typ): (WkbArray<O>, GeometryType),
    ) -> std::result::Result<Self, Self::Error> {
        assert_eq!(
            value.nulls().map_or(0, |validity| validity.null_count()),
            0,
            "Parsing a WkbArray with null elements not supported",
        );

        let wkb_objects = value
            .iter()
            .map(|x| x.transpose())
            .collect::<Result<Vec<_>>>()?;
        Self::from_nullable_geometries(&wkb_objects, typ, DEFAULT_PREFER_MULTI)
    }
}

impl GeometryArrayBuilder for GeometryBuilder {
    fn len(&self) -> usize {
        self.types.len()
    }
}
