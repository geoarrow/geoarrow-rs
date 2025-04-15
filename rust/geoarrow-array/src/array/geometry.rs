use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, OffsetSizeTrait, UnionArray};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, UnionMode};
use geoarrow_schema::{
    CoordType, Dimension, GeometryCollectionType, GeometryType, LineStringType, Metadata,
    MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType,
};

use crate::array::*;
use crate::builder::*;
use crate::capacity::GeometryCapacity;
use crate::datatypes::GeoArrowType;
use crate::error::{GeoArrowError, Result};
use crate::scalar::Geometry;
use crate::trait_::{ArrayAccessor, GeoArrowArray, IntoArrow};

/// An immutable array of geometries of unknown geometry type and dimension.
///
// # Invariants
//
// - All arrays must have the same dimension
// - All arrays must have the same coordinate layout (interleaved or separated)
//
// - 1: Point
// - 2: LineString
// - 3: Polygon
// - 4: MultiPoint
// - 5: MultiLineString
// - 6: MultiPolygon
// - 7: GeometryCollection
// - 11: Point Z
// - 12: LineString Z
// - 13: Polygon Z
// - 14: MultiPoint Z
// - 15: MultiLineString Z
// - 16: MultiPolygon Z
// - 17: GeometryCollection Z
// - 21: Point M
// - 22: LineString M
// - 23: Polygon M
// - 24: MultiPoint M
// - 25: MultiLineString M
// - 26: MultiPolygon M
// - 27: GeometryCollection M
// - 31: Point ZM
// - 32: LineString ZM
// - 33: Polygon ZM
// - 34: MultiPoint ZM
// - 35: MultiLineString ZM
// - 36: MultiPolygon ZM
// - 37: GeometryCollection ZM
#[derive(Debug, Clone)]
pub struct GeometryArray {
    pub(crate) data_type: GeometryType,

    /// Invariant: every item in `type_ids` is `> 0 && < fields.len()` if `type_ids` are not
    /// provided. If `type_ids` exist in the NativeType, then every item in `type_ids` is `> 0 && `
    pub(crate) type_ids: ScalarBuffer<i8>,

    /// Invariant: `offsets.len() == type_ids.len()`
    pub(crate) offsets: ScalarBuffer<i32>,

    /// An array of PointArray, ordered XY, XYZ, XYM, XYZM
    pub(crate) points: [PointArray; 4],
    pub(crate) line_strings: [LineStringArray; 4],
    pub(crate) polygons: [PolygonArray; 4],
    pub(crate) mpoints: [MultiPointArray; 4],
    pub(crate) mline_strings: [MultiLineStringArray; 4],
    pub(crate) mpolygons: [MultiPolygonArray; 4],
    pub(crate) gcs: [GeometryCollectionArray; 4],
}

impl GeometryArray {
    /// Create a new GeometryArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Panics
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest geometry offset does not match the number of coordinates
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        type_ids: ScalarBuffer<i8>,
        offsets: ScalarBuffer<i32>,
        points: [PointArray; 4],
        line_strings: [LineStringArray; 4],
        polygons: [PolygonArray; 4],
        mpoints: [MultiPointArray; 4],
        mline_strings: [MultiLineStringArray; 4],
        mpolygons: [MultiPolygonArray; 4],
        gcs: [GeometryCollectionArray; 4],
        metadata: Arc<Metadata>,
    ) -> Self {
        // Validate that all arrays have the same coord type.
        let mut coord_types = HashSet::new();
        points.iter().for_each(|arr| {
            coord_types.insert(arr.data_type.coord_type());
        });
        line_strings.iter().for_each(|arr| {
            coord_types.insert(arr.data_type.coord_type());
        });
        polygons.iter().for_each(|arr| {
            coord_types.insert(arr.data_type.coord_type());
        });
        mpoints.iter().for_each(|arr| {
            coord_types.insert(arr.data_type.coord_type());
        });
        mline_strings.iter().for_each(|arr| {
            coord_types.insert(arr.data_type.coord_type());
        });
        mpolygons.iter().for_each(|arr| {
            coord_types.insert(arr.data_type.coord_type());
        });
        assert!(coord_types.len() <= 1);

        let coord_type = coord_types
            .into_iter()
            .next()
            .unwrap_or(CoordType::Interleaved);

        Self {
            data_type: GeometryType::new(coord_type, metadata),
            type_ids,
            offsets,
            points,
            line_strings,
            polygons,
            mpoints,
            mline_strings,
            mpolygons,
            gcs,
        }
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> GeometryCapacity {
        GeometryCapacity::new(
            0,
            core::array::from_fn(|i| self.points[i].buffer_lengths()),
            core::array::from_fn(|i| self.line_strings[i].buffer_lengths()),
            core::array::from_fn(|i| self.polygons[i].buffer_lengths()),
            core::array::from_fn(|i| self.mpoints[i].buffer_lengths()),
            core::array::from_fn(|i| self.mline_strings[i].buffer_lengths()),
            core::array::from_fn(|i| self.mpolygons[i].buffer_lengths()),
            core::array::from_fn(|i| self.gcs[i].buffer_lengths()),
            false,
        )
    }

    // TODO: handle slicing
    pub(crate) fn has_points(&self, dim: Dimension) -> bool {
        !self.points[dim.order()].is_empty()
    }

    pub(crate) fn has_line_strings(&self, dim: Dimension) -> bool {
        !self.line_strings[dim.order()].is_empty()
    }

    pub(crate) fn has_polygons(&self, dim: Dimension) -> bool {
        !self.polygons[dim.order()].is_empty()
    }

    pub(crate) fn has_multi_points(&self, dim: Dimension) -> bool {
        !self.mpoints[dim.order()].is_empty()
    }

    pub(crate) fn has_multi_line_strings(&self, dim: Dimension) -> bool {
        !self.mline_strings[dim.order()].is_empty()
    }

    pub(crate) fn has_multi_polygons(&self, dim: Dimension) -> bool {
        !self.mpolygons[dim.order()].is_empty()
    }

    #[allow(dead_code)]
    pub(crate) fn has_geometry_collections(&self, dim: Dimension) -> bool {
        !self.gcs[dim.order()].is_empty()
    }

    /// Return `true` if this array holds at least one non-empty array of the given dimension
    pub fn has_dimension(&self, dim: Dimension) -> bool {
        self.has_points(dim)
            || self.has_line_strings(dim)
            || self.has_polygons(dim)
            || self.has_multi_points(dim)
            || self.has_multi_line_strings(dim)
            || self.has_multi_polygons(dim)
    }

    /// Return `true` if this array holds at least one geometry array of the given dimension and no
    /// arrays of any other dimension.
    pub fn has_only_dimension(&self, dim: Dimension) -> bool {
        use Dimension::*;
        let existant_dims = [
            self.has_dimension(XY),
            self.has_dimension(XYZ),
            self.has_dimension(XYM),
            self.has_dimension(XYZM),
        ];
        existant_dims.iter().map(|b| *b as u8).sum::<u8>() == 1 && existant_dims[dim.order()]
    }

    // Handle sliced data before downcasting.
    // pub fn compact_children()

    // /// The number of non-empty child arrays
    // fn num_non_empty_children(&self) -> usize {
    //     let mut count = 0;

    //     if !self.point_xy.is_empty() {
    //         count += 1
    //     };
    //     if !self.line_string_xy.is_empty() {
    //         count += 1
    //     };
    //     if !self.polygon_xy.is_empty() {
    //         count += 1
    //     };
    //     if !self.mpoint_xy.is_empty() {
    //         count += 1
    //     };
    //     if !self.mline_string_xy.is_empty() {
    //         count += 1
    //     };
    //     if !self.mpolygon_xy.is_empty() {
    //         count += 1
    //     };

    //     if !self.point_xyz.is_empty() {
    //         count += 1
    //     };
    //     if !self.line_string_xyz.is_empty() {
    //         count += 1
    //     };
    //     if !self.polygon_xyz.is_empty() {
    //         count += 1
    //     };
    //     if !self.mpoint_xyz.is_empty() {
    //         count += 1
    //     };
    //     if !self.mline_string_xyz.is_empty() {
    //         count += 1
    //     };
    //     if !self.mpolygon_xyz.is_empty() {
    //         count += 1
    //     };

    //     count
    // }

    // TODO: restore to enable downcasting

    // pub fn has_only_type(&self, typ: NativeType) -> bool {
    //     use Dimension::*;

    //     if self.num_non_empty_children() == 0 {
    //         // Empty array
    //         false
    //     }

    //     if self.num_non_empty_children() > 1 {}

    //     match typ {
    //         NativeType::Point(_, dim)
    //     }

    //     self.has_points(XY)
    //         && !self.has_line_strings(XY)
    //         && !self.has_polygons(XY)
    //         && !self.has_multi_points(XY)
    //         && !self.has_multi_line_strings(XY)
    //         && !self.has_multi_polygons(XY)
    //         && !self.has_points(XYZ)
    //         && !self.has_line_strings(XYZ)
    //         && !self.has_polygons(XYZ)
    //         && !self.has_multi_points(XYZ)
    //         && !self.has_multi_line_strings(XYZ)
    //         && !self.has_multi_polygons(XYZ)
    // }

    // pub fn has_only_line_strings(&self) -> bool {
    //     !self.has_points()
    //         && self.has_line_strings()
    //         && !self.has_polygons()
    //         && !self.has_multi_points()
    //         && !self.has_multi_line_strings()
    //         && !self.has_multi_polygons()
    // }

    // pub fn has_only_polygons(&self) -> bool {
    //     !self.has_points()
    //         && !self.has_line_strings()
    //         && self.has_polygons()
    //         && !self.has_multi_points()
    //         && !self.has_multi_line_strings()
    //         && !self.has_multi_polygons()
    // }

    // pub fn has_only_multi_points(&self) -> bool {
    //     !self.has_points()
    //         && !self.has_line_strings()
    //         && !self.has_polygons()
    //         && self.has_multi_points()
    //         && !self.has_multi_line_strings()
    //         && !self.has_multi_polygons()
    // }

    // pub fn has_only_multi_line_strings(&self) -> bool {
    //     !self.has_points()
    //         && !self.has_line_strings()
    //         && !self.has_polygons()
    //         && !self.has_multi_points()
    //         && self.has_multi_line_strings()
    //         && !self.has_multi_polygons()
    // }

    // pub fn has_only_multi_polygons(&self) -> bool {
    //     !self.has_points()
    //         && !self.has_line_strings()
    //         && !self.has_polygons()
    //         && !self.has_multi_points()
    //         && !self.has_multi_line_strings()
    //         && self.has_multi_polygons()
    // }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        self.buffer_lengths().num_bytes()
    }

    /// Slices this [`MixedGeometryArray`] in place.
    ///
    /// # Implementation
    ///
    /// This operation is `O(F)` where `F` is the number of fields.
    ///
    /// # Panic
    ///
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        Self {
            data_type: self.data_type.clone(),
            type_ids: self.type_ids.slice(offset, length),
            offsets: self.offsets.slice(offset, length),

            points: self.points.clone(),
            line_strings: self.line_strings.clone(),
            polygons: self.polygons.clone(),
            mpoints: self.mpoints.clone(),
            mline_strings: self.mline_strings.clone(),
            mpolygons: self.mpolygons.clone(),
            gcs: self.gcs.clone(),
        }
    }

    // TODO: recursively expand the types from the geometry collection array
    #[allow(dead_code)]
    pub(crate) fn contained_types(&self) -> HashSet<GeoArrowType> {
        let mut types = HashSet::new();
        self.points.iter().for_each(|arr| {
            if !arr.is_empty() {
                types.insert(arr.data_type());
            }
        });
        self.line_strings.iter().for_each(|arr| {
            if !arr.is_empty() {
                types.insert(arr.data_type());
            }
        });
        self.polygons.iter().for_each(|arr| {
            if !arr.is_empty() {
                types.insert(arr.data_type());
            }
        });
        self.mpoints.iter().for_each(|arr| {
            if !arr.is_empty() {
                types.insert(arr.data_type());
            }
        });
        self.mline_strings.iter().for_each(|arr| {
            if !arr.is_empty() {
                types.insert(arr.data_type());
            }
        });
        self.mpolygons.iter().for_each(|arr| {
            if !arr.is_empty() {
                types.insert(arr.data_type());
            }
        });
        self.gcs.iter().for_each(|arr| {
            if !arr.is_empty() {
                types.insert(arr.data_type());
            }
        });

        types
    }
}

impl GeoArrowArray for GeometryArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn into_array_ref(self) -> ArrayRef {
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> ArrayRef {
        self.clone().into_array_ref()
    }

    #[inline]
    fn len(&self) -> usize {
        // Note that `type_ids` is sliced as usual, and thus always has the correct length.
        self.type_ids.len()
    }

    #[inline]
    fn nulls(&self) -> Option<&NullBuffer> {
        None
    }

    fn data_type(&self) -> GeoArrowType {
        GeoArrowType::Geometry(self.data_type.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }
}

impl<'a> ArrayAccessor<'a> for GeometryArray {
    type Item = Geometry<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> Result<Self::Item> {
        let type_id = self.type_ids[index];
        let offset = self.offsets[index] as usize;

        let dim = (type_id / 10) as usize;

        let result = match type_id % 10 {
            1 => Geometry::Point(self.points[dim].value(offset)?),
            2 => Geometry::LineString(self.line_strings[dim].value(offset)?),
            3 => Geometry::Polygon(self.polygons[dim].value(offset)?),
            4 => Geometry::MultiPoint(self.mpoints[dim].value(offset)?),
            5 => Geometry::MultiLineString(self.mline_strings[dim].value(offset)?),
            6 => Geometry::MultiPolygon(self.mpolygons[dim].value(offset)?),
            7 => Geometry::GeometryCollection(self.gcs[dim].value(offset)?),
            _ => panic!("unknown type_id {}", type_id),
        };
        Ok(result)
    }
}

impl IntoArrow for GeometryArray {
    type ArrowArray = UnionArray;
    type ExtensionType = GeometryType;

    fn into_arrow(self) -> Self::ArrowArray {
        let union_fields = match self.data_type.data_type() {
            DataType::Union(union_fields, _) => union_fields,
            _ => unreachable!(),
        };

        // https://stackoverflow.com/a/34406459/7319250
        let mut child_arrays: Vec<Option<ArrayRef>> = vec![None; 28];
        for (i, arr) in self.points.into_iter().enumerate() {
            child_arrays[i * 7] = Some(arr.into_array_ref());
        }
        for (i, arr) in self.line_strings.into_iter().enumerate() {
            child_arrays[i * 7 + 1] = Some(arr.into_array_ref());
        }
        for (i, arr) in self.polygons.into_iter().enumerate() {
            child_arrays[i * 7 + 2] = Some(arr.into_array_ref());
        }
        for (i, arr) in self.mpoints.into_iter().enumerate() {
            child_arrays[i * 7 + 3] = Some(arr.into_array_ref());
        }
        for (i, arr) in self.mline_strings.into_iter().enumerate() {
            child_arrays[i * 7 + 4] = Some(arr.into_array_ref());
        }
        for (i, arr) in self.mpolygons.into_iter().enumerate() {
            child_arrays[i * 7 + 5] = Some(arr.into_array_ref());
        }
        for (i, arr) in self.gcs.into_iter().enumerate() {
            child_arrays[i * 7 + 6] = Some(arr.into_array_ref());
        }

        UnionArray::try_new(
            union_fields,
            self.type_ids,
            Some(self.offsets),
            child_arrays.into_iter().map(|x| x.unwrap()).collect(),
        )
        .unwrap()
    }

    fn ext_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl TryFrom<(&UnionArray, GeometryType)> for GeometryArray {
    type Error = GeoArrowError;

    fn try_from(
        (value, typ): (&UnionArray, GeometryType),
    ) -> std::result::Result<Self, Self::Error> {
        let mut points: [Option<PointArray>; 4] = Default::default();
        let mut line_strings: [Option<LineStringArray>; 4] = Default::default();
        let mut polygons: [Option<PolygonArray>; 4] = Default::default();
        let mut mpoints: [Option<MultiPointArray>; 4] = Default::default();
        let mut mline_strings: [Option<MultiLineStringArray>; 4] = Default::default();
        let mut mpolygons: [Option<MultiPolygonArray>; 4] = Default::default();
        let mut gcs: [Option<GeometryCollectionArray>; 4] = Default::default();

        let coord_type = typ.coord_type();

        match value.data_type() {
            DataType::Union(fields, mode) => {
                if !matches!(mode, UnionMode::Dense) {
                    return Err(GeoArrowError::General("Expected dense union".to_string()));
                }

                for (type_id, _field) in fields.iter() {
                    let dim = Dimension::from_order((type_id / 10) as _);
                    let index = dim.order();

                    match type_id % 10 {
                        1 => {
                            points[index] = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    PointType::new(coord_type, dim, Default::default()),
                                )
                                    .try_into()?,
                            );
                        }
                        2 => {
                            line_strings[index] = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    LineStringType::new(coord_type, dim, Default::default()),
                                )
                                    .try_into()?,
                            );
                        }
                        3 => {
                            polygons[index] = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    PolygonType::new(coord_type, dim, Default::default()),
                                )
                                    .try_into()?,
                            );
                        }
                        4 => {
                            mpoints[index] = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    MultiPointType::new(coord_type, dim, Default::default()),
                                )
                                    .try_into()?,
                            );
                        }
                        5 => {
                            mline_strings[index] = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    MultiLineStringType::new(coord_type, dim, Default::default()),
                                )
                                    .try_into()?,
                            );
                        }
                        6 => {
                            mpolygons[index] = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    MultiPolygonType::new(coord_type, dim, Default::default()),
                                )
                                    .try_into()?,
                            );
                        }
                        7 => {
                            gcs[index] = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    GeometryCollectionType::new(
                                        coord_type,
                                        dim,
                                        Default::default(),
                                    ),
                                )
                                    .try_into()?,
                            );
                        }
                        _ => {
                            return Err(GeoArrowError::General(format!(
                                "Unexpected type_id {}",
                                type_id
                            )));
                        }
                    }
                }
            }
            _ => panic!("expected union type"),
        };

        let type_ids = value.type_ids().clone();
        // This is after checking for dense union
        let offsets = value.offsets().unwrap().clone();

        // We need to convert the array [Option<PointArray>; 4] into `[PointArray; 4]`.
        // But we also need to ensure the underlying PointArray has the correct `Dimension` for the
        // given array index.
        // In order to do this, we need the index of the array, which `map` doesn't give us. And
        // using `core::array::from_fn` doesn't let us move out of the existing array.
        // So we mutate the existing array of `[Option<PointArray>; 4]` to ensure all values are
        // `Some`, and then later we call `unwrap` on all array values in a `map`.
        points.iter_mut().enumerate().for_each(|(i, arr)| {
            let new_val = if let Some(arr) = arr.take() {
                arr
            } else {
                PointBuilder::new(PointType::new(
                    coord_type,
                    Dimension::from_order(i),
                    Default::default(),
                ))
                .finish()
            };
            arr.replace(new_val);
        });
        line_strings.iter_mut().enumerate().for_each(|(i, arr)| {
            let new_val = if let Some(arr) = arr.take() {
                arr
            } else {
                LineStringBuilder::new(LineStringType::new(
                    coord_type,
                    Dimension::from_order(i),
                    Default::default(),
                ))
                .finish()
            };
            arr.replace(new_val);
        });
        polygons.iter_mut().enumerate().for_each(|(i, arr)| {
            let new_val = if let Some(arr) = arr.take() {
                arr
            } else {
                PolygonBuilder::new(PolygonType::new(
                    coord_type,
                    Dimension::from_order(i),
                    Default::default(),
                ))
                .finish()
            };
            arr.replace(new_val);
        });
        mpoints.iter_mut().enumerate().for_each(|(i, arr)| {
            let new_val = if let Some(arr) = arr.take() {
                arr
            } else {
                MultiPointBuilder::new(MultiPointType::new(
                    coord_type,
                    Dimension::from_order(i),
                    Default::default(),
                ))
                .finish()
            };
            arr.replace(new_val);
        });
        mline_strings.iter_mut().enumerate().for_each(|(i, arr)| {
            let new_val = if let Some(arr) = arr.take() {
                arr
            } else {
                MultiLineStringBuilder::new(MultiLineStringType::new(
                    coord_type,
                    Dimension::from_order(i),
                    Default::default(),
                ))
                .finish()
            };
            arr.replace(new_val);
        });
        mpolygons.iter_mut().enumerate().for_each(|(i, arr)| {
            let new_val = if let Some(arr) = arr.take() {
                arr
            } else {
                MultiPolygonBuilder::new(MultiPolygonType::new(
                    coord_type,
                    Dimension::from_order(i),
                    Default::default(),
                ))
                .finish()
            };
            arr.replace(new_val);
        });
        gcs.iter_mut().enumerate().for_each(|(i, arr)| {
            let new_val = if let Some(arr) = arr.take() {
                arr
            } else {
                GeometryCollectionBuilder::new(
                    GeometryCollectionType::new(
                        coord_type,
                        Dimension::from_order(i),
                        Default::default(),
                    ),
                    false,
                )
                .finish()
            };
            arr.replace(new_val);
        });

        Ok(Self::new(
            type_ids,
            offsets,
            points.map(|x| x.unwrap()),
            line_strings.map(|x| x.unwrap()),
            polygons.map(|x| x.unwrap()),
            mpoints.map(|x| x.unwrap()),
            mline_strings.map(|x| x.unwrap()),
            mpolygons.map(|x| x.unwrap()),
            gcs.map(|x| x.unwrap()),
            Default::default(),
        ))
    }
}

impl TryFrom<(&dyn Array, GeometryType)> for GeometryArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, GeometryType)) -> Result<Self> {
        match value.data_type() {
            DataType::Union(_, _) => (value.as_union(), typ).try_into(),
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for GeometryArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field.try_extension_type::<GeometryType>()?;
        (arr, typ).try_into()
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WkbArray<O>, GeometryType)> for GeometryArray {
    type Error = GeoArrowError;

    fn try_from(value: (WkbArray<O>, GeometryType)) -> Result<Self> {
        let mut_arr: GeometryBuilder = value.try_into()?;
        Ok(mut_arr.finish())
    }
}

pub(crate) trait DimensionIndex {
    /// Get the positional index of the internal array for the given dimension.
    fn order(&self) -> usize;

    fn from_order(index: usize) -> Self;
}

impl DimensionIndex for Dimension {
    fn order(&self) -> usize {
        match self {
            Self::XY => 0,
            Self::XYZ => 1,
            Self::XYM => 2,
            Self::XYZM => 3,
        }
    }

    fn from_order(index: usize) -> Self {
        match index {
            0 => Self::XY,
            1 => Self::XYZ,
            2 => Self::XYM,
            3 => Self::XYZM,
            _ => panic!("unsupported index in from_order"),
        }
    }
}

impl DimensionIndex for geo_traits::Dimensions {
    fn order(&self) -> usize {
        match self {
            Self::Xy => 0,
            Self::Xyz => 1,
            Self::Xym => 2,
            Self::Xyzm => 3,
            Self::Unknown(_) => panic!("Unsupported DimensionIndex with unknown dimension"),
        }
    }

    fn from_order(index: usize) -> Self {
        match index {
            0 => Self::Xy,
            1 => Self::Xyz,
            2 => Self::Xym,
            3 => Self::Xyzm,
            _ => panic!("unsupported index in from_order"),
        }
    }
}

impl PartialEq for GeometryArray {
    fn eq(&self, other: &Self) -> bool {
        self.type_ids == other.type_ids
            && self.offsets == other.offsets
            && self.points == other.points
            && self.line_strings == other.line_strings
            && self.polygons == other.polygons
            && self.mpoints == other.mpoints
            && self.mline_strings == other.mline_strings
            && self.mpolygons == other.mpolygons
            && self.gcs == other.gcs
    }
}

#[cfg(test)]
mod test {
    use geo_traits::to_geo::ToGeoGeometry;

    use super::*;
    use crate::test::{linestring, multilinestring, multipoint, multipolygon, point, polygon};

    fn geoms() -> Vec<geo_types::Geometry> {
        vec![
            point::p0().into(),
            point::p1().into(),
            point::p2().into(),
            linestring::ls0().into(),
            linestring::ls1().into(),
            polygon::p0().into(),
            polygon::p1().into(),
            multipoint::mp0().into(),
            multipoint::mp1().into(),
            multilinestring::ml0().into(),
            multilinestring::ml1().into(),
            multipolygon::mp0().into(),
            multipolygon::mp1().into(),
        ]
    }

    fn geom_array(coord_type: CoordType) -> GeometryArray {
        let geoms = geoms();
        let typ = GeometryType::new(coord_type, Default::default());
        GeometryBuilder::from_geometries(&geoms, typ, false)
            .unwrap()
            .finish()
    }

    #[test]
    fn test_2d() {
        for coord_type in [CoordType::Interleaved, CoordType::Separated] {
            let geoms = geoms();
            let geometry_array = geom_array(coord_type);
            let geoms_again = geometry_array
                .iter_values()
                .map(|g| g.unwrap().to_geometry())
                .collect::<Vec<_>>();
            assert_eq!(geoms, geoms_again);
        }
    }

    #[test]
    fn test_2d_roundtrip_arrow() {
        for coord_type in [CoordType::Interleaved, CoordType::Separated] {
            let geoms = geoms();
            let geometry_array = geom_array(coord_type);
            let field = geometry_array.data_type.to_field("geometry", true);
            let union_array = geometry_array.into_arrow();

            let geometry_array_again =
                GeometryArray::try_from((&union_array as _, &field)).unwrap();
            let geoms_again = geometry_array_again
                .iter_values()
                .map(|g| g.unwrap().to_geometry())
                .collect::<Vec<_>>();
            assert_eq!(geoms, geoms_again);
        }
    }

    #[test]
    fn try_from_arrow() {
        for coord_type in [CoordType::Interleaved, CoordType::Separated] {
            for prefer_multi in [true, false] {
                let geo_arr = crate::test::geometry::array(coord_type, prefer_multi);

                let point_type = geo_arr.ext_type().clone();
                let field = point_type.to_field("geometry", true);

                let arrow_arr = geo_arr.to_array_ref();

                let geo_arr2: GeometryArray = (arrow_arr.as_ref(), point_type).try_into().unwrap();
                let geo_arr3: GeometryArray = (arrow_arr.as_ref(), &field).try_into().unwrap();

                assert_eq!(geo_arr, geo_arr2);
                assert_eq!(geo_arr, geo_arr3);
            }
        }
    }

    #[test]
    fn partial_eq() {
        for prefer_multi in [true, false] {
            let arr1 = crate::test::geometry::array(CoordType::Interleaved, prefer_multi);
            let arr2 = crate::test::geometry::array(CoordType::Separated, prefer_multi);

            assert_eq!(arr1, arr1);
            assert_eq!(arr2, arr2);
            assert_eq!(arr1, arr2);

            assert_ne!(arr1, arr2.slice(0, 2));
        }
    }
}

// #[cfg(test)]
// mod test {
//     use super::*;
//     use crate::test::{linestring, multilinestring, multipoint, multipolygon, point, polygon};

//     #[test]
//     fn geo_roundtrip_accurate_points() {
//         let geoms: Vec<geo::Geometry> = vec![
//             geo::Geometry::Point(point::p0()),
//             geo::Geometry::Point(point::p1()),
//             geo::Geometry::Point(point::p2()),
//         ];

//         let arr: GeometryArray = GeometryBuilder::from_geometries(
//             geoms.as_slice(),
//             CoordType::Interleaved,
//             Default::default(),
//             false,
//         )
//         .unwrap()
//         .finish();

//         assert_eq!(arr.value_as_geo(0), geo::Geometry::Point(point::p0()));
//         assert_eq!(arr.value_as_geo(1), geo::Geometry::Point(point::p1()));
//         assert_eq!(arr.value_as_geo(2), geo::Geometry::Point(point::p2()));
//     }

//     #[test]
//     fn geo_roundtrip_accurate_multi_points() {
//         let geoms: Vec<geo::Geometry> = vec![
//             geo::Geometry::Point(point::p0()),
//             geo::Geometry::Point(point::p1()),
//             geo::Geometry::Point(point::p2()),
//         ];
//         let arr: GeometryArray = GeometryBuilder::from_geometries(
//             geoms.as_slice(),
//             CoordType::Interleaved,
//             Default::default(),
//             true,
//         )
//         .unwrap()
//         .finish();

//         assert_eq!(
//             arr.value_as_geo(0),
//             geo::Geometry::MultiPoint(geo::MultiPoint(vec![point::p0()]))
//         );
//         assert_eq!(
//             arr.value_as_geo(1),
//             geo::Geometry::MultiPoint(geo::MultiPoint(vec![point::p1()]))
//         );
//         assert_eq!(
//             arr.value_as_geo(2),
//             geo::Geometry::MultiPoint(geo::MultiPoint(vec![point::p2()]))
//         );
//     }

//     #[test]
//     fn geo_roundtrip_accurate_all() {
//         let geoms: Vec<geo::Geometry> = vec![
//             geo::Geometry::Point(point::p0()),
//             geo::Geometry::LineString(linestring::ls0()),
//             geo::Geometry::Polygon(polygon::p0()),
//             geo::Geometry::MultiPoint(multipoint::mp0()),
//             geo::Geometry::MultiLineString(multilinestring::ml0()),
//             geo::Geometry::MultiPolygon(multipolygon::mp0()),
//         ];

//         let arr: GeometryArray = GeometryBuilder::from_geometries(
//             geoms.as_slice(),
//             CoordType::Interleaved,
//             Default::default(),
//             false,
//         )
//         .unwrap()
//         .finish();

//         assert_eq!(arr.value_as_geo(0), geoms[0]);
//         assert_eq!(arr.value_as_geo(1), geoms[1]);
//         assert_eq!(arr.value_as_geo(2), geoms[2]);
//         assert_eq!(arr.value_as_geo(3), geoms[3]);
//         assert_eq!(arr.value_as_geo(4), geoms[4]);
//         assert_eq!(arr.value_as_geo(5), geoms[5]);
//     }

//     #[test]
//     fn arrow_roundtrip() {
//         let geoms: Vec<geo::Geometry> = vec![
//             geo::Geometry::Point(point::p0()),
//             geo::Geometry::LineString(linestring::ls0()),
//             geo::Geometry::Polygon(polygon::p0()),
//             geo::Geometry::MultiPoint(multipoint::mp0()),
//             geo::Geometry::MultiLineString(multilinestring::ml0()),
//             geo::Geometry::MultiPolygon(multipolygon::mp0()),
//         ];

//         let arr: GeometryArray = GeometryBuilder::from_geometries(
//             geoms.as_slice(),
//             CoordType::Interleaved,
//             Default::default(),
//             false,
//         )
//         .unwrap()
//         .finish();

//         // Round trip to/from arrow-rs
//         let arrow_array = arr.into_arrow();
//         let round_trip_arr: GeometryArray = (&arrow_array).try_into().unwrap();

//         assert_eq!(round_trip_arr.value_as_geo(0), geoms[0]);
//         assert_eq!(round_trip_arr.value_as_geo(1), geoms[1]);
//         assert_eq!(round_trip_arr.value_as_geo(2), geoms[2]);
//         assert_eq!(round_trip_arr.value_as_geo(3), geoms[3]);
//         assert_eq!(round_trip_arr.value_as_geo(4), geoms[4]);
//         assert_eq!(round_trip_arr.value_as_geo(5), geoms[5]);
//     }

//     #[test]
//     fn arrow_roundtrip_not_all_types() {
//         let geoms: Vec<geo::Geometry> = vec![
//             geo::Geometry::MultiPoint(multipoint::mp0()),
//             geo::Geometry::MultiLineString(multilinestring::ml0()),
//             geo::Geometry::MultiPolygon(multipolygon::mp0()),
//         ];

//         let arr: GeometryArray = GeometryBuilder::from_geometries(
//             geoms.as_slice(),
//             CoordType::Interleaved,
//             Default::default(),
//             false,
//         )
//         .unwrap()
//         .finish();

//         // Round trip to/from arrow-rs
//         let arrow_array = arr.into_arrow();
//         let round_trip_arr: GeometryArray = (&arrow_array).try_into().unwrap();

//         assert_eq!(round_trip_arr.value_as_geo(0), geoms[0]);
//         assert_eq!(round_trip_arr.value_as_geo(1), geoms[1]);
//         assert_eq!(round_trip_arr.value_as_geo(2), geoms[2]);
//     }

//     #[test]
//     fn arrow_roundtrip_not_all_types2() {
//         let geoms: Vec<geo::Geometry> = vec![
//             geo::Geometry::MultiPoint(multipoint::mp0()),
//             geo::Geometry::MultiPolygon(multipolygon::mp0()),
//         ];

//         let arr: GeometryArray = GeometryBuilder::from_geometries(
//             geoms.as_slice(),
//             CoordType::Interleaved,
//             Default::default(),
//             false,
//         )
//         .unwrap()
//         .finish();

//         // Round trip to/from arrow-rs
//         let arrow_array = arr.into_arrow();
//         let round_trip_arr: GeometryArray = (&arrow_array).try_into().unwrap();

//         assert_eq!(round_trip_arr.value_as_geo(0), geoms[0]);
//         assert_eq!(round_trip_arr.value_as_geo(1), geoms[1]);
//     }

//     #[test]
//     fn test_slicing() {
//         let geoms: Vec<geo::Geometry> = vec![
//             geo::Geometry::Point(point::p0()),
//             geo::Geometry::LineString(linestring::ls0()),
//             geo::Geometry::Polygon(polygon::p0()),
//             geo::Geometry::MultiPoint(multipoint::mp0()),
//             geo::Geometry::MultiLineString(multilinestring::ml0()),
//             geo::Geometry::MultiPolygon(multipolygon::mp0()),
//         ];

//         let arr: GeometryArray = GeometryBuilder::from_geometries(
//             geoms.as_slice(),
//             CoordType::Interleaved,
//             Default::default(),
//             false,
//         )
//         .unwrap()
//         .finish();

//         assert_eq!(arr.slice(1, 2).value_as_geo(0), geoms[1]);
//         assert_eq!(arr.slice(1, 2).value_as_geo(1), geoms[2]);
//         assert_eq!(arr.slice(3, 3).value_as_geo(2), geoms[5]);
//     }
// }
