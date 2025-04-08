#![allow(unused_variables)]

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use geoarrow_schema::{
    CoordType, Dimension, GeometryCollectionType, GeometryType, MultiLineStringType,
    MultiPointType, MultiPolygonType,
};

use crate::NativeArray;
use crate::algorithm::native::cast::Cast;
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::NativeType;
use crate::error::Result;
use crate::schema::GeoSchemaExt;
use crate::table::Table;

/// Downcast will change between geometry types but will not affect the dimension of the data.
///
/// Downcast will not change the coordinate type of the data.
pub trait Downcast {
    type Output;

    /// The data type that downcasting would result in.
    fn downcasted_data_type(&self) -> NativeType;

    /// If possible, convert this array to a simpler and/or smaller data type
    ///
    /// Conversions include:
    ///
    /// - MultiPoint -> Point
    /// - MultiLineString -> LineString
    /// - MultiPolygon -> Polygon
    /// - MixedGeometry -> any of the 6 concrete types
    /// - GeometryCollection -> MixedGeometry or any of the 6 concrete types
    ///
    fn downcast(&self) -> Self::Output;
}

impl Downcast for PointArray {
    type Output = Arc<dyn NativeArray>;

    fn downcasted_data_type(&self) -> NativeType {
        self.data_type()
    }

    fn downcast(&self) -> Self::Output {
        Arc::new(self.clone())
    }
}

/// Returns `true` if this Multi-geometry array can fit into a non-multi array
///
/// Note that we can't just check the value of the last offset, because there could be a null
/// element with length 0 and then a multi point of length 2. We need to check that every offset is
/// <= 1.
///
/// Also note that for now, we explicitly check `== 1` instead of `<= 1`. Having an offset of
/// length 0 means that the geometry is empty, and the cast functionality would need to handle
/// that.
pub(crate) fn can_downcast_multi<O: OffsetSizeTrait>(buffer: &OffsetBuffer<O>) -> bool {
    buffer
        .windows(2)
        .all(|slice| *slice.get(1).unwrap() - *slice.first().unwrap() == O::one())
}

impl Downcast for LineStringArray {
    type Output = Arc<dyn NativeArray>;

    fn downcasted_data_type(&self) -> NativeType {
        self.data_type()
    }

    fn downcast(&self) -> Self::Output {
        Arc::new(self.clone())
    }
}

impl Downcast for PolygonArray {
    type Output = Arc<dyn NativeArray>;

    fn downcasted_data_type(&self) -> NativeType {
        self.data_type()
    }

    fn downcast(&self) -> Self::Output {
        Arc::new(self.clone())
    }
}

impl Downcast for MultiPointArray {
    type Output = Arc<dyn NativeArray>;

    fn downcasted_data_type(&self) -> NativeType {
        todo!("downcast");
        // match self.data_type() {
        //     NativeType::MultiPoint(ct, dim) => {
        //         if can_downcast_multi(&self.geom_offsets) {
        //             NativeType::Point(ct, dim)
        //         } else {
        //             NativeType::MultiPoint(ct, dim)
        //         }
        //     }
        //     _ => unreachable!(),
        // }
    }
    fn downcast(&self) -> Self::Output {
        if let Ok(array) = PointArray::try_from(self.clone()) {
            Arc::new(array)
        } else {
            Arc::new(self.clone())
        }
    }
}

impl Downcast for MultiLineStringArray {
    type Output = Arc<dyn NativeArray>;

    fn downcasted_data_type(&self) -> NativeType {
        todo!("downcast");

        // match self.data_type() {
        //     NativeType::MultiLineString(ct, dim) => {
        //         if can_downcast_multi(&self.geom_offsets) {
        //             NativeType::LineString(ct, dim)
        //         } else {
        //             NativeType::MultiLineString(ct, dim)
        //         }
        //     }
        //     _ => unreachable!(),
        // }
    }

    fn downcast(&self) -> Self::Output {
        if let Ok(array) = LineStringArray::try_from(self.clone()) {
            Arc::new(array)
        } else {
            Arc::new(self.clone())
        }
    }
}

impl Downcast for MultiPolygonArray {
    type Output = Arc<dyn NativeArray>;

    fn downcasted_data_type(&self) -> NativeType {
        todo!("downcast");

        // match self.data_type() {
        //     NativeType::MultiPolygon(ct, dim) => {
        //         if can_downcast_multi(&self.geom_offsets) {
        //             NativeType::Polygon(ct, dim)
        //         } else {
        //             NativeType::MultiPolygon(ct, dim)
        //         }
        //     }
        //     _ => unreachable!(),
        // }
    }

    fn downcast(&self) -> Self::Output {
        if let Ok(array) = PolygonArray::try_from(self.clone()) {
            Arc::new(array)
        } else {
            Arc::new(self.clone())
        }
    }
}

// Note: this will not downcast on sliced data when it otherwise could, because the children
// haven't been sliced, just the offsets. So it still looks like the children have data.
impl Downcast for MixedGeometryArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn downcasted_data_type(&self) -> NativeType {
        let types = self.contained_types();
        if types.len() == 1 {
            let typ = types.iter().next().unwrap().clone();

            // Only has non-multi geometry children
            if matches!(typ, NativeType::Point(_))
                || matches!(typ, NativeType::LineString(_))
                || matches!(typ, NativeType::Polygon(_))
            {
                return typ;
            }
        }

        // Whether or not we have the single-geom type, if we only otherwise have the multi-geom
        // type, then we can downcast if we can downcast the multi-geom type.
        if !self.has_line_strings()
            && !self.has_polygons()
            && self.has_multi_points()
            && !self.has_multi_line_strings()
            && !self.has_multi_polygons()
        {
            return self.multi_points.downcasted_data_type();
        }

        if !self.has_points()
            && !self.has_polygons()
            && !self.has_multi_points()
            && self.has_multi_line_strings()
            && !self.has_multi_polygons()
        {
            return self.multi_line_strings.downcasted_data_type();
        }

        if !self.has_points()
            && !self.has_line_strings()
            && !self.has_multi_points()
            && !self.has_multi_line_strings()
            && self.has_multi_polygons()
        {
            return self.multi_polygons.downcasted_data_type();
        }

        self.data_type()
    }

    fn downcast(&self) -> Self::Output {
        self.cast(self.downcasted_data_type())
    }
}

impl Downcast for GeometryCollectionArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn downcasted_data_type(&self) -> NativeType {
        // TODO: support downcasting with null elements
        if can_downcast_multi(&self.geom_offsets) && self.null_count() == 0 {
            self.array.downcasted_data_type()
        } else {
            self.data_type()
        }
    }

    fn downcast(&self) -> Self::Output {
        self.cast(self.downcasted_data_type())
    }
}

impl Downcast for RectArray {
    type Output = Arc<dyn NativeArray>;

    fn downcasted_data_type(&self) -> NativeType {
        self.data_type()
    }

    fn downcast(&self) -> Self::Output {
        Arc::new(self.clone())
    }
}

impl Downcast for GeometryArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn downcasted_data_type(&self) -> NativeType {
        if let Ok(mixed_array) = MixedGeometryArray::try_from(self.clone()) {
            mixed_array.downcasted_data_type()
        } else {
            self.data_type()
        }
    }

    fn downcast(&self) -> Self::Output {
        self.cast(self.downcasted_data_type())
    }
}

impl Downcast for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn downcasted_data_type(&self) -> NativeType {
        use NativeType::*;

        match self.data_type() {
            Point(_) => self.as_point().downcasted_data_type(),
            LineString(_) => self.as_line_string().downcasted_data_type(),
            Polygon(_) => self.as_polygon().downcasted_data_type(),
            MultiPoint(_) => self.as_multi_point().downcasted_data_type(),
            MultiLineString(_) => self.as_multi_line_string().downcasted_data_type(),
            MultiPolygon(_) => self.as_multi_polygon().downcasted_data_type(),
            GeometryCollection(_) => self.as_geometry_collection().downcasted_data_type(),
            Rect(_) => self.as_rect().downcasted_data_type(),
            Geometry(_) => self.as_geometry().downcasted_data_type(),
        }
    }

    fn downcast(&self) -> Self::Output {
        use NativeType::*;

        match self.data_type() {
            Point(_) => Ok(self.as_point().downcast()),
            LineString(_) => Ok(self.as_line_string().downcast()),
            Polygon(_) => Ok(self.as_polygon().downcast()),
            MultiPoint(_) => Ok(self.as_multi_point().downcast()),
            MultiLineString(_) => Ok(self.as_multi_line_string().downcast()),
            MultiPolygon(_) => Ok(self.as_multi_polygon().downcast()),
            GeometryCollection(_) => self.as_geometry_collection().downcast(),
            Rect(_) => Ok(self.as_rect().downcast()),
            Geometry(_) => self.as_geometry().downcast(),
        }
    }
}

/// Given a set of types, return a single type that the result should be casted to
fn resolve_types(types: &HashSet<NativeType>) -> NativeType {
    if types.is_empty() {
        // TODO: error here
        panic!("empty types");
    }

    // If only one type, we can cast to that.
    if types.len() == 1 {
        return types.iter().next().unwrap().clone();
    }

    // If Geometry is in the type set, short circuit to that.
    if types.contains(&NativeType::Geometry(GeometryType::new(
        CoordType::Interleaved,
        Default::default(),
    ))) {
        return NativeType::Geometry(GeometryType::new(
            CoordType::Interleaved,
            Default::default(),
        ));
    } else if types.contains(&NativeType::Geometry(GeometryType::new(
        CoordType::Separated,
        Default::default(),
    ))) {
        return NativeType::Geometry(GeometryType::new(CoordType::Separated, Default::default()));
    }

    // Since we don't have NativeType::Geometry, dimension should never be null
    let dimensions: HashSet<Dimension> =
        HashSet::from_iter(types.iter().map(|ty| ty.dimension().unwrap()));
    let coord_types: HashSet<CoordType> =
        HashSet::from_iter(types.iter().map(|ty| ty.coord_type()));

    // Just take the first one
    let coord_type = *coord_types.iter().next().unwrap();

    // For data with multiple dimensions, we must cast to GeometryArray
    if dimensions.len() > 1 {
        return NativeType::Geometry(GeometryType::new(coord_type, Default::default()));
    }
    // Otherwise, we have just one dimension
    let dimension = *dimensions.iter().next().unwrap();

    // We want to compare geometry types without looking at dimension or coord type. This is a
    // slight hack but for now we do that by the string geometry type.
    let geometry_type_names: HashSet<&str> =
        HashSet::from_iter(types.iter().map(|x| x.extension_name()));

    if geometry_type_names.len() == 2 {
        if geometry_type_names.contains("geoarrow.point")
            && geometry_type_names.contains("geoarrow.multipoint")
        {
            return NativeType::MultiPoint(MultiPointType::new(
                coord_type,
                dimension,
                Default::default(),
            ));
        } else if geometry_type_names.contains("geoarrow.linestring")
            && geometry_type_names.contains("geoarrow.multilinestring")
        {
            return NativeType::MultiLineString(MultiLineStringType::new(
                coord_type,
                dimension,
                Default::default(),
            ));
        } else if geometry_type_names.contains("geoarrow.polygon")
            && geometry_type_names.contains("geoarrow.multipolygon")
        {
            return NativeType::MultiPolygon(MultiPolygonType::new(
                coord_type,
                dimension,
                Default::default(),
            ));
        } else if geometry_type_names.contains("geoarrow.geometrycollection") {
            return NativeType::GeometryCollection(GeometryCollectionType::new(
                coord_type,
                dimension,
                Default::default(),
            ));
        }
    }

    NativeType::Geometry(GeometryType::new(coord_type, Default::default()))
}

impl Downcast for ChunkedPointArray {
    type Output = Arc<dyn ChunkedNativeArray>;

    fn downcasted_data_type(&self) -> NativeType {
        self.data_type()
    }
    fn downcast(&self) -> Self::Output {
        Arc::new(self.clone())
    }
}

macro_rules! impl_chunked_downcast {
    ($chunked_array:ty) => {
        impl Downcast for $chunked_array {
            type Output = Arc<dyn ChunkedNativeArray>;

            fn downcasted_data_type(&self) -> NativeType {
                let mut types = HashSet::new();
                self.chunks.iter().for_each(|chunk| {
                    types.insert(chunk.downcasted_data_type());
                });
                resolve_types(&types)
            }
            fn downcast(&self) -> Self::Output {
                let to_data_type = self.downcasted_data_type();

                if to_data_type == self.data_type() {
                    return Arc::new(self.clone());
                }

                self.cast(to_data_type).unwrap()
            }
        }
    };
}

impl_chunked_downcast!(ChunkedLineStringArray);
impl_chunked_downcast!(ChunkedPolygonArray);
impl_chunked_downcast!(ChunkedMultiPointArray);
impl_chunked_downcast!(ChunkedMultiLineStringArray);
impl_chunked_downcast!(ChunkedMultiPolygonArray);
impl_chunked_downcast!(ChunkedMixedGeometryArray);
impl_chunked_downcast!(ChunkedGeometryCollectionArray);
impl_chunked_downcast!(ChunkedUnknownGeometryArray);

impl Downcast for ChunkedRectArray {
    type Output = Arc<dyn ChunkedNativeArray>;

    fn downcasted_data_type(&self) -> NativeType {
        self.data_type()
    }
    fn downcast(&self) -> Self::Output {
        Arc::new(self.clone())
    }
}

impl Downcast for &dyn ChunkedNativeArray {
    type Output = Arc<dyn ChunkedNativeArray>;

    fn downcasted_data_type(&self) -> NativeType {
        use NativeType::*;

        match self.data_type() {
            Point(_) => self.as_point().downcasted_data_type(),
            LineString(_) => self.as_line_string().downcasted_data_type(),
            Polygon(_) => self.as_polygon().downcasted_data_type(),
            MultiPoint(_) => self.as_multi_point().downcasted_data_type(),
            MultiLineString(_) => self.as_multi_line_string().downcasted_data_type(),
            MultiPolygon(_) => self.as_multi_polygon().downcasted_data_type(),
            GeometryCollection(_) => self.as_geometry_collection().downcasted_data_type(),
            Rect(_) => self.as_rect().downcasted_data_type(),
            Geometry(_) => self.as_geometry().downcasted_data_type(),
        }
    }

    fn downcast(&self) -> Self::Output {
        use NativeType::*;

        match self.data_type() {
            Point(_) => self.as_point().downcast(),
            LineString(_) => self.as_line_string().downcast(),
            Polygon(_) => self.as_polygon().downcast(),
            MultiPoint(_) => self.as_multi_point().downcast(),
            MultiLineString(_) => self.as_multi_line_string().downcast(),
            MultiPolygon(_) => self.as_multi_polygon().downcast(),
            GeometryCollection(_) => self.as_geometry_collection().downcast(),
            Rect(_) => self.as_rect().downcast(),
            Geometry(_) => self.as_geometry().downcast(),
        }
    }
}

pub trait DowncastTable {
    /// If possible, convert this array to a simpler and/or smaller data type
    ///
    /// Conversions include:
    ///
    /// - MultiPoint -> Point
    /// - MultiLineString -> LineString
    /// - MultiPolygon -> Polygon
    /// - MixedGeometry -> any of the 6 concrete types
    /// - GeometryCollection -> MixedGeometry or any of the 6 concrete types
    ///
    /// If small_offsets is `true`, it will additionally try to convert `i64` offset buffers to
    /// `i32` if the offsets would not overflow.
    fn downcast(&self) -> Result<Table>;
}

impl DowncastTable for Table {
    fn downcast(&self) -> Result<Table> {
        let downcasted_columns = self
            .schema()
            .as_ref()
            .geometry_columns()
            .iter()
            .map(|idx| {
                let geometry = self.geometry_column(Some(*idx))?;
                Ok((*idx, geometry.as_ref().downcast()))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut new_table = self.clone();

        for (column_idx, column) in downcasted_columns.iter() {
            let prev_field = self.schema().field(*column_idx);
            let new_field = column
                .data_type()
                .to_field(prev_field.name(), prev_field.is_nullable());
            new_table.set_column(*column_idx, new_field.into(), column.array_refs())?;
        }

        Ok(new_table)
    }
}

// impl Downcast for ChunkedMultiPointArray {
//     type Output = Arc<dyn ChunkedNativeArray>;

//     fn downcast(&self) -> Self::Output {
//         let data_types = self.chunks.iter().map(|chunk| chunk.downcasted_data_type()).collect::<Vec<_>>();
//         let data_types_same = data_types.windows(2).all(|w| w[0] == w[1]);
//         if !data_types_same {
//             return Arc::new(self.clone());
//         }

//         //  else {
//         //     let x = ChunkedGeometryArray::new(self.chunks.iter().map(|chunk| chunk.downcast()).collect());

//         // }

//     }
// }
