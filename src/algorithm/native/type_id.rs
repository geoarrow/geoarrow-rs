use crate::array::*;
use crate::io::wkb::reader::WKBGeometryType;
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
use arrow_array::builder::Int8Builder;
use arrow_array::{Int8Array, OffsetSizeTrait};
use std::collections::HashSet;

/// Calculation of the geometry types within a GeometryArray
pub trait TypeIds {
    /// Return the geometry types stored in this array
    ///
    /// The integer values of this return type match that of GEOS and Shapely:
    ///
    /// - None (missing) is -1
    /// - POINT is 0
    /// - LINESTRING is 1
    /// - LINEARRING is 2
    /// - POLYGON is 3
    /// - MULTIPOINT is 4
    /// - MULTILINESTRING is 5
    /// - MULTIPOLYGON is 6
    /// - GEOMETRYCOLLECTION is 7
    fn get_type_ids(&self) -> Int8Array;

    /// Return the unique geometry types stored in this array
    ///
    /// The integer values of this return type match that of GEOS and Shapely:
    ///
    /// - None (missing) is -1
    /// - POINT is 0
    /// - LINESTRING is 1
    /// - LINEARRING is 2
    /// - POLYGON is 3
    /// - MULTIPOINT is 4
    /// - MULTILINESTRING is 5
    /// - MULTIPOLYGON is 6
    /// - GEOMETRYCOLLECTION is 7
    fn get_unique_type_ids(&self) -> HashSet<i8>;
}

impl TypeIds for PointArray {
    fn get_type_ids(&self) -> Int8Array {
        let values = vec![0i8; self.len()];
        Int8Array::new(values.into(), self.nulls().cloned())
    }

    fn get_unique_type_ids(&self) -> HashSet<i8> {
        let mut values = HashSet::with_capacity(1);
        values.insert(0);
        values
    }
}

macro_rules! constant_impl {
    ($type:ty, $value:expr) => {
        impl<O: OffsetSizeTrait> TypeIds for $type {
            fn get_type_ids(&self) -> Int8Array {
                let values = vec![$value; self.len()];
                Int8Array::new(values.into(), self.nulls().cloned())
            }

            fn get_unique_type_ids(&self) -> HashSet<i8> {
                let mut values = HashSet::with_capacity(1);
                values.insert($value);
                values
            }
        }
    };
}

constant_impl!(LineStringArray<O>, 1);
constant_impl!(PolygonArray<O>, 3);
constant_impl!(MultiPointArray<O>, 4);
constant_impl!(MultiLineStringArray<O>, 5);
constant_impl!(MultiPolygonArray<O>, 6);

impl<O: OffsetSizeTrait> TypeIds for MixedGeometryArray<O> {
    fn get_type_ids(&self) -> Int8Array {
        use crate::scalar::Geometry::*;

        let mut output_array = Int8Builder::with_capacity(self.len());
        self.iter().for_each(|maybe_g| {
            output_array.append_option(maybe_g.map(|g| match g {
                Point(_) => 0,
                LineString(_) => 1,
                Polygon(_) => 3,
                Rect(_) => 3,
                MultiPoint(_) => 4,
                MultiLineString(_) => 5,
                MultiPolygon(_) => 6,
                GeometryCollection(_) => 7,
            }))
        });
        output_array.finish()
    }

    fn get_unique_type_ids(&self) -> HashSet<i8> {
        use crate::scalar::Geometry::*;

        let mut values = HashSet::new();
        self.iter().flatten().for_each(|g| {
            let type_id = match g {
                Point(_) => 0,
                LineString(_) => 1,
                Polygon(_) => 3,
                Rect(_) => 3,
                MultiPoint(_) => 4,
                MultiLineString(_) => 5,
                MultiPolygon(_) => 6,
                GeometryCollection(_) => 7,
            };
            values.insert(type_id);
        });

        values
    }
}

impl<O: OffsetSizeTrait> TypeIds for WKBArray<O> {
    fn get_type_ids(&self) -> Int8Array {
        use WKBGeometryType::*;

        let mut output_array = Int8Builder::with_capacity(self.len());

        self.iter().for_each(|maybe_wkb| {
            output_array.append_option(maybe_wkb.map(|wkb| match wkb.get_wkb_geometry_type() {
                Point => 0,
                LineString => 1,
                Polygon => 3,
                MultiPoint => 4,
                MultiLineString => 5,
                MultiPolygon => 6,
                GeometryCollection => 7,
            }))
        });

        output_array.finish()
    }

    fn get_unique_type_ids(&self) -> HashSet<i8> {
        use WKBGeometryType::*;

        let mut values = HashSet::new();
        self.iter().flatten().for_each(|wkb| {
            let type_id = match wkb.get_wkb_geometry_type() {
                Point => 0,
                LineString => 1,
                Polygon => 3,
                MultiPoint => 4,
                MultiLineString => 5,
                MultiPolygon => 6,
                GeometryCollection => 7,
            };
            values.insert(type_id);
        });

        values
    }
}
