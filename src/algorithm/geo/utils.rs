use arrow_array::Float64Array;
use arrow_buffer::NullBuffer;

pub(crate) fn zeroes(len: usize, nulls: Option<&NullBuffer>) -> Float64Array {
    let values = vec![0.0f64; len];
    Float64Array::new(values.into(), nulls.cloned())
}

/// Implements the common pattern where a [`GeometryArray`][crate::array::GeometryArray] enum
/// simply delegates its trait impl to it's inner type.
///
// This is derived from geo https://github.com/georust/geo/blob/d4c858308ba910f69beab175e08af263b17c5f9f/geo/src/types.rs#L119-L158
#[macro_export]
macro_rules! geometry_array_delegate_impl {
    ($($a:tt)*) => { $crate::__geometry_array_delegate_impl_helper!{ GeometryArray, $($a)* } }
}

#[doc(hidden)]
#[macro_export]
macro_rules! __geometry_array_delegate_impl_helper {
    (
        $enum:ident,
        $(
            $(#[$outer:meta])*
            fn $func_name: ident(&$($self_life:lifetime)?self $(, $arg_name: ident: $arg_type: ty)*) -> $return: ty;
         )+
    ) => {
            $(
                $(#[$outer])*
                fn $func_name(&$($self_life)? self, $($arg_name: $arg_type),*) -> $return {
                    match self {
                        $enum::Point(g) => g.$func_name($($arg_name),*).into(),
                        // $enum::Line(g) =>  g.$func_name($($arg_name),*).into(),
                        $enum::LineString(g) => g.$func_name($($arg_name),*).into(),
                        $enum::Polygon(g) => g.$func_name($($arg_name),*).into(),
                        $enum::MultiPoint(g) => g.$func_name($($arg_name),*).into(),
                        $enum::MultiLineString(g) => g.$func_name($($arg_name),*).into(),
                        $enum::MultiPolygon(g) => g.$func_name($($arg_name),*).into(),
                        // $enum::GeometryCollection(g) => g.$func_name($($arg_name),*).into(),
                        $enum::Rect(_g) => todo!(),
                        // $enum::Rect(g) => g.$func_name($($arg_name),*).into(),
                        // $enum::Triangle(g) => g.$func_name($($arg_name),*).into(),
                    }
                }
            )+
        };
}

/// Implements the common pattern where a [`GeometryArray`][crate::array::GeometryArray] enum
/// simply delegates its trait impl to it's inner type.
///
// This is derived from geo https://github.com/georust/geo/blob/d4c858308ba910f69beab175e08af263b17c5f9f/geo/src/types.rs#L119-L158
#[macro_export]
macro_rules! geometry_dyn_array_delegate_impl {
    ($($a:tt)*) => { $crate::__geometry_dyn_array_delegate_impl_helper!{  $($a)* } }
}

#[doc(hidden)]
#[macro_export]
macro_rules! __geometry_dyn_array_delegate_impl_helper {
    (
        $(
            $(#[$outer:meta])*
            fn $func_name: ident(&$($self_life:lifetime)?self $(, $arg_name: ident: $arg_type: ty)*) -> $return: ty;
         )+
    ) => {
            $(
                $(#[$outer])*
                fn $func_name(&$($self_life)? self, $($arg_name: $arg_type),*) -> $return {
                    use $crate::array::*;
                    use $crate::datatypes::GeoDataType;

                    match self.data_type() {
                        GeoDataType::Point(_) => {
                            let arr = self.as_any().downcast_ref::<PointArray>().unwrap();
                            let new_arr = arr.$func_name($($arg_name),*);
                            new_arr.into()
                        },
                        GeoDataType::LineString(_) => {
                            let arr = self.as_any().downcast_ref::<LineStringArray<i32>>().unwrap();
                            let new_arr = arr.$func_name($($arg_name),*);
                            new_arr.into()
                        }
                        GeoDataType::LargeLineString(_) => {
                            let arr = self.as_any().downcast_ref::<LineStringArray<i64>>().unwrap();
                            let new_arr = arr.$func_name($($arg_name),*);
                            new_arr.into()
                        }
                        GeoDataType::Polygon(_) => {
                            let arr = self.as_any().downcast_ref::<PolygonArray<i32>>().unwrap();
                            let new_arr = arr.$func_name($($arg_name),*);
                            new_arr.into()
                        }
                        GeoDataType::LargePolygon(_) => {
                            let arr = self.as_any().downcast_ref::<PolygonArray<i64>>().unwrap();
                            let new_arr = arr.$func_name($($arg_name),*);
                            new_arr.into()
                        }
                        GeoDataType::MultiPoint(_) => {
                            let arr = self.as_any().downcast_ref::<MultiPointArray<i32>>().unwrap();
                            let new_arr = arr.$func_name($($arg_name),*);
                            new_arr.into()
                        }
                        GeoDataType::LargeMultiPoint(_) => {
                            let arr = self.as_any().downcast_ref::<MultiPointArray<i64>>().unwrap();
                            let new_arr = arr.$func_name($($arg_name),*);
                            new_arr.into()
                        }
                        GeoDataType::MultiLineString(_) => {
                            let arr = self.as_any().downcast_ref::<MultiLineStringArray<i32>>().unwrap();
                            let new_arr = arr.$func_name($($arg_name),*);
                            new_arr.into()
                        }
                        GeoDataType::LargeMultiLineString(_) => {
                            let arr = self.as_any().downcast_ref::<MultiLineStringArray<i64>>().unwrap();
                            let new_arr = arr.$func_name($($arg_name),*);
                            new_arr.into()
                        }
                        GeoDataType::MultiPolygon(_) => {
                            let arr = self.as_any().downcast_ref::<MultiPolygonArray<i32>>().unwrap();
                            let new_arr = arr.$func_name($($arg_name),*);
                            new_arr.into()
                        }
                        GeoDataType::LargeMultiPolygon(_) => {
                            let arr = self.as_any().downcast_ref::<MultiPolygonArray<i64>>().unwrap();
                            let new_arr = arr.$func_name($($arg_name),*);
                            new_arr.into()
                        }
                        GeoDataType::Mixed(_) => {
                            let arr = self.as_any().downcast_ref::<MixedGeometryArray<i32>>().unwrap();
                            let new_arr = arr.$func_name($($arg_name),*);
                            new_arr.into()
                        }
                        GeoDataType::LargeMixed(_) => {
                            let arr = self.as_any().downcast_ref::<MixedGeometryArray<i64>>().unwrap();
                            let new_arr = arr.$func_name($($arg_name),*);
                            new_arr.into()
                        }
                        GeoDataType::GeometryCollection(_) => {
                            let arr = self.as_any().downcast_ref::<GeometryCollectionArray<i32>>().unwrap();
                            let new_arr = arr.$func_name($($arg_name),*);
                            new_arr.into()
                        }
                        GeoDataType::LargeGeometryCollection(_) => {
                            let arr = self.as_any().downcast_ref::<GeometryCollectionArray<i64>>().unwrap();
                            let new_arr = arr.$func_name($($arg_name),*);
                            new_arr.into()
                        }
                        GeoDataType::Rect => todo!(),
                        GeoDataType::WKB => todo!(),
                        GeoDataType::LargeWKB => todo!(),
                    }
                }
            )+
        };
}
