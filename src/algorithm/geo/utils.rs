use arrow2::array::PrimitiveArray;
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::DataType;

pub(crate) fn zeroes(len: usize, validity: Option<&Bitmap>) -> PrimitiveArray<f64> {
    let values = vec![0.0f64; len];
    PrimitiveArray::new(DataType::Float64, values.into(), validity.cloned())
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
                        $enum::WKB(g) => g.$func_name($($arg_name),*).into(),
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
