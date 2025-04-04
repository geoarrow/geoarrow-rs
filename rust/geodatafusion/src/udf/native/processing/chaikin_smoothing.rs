// use std::any::Any;
// use std::sync::OnceLock;

// use arrow_schema::DataType;
// use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
// use datafusion::logical_expr::{ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignature};
// use geoarrow::algorithm::geo::ChaikinSmoothing as _;
// use geoarrow::array::{CoordType, GeometryArray};
// use geoarrow::ArrayBase;

// use crate::data_types::{any_single_geometry_type_input, parse_to_native_array, GEOMETRY_TYPE};
// use crate::error::GeoDataFusionResult;

// #[derive(Debug)]
// pub(super) struct ChaikinSmoothing {
//     signature: Signature,
// }

// impl ChaikinSmoothing {
//     pub fn new() -> Self {
//         // TypeSignature::
//         Signature::co(vec![GEOMETRY_TYPE.into(), ], volatility)
//         Self {
//             signature: any_single_geometry_type_input(),
//         }
//     }
// }

// static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

// impl ScalarUDFImpl for ChaikinSmoothing {
//     fn as_any(&self) -> &dyn Any {
//         self
//     }

//     fn name(&self) -> &str {
//         "st_convexhull"
//     }

//     fn signature(&self) -> &Signature {
//         &self.signature
//     }

//     fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
//         Ok(GEOMETRY_TYPE.into())
//     }

//     fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
//         Ok(chaikin_impl(args)?)
//     }

//     fn documentation(&self) -> Option<&Documentation> {
//         Some(DOCUMENTATION.get_or_init(|| {
//             Documentation::builder()
//                 .with_doc_section(DOC_SECTION_OTHER)
//                 .with_description(
//                     "Smoothes a linear or polygonal geometry using Chaikin's algorithm.",
//                 )
//                 .with_argument("g1", "geometry")
//                 .build()
//                 .unwrap()
//         }))
//     }
// }

// fn chaikin_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
//     let array = ColumnarValue::values_to_arrays(args)?
//         .into_iter()
//         .next()
//         .unwrap();
//     let native_array = parse_to_native_array(array)?;
//     let output = native_array
//         .as_ref()
//         .convex_hull()?
//         .into_coord_type(CoordType::Separated);
//     Ok(GeometryArray::from(output).into_array_ref().into())
// }
