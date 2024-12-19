//! Accessors from LineString geometries

use std::any::Any;
use std::sync::OnceLock;

use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{ColumnarValue, Documentation, ScalarUDFImpl, Signature};
use geo_traits::LineStringTrait;
use geoarrow::array::{AsNativeArray, CoordType, PointBuilder};
use geoarrow::datatypes::Dimension;
use geoarrow::error::GeoArrowError;
use geoarrow::scalar::Geometry;
use geoarrow::trait_::ArrayAccessor;
use geoarrow::ArrayBase;

use crate::data_types::{any_single_geometry_type_input, parse_to_native_array, POINT2D_TYPE};
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub(super) struct StartPoint {
    signature: Signature,
}

impl StartPoint {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

static START_POINT_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for StartPoint {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_startpoint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(POINT2D_TYPE.into())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(start_point_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(START_POINT_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(DOC_SECTION_OTHER, "Returns the first point of a LINESTRING geometry as a POINT. Returns NULL if the input is not a LINESTRING", "ST_StartPoint(line_string)" )
                .with_argument("g1", "geometry")
                .build()
        }))
    }
}

fn start_point_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .unwrap();
    let native_array = parse_to_native_array(array)?;
    let native_array_ref = native_array.as_ref();
    let geometry_array = native_array_ref
        .as_geometry_opt()
        .ok_or(GeoArrowError::General(
            "Expected Geometry-typed array in ST_StartPoint".to_string(),
        ))?;

    let mut output_builder = PointBuilder::with_capacity_and_options(
        Dimension::XY,
        geometry_array.len(),
        CoordType::Separated,
        Default::default(),
    );

    for geom in geometry_array.iter() {
        if let Some(Geometry::LineString(line_string)) = geom {
            output_builder.push_coord(line_string.coord(0).as_ref());
        } else {
            output_builder.push_null();
        }
    }

    Ok(output_builder.finish().into_array_ref().into())
}
