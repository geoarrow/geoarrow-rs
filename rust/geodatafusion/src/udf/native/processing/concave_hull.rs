use std::any::Any;
use std::sync::OnceLock;

use arrow::array::AsArray;
use arrow::datatypes::Float64Type;
use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use geoarrow::algorithm::broadcasting::BroadcastablePrimitive;
use geoarrow::algorithm::geo::ConcaveHull as _;
use geoarrow::array::GeometryArray;
use geoarrow::ArrayBase;

use crate::data_types::{parse_to_native_array, GEOMETRY_TYPE, POINT2D_TYPE};
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub(super) struct ConcaveHull {
    signature: Signature,
}

impl ConcaveHull {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![GEOMETRY_TYPE.into(), DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for ConcaveHull {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_concavehull"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(POINT2D_TYPE.into())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(concave_hull_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "A concave hull is a (usually) concave geometry which contains the input, and whose vertices are a subset of the input vertices. In the general case the concave hull is a Polygon. The concave hull of two or more collinear points is a two-point LineString. The concave hull of one or more identical points is a Point. The polygon will not contain holes unless the optional param_allow_holes argument is specified as true.

One can think of a concave hull as \"shrink-wrapping\" a set of points. This is different to the convex hull, which is more like wrapping a rubber band around the points. A concave hull generally has a smaller area and represents a more natural boundary for the input points.

The param_pctconvex controls the concaveness of the computed hull. A value of 1 produces the convex hull. Values between 1 and 0 produce hulls of increasing concaveness. A value of 0 produces a hull with maximum concaveness (but still a single polygon). Choosing a suitable value depends on the nature of the input data, but often values between 0.3 and 0.1 produce reasonable results.",
                "ST_ConcaveHull(geometry)",
            )
            .with_argument("g1", "geometry")
            .with_argument("param_pctconvex", "float")
            .build()
        }))
    }
}

fn concave_hull_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args[..1])?
        .into_iter()
        .next()
        .unwrap();
    let native_array = parse_to_native_array(array)?;
    let output = match &args[1] {
        ColumnarValue::Scalar(concavity) => {
            let concavity = concavity.to_scalar()?.into_inner();
            let concavity = concavity.as_primitive::<Float64Type>().value(0);
            native_array.as_ref().concave_hull(&concavity.into())?
        }
        ColumnarValue::Array(concavity) => {
            native_array
                .as_ref()
                .concave_hull(&BroadcastablePrimitive::Array(
                    concavity.as_primitive().clone(),
                ))?
        }
    };

    Ok(GeometryArray::from(output).to_array_ref().into())
}
