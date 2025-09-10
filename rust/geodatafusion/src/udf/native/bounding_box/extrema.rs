use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{CoordTrait, RectTrait};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::scalar::Rect;

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::bounding_box::util::bounds::impl_extrema;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct XMin {
    signature: Signature,
}

impl XMin {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

impl Default for XMin {
    fn default() -> Self {
        Self::new()
    }
}

static XMIN_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for XMin {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_xmin"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(extrema_impl(args, false, |rect| rect.min().x())?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(XMIN_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns X minima of a bounding box 2d or 3d or a geometry",
                "ST_XMin(geometry)",
            )
            .with_argument("box", "The geometry or box input")
            .with_related_udf("st_xmin")
            .with_related_udf("st_ymin")
            .with_related_udf("st_zmin")
            .with_related_udf("st_xmax")
            .with_related_udf("st_ymax")
            .with_related_udf("st_zmax")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct YMin {
    signature: Signature,
}

impl YMin {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

impl Default for YMin {
    fn default() -> Self {
        Self::new()
    }
}

static YMIN_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for YMin {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_ymin"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(extrema_impl(args, false, |rect| rect.min().y())?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(YMIN_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns Y minima of a bounding box 2d or 3d or a geometry",
                "ST_YMin(geometry)",
            )
            .with_argument("box", "The geometry or box input")
            .with_related_udf("st_xmin")
            .with_related_udf("st_ymin")
            .with_related_udf("st_zmin")
            .with_related_udf("st_xmax")
            .with_related_udf("st_ymax")
            .with_related_udf("st_zmax")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ZMin {
    signature: Signature,
}

impl ZMin {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

impl Default for ZMin {
    fn default() -> Self {
        Self::new()
    }
}

static ZMIN_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for ZMin {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_zmin"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(extrema_impl(args, true, |rect| {
            rect.min().nth(2).unwrap_or(f64::MIN)
        })?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(ZMIN_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the Z minima of a 2D or 3D bounding box or a geometry",
                "ST_ZMin(geometry)",
            )
            .with_argument("box", "The geometry or box input")
            .with_related_udf("st_xmin")
            .with_related_udf("st_ymin")
            .with_related_udf("st_zmin")
            .with_related_udf("st_xmax")
            .with_related_udf("st_ymax")
            .with_related_udf("st_zmax")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct XMax {
    signature: Signature,
}

impl XMax {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

impl Default for XMax {
    fn default() -> Self {
        Self::new()
    }
}

static XMAX_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for XMax {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_xmax"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(extrema_impl(args, false, |rect| rect.max().x())?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(XMAX_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns X maxima of a bounding box 2d or 3d or a geometry",
                "ST_XMax(geometry)",
            )
            .with_argument("box", "The geometry or box input")
            .with_related_udf("st_xmin")
            .with_related_udf("st_ymin")
            .with_related_udf("st_zmin")
            .with_related_udf("st_xmax")
            .with_related_udf("st_ymax")
            .with_related_udf("st_zmax")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct YMax {
    signature: Signature,
}

impl YMax {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

impl Default for YMax {
    fn default() -> Self {
        Self::new()
    }
}

static YMAX_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for YMax {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_ymax"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(extrema_impl(args, false, |rect| rect.max().y())?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(YMAX_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns Y maxima of a bounding box 2d or 3d or a geometry",
                "ST_YMax(geometry)",
            )
            .with_argument("box", "The geometry or box input")
            .with_related_udf("st_xmin")
            .with_related_udf("st_ymin")
            .with_related_udf("st_zmin")
            .with_related_udf("st_xmax")
            .with_related_udf("st_ymax")
            .with_related_udf("st_zmax")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ZMax {
    signature: Signature,
}

impl ZMax {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

impl Default for ZMax {
    fn default() -> Self {
        Self::new()
    }
}

static ZMAX_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for ZMax {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_zmax"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(extrema_impl(args, true, |rect| {
            rect.max().nth(2).unwrap_or(f64::MAX)
        })?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(ZMAX_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns Z maxima of a bounding box 2d or 3d or a geometry",
                "ST_ZMax(geometry)",
            )
            .with_argument("box", "The geometry or box input")
            .with_related_udf("st_xmin")
            .with_related_udf("st_ymin")
            .with_related_udf("st_zmin")
            .with_related_udf("st_xmax")
            .with_related_udf("st_ymax")
            .with_related_udf("st_zmax")
            .build()
        }))
    }
}

fn extrema_impl(
    args: ScalarFunctionArgs,
    include_z: bool,
    cb: impl Fn(Rect) -> f64,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = impl_extrema(&geo_array, include_z, cb)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

#[cfg(test)]
mod test {
    use approx::relative_eq;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use datafusion::prelude::*;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    async fn extrema_test(udf: &str, expected: f64) {
        let ctx = SessionContext::new();

        ctx.register_udf(XMin::new().into());
        ctx.register_udf(YMin::new().into());
        ctx.register_udf(ZMin::new().into());
        ctx.register_udf(XMax::new().into());
        ctx.register_udf(YMax::new().into());
        ctx.register_udf(ZMax::new().into());
        ctx.register_udf(GeomFromText::default().into());

        let out = ctx
            .sql(&format!(
                "SELECT {udf}(ST_GeomFromText('LINESTRING Z(1 2 3, 3 4 5, 5 6 7)'));"
            ))
            .await
            .unwrap();
        let batch = out.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let arr = col.as_primitive::<Float64Type>();
        assert!(relative_eq!(arr.value(0), expected));
    }

    #[tokio::test]
    async fn test_2d() {
        extrema_test("ST_XMin", 1.0).await;
        extrema_test("ST_YMin", 2.0).await;
        extrema_test("ST_ZMin", 3.0).await;
        extrema_test("ST_XMax", 5.0).await;
        extrema_test("ST_YMax", 6.0).await;
        extrema_test("ST_ZMax", 7.0).await;
    }
}
