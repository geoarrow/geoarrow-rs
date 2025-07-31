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

#[derive(Debug)]
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
        Ok(extrema_impl(args, |rect| rect.min().x())?)
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

#[derive(Debug)]
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
        Ok(extrema_impl(args, |rect| rect.min().y())?)
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

#[derive(Debug)]
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
        Ok(extrema_impl(args, |rect| rect.max().x())?)
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

#[derive(Debug)]
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
        Ok(extrema_impl(args, |rect| rect.max().y())?)
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

fn extrema_impl(
    args: ScalarFunctionArgs,
    cb: impl Fn(Rect) -> f64,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = impl_extrema(&geo_array, cb)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}
