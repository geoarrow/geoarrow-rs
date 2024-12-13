use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow::array::Float64Builder;
use arrow_array::ArrayRef;
use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{ColumnarValue, Documentation, ScalarUDFImpl, Signature};
use geo_traits::{CoordTrait, RectTrait};
use geoarrow::algorithm::native::BoundingRectArray;
use geoarrow::array::RectArray;
use geoarrow::trait_::ArrayAccessor;

use crate::data_types::{any_single_geometry_type_input, parse_to_native_array};
use crate::error::GeoDataFusionResult;

fn rect_array_from_array_ref(array: ArrayRef) -> GeoDataFusionResult<RectArray> {
    let native_arr = parse_to_native_array(array)?;
    Ok(native_arr.as_ref().bounding_rect()?)
}

#[derive(Debug)]
pub(super) struct XMin {
    signature: Signature,
}

impl XMin {
    pub(super) fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
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

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        let arg = ColumnarValue::values_to_arrays(args)?
            .into_iter()
            .next()
            .unwrap();
        let mut output_array = Float64Builder::with_capacity(arg.len());

        let rect_array = rect_array_from_array_ref(arg)?;

        for rect in rect_array.iter() {
            output_array.append_option(rect.map(|r| r.min().x()));
        }
        Ok(ColumnarValue::from(
            Arc::new(output_array.finish()) as ArrayRef
        ))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(XMIN_DOC.get_or_init(|| {
            Documentation::builder()
                .with_doc_section(DOC_SECTION_OTHER)
                .with_description("Returns X minima of a bounding box 2d or 3d or a geometry")
                .with_syntax_example("ST_XMin(geometry)")
                .with_argument("box", "The geometry or box input")
                .with_related_udf("st_xmin")
                .with_related_udf("st_ymin")
                .with_related_udf("st_zmin")
                .with_related_udf("st_xmax")
                .with_related_udf("st_ymax")
                .with_related_udf("st_zmax")
                .build()
                .unwrap()
        }))
    }
}

#[derive(Debug)]
pub(super) struct YMin {
    signature: Signature,
}

impl YMin {
    pub(super) fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
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

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        let arg = ColumnarValue::values_to_arrays(args)?
            .into_iter()
            .next()
            .unwrap();
        let mut output_array = Float64Builder::with_capacity(arg.len());

        let rect_array = rect_array_from_array_ref(arg)?;

        for rect in rect_array.iter() {
            output_array.append_option(rect.map(|r| r.min().y()));
        }
        Ok(ColumnarValue::from(
            Arc::new(output_array.finish()) as ArrayRef
        ))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(YMIN_DOC.get_or_init(|| {
            Documentation::builder()
                .with_doc_section(DOC_SECTION_OTHER)
                .with_description("Returns Y minima of a bounding box 2d or 3d or a geometry")
                .with_syntax_example("ST_YMin(geometry)")
                .with_argument("box", "The geometry or box input")
                .with_related_udf("st_xmin")
                .with_related_udf("st_ymin")
                .with_related_udf("st_zmin")
                .with_related_udf("st_xmax")
                .with_related_udf("st_ymax")
                .with_related_udf("st_zmax")
                .build()
                .unwrap()
        }))
    }
}

#[derive(Debug)]
pub(super) struct XMax {
    signature: Signature,
}

impl XMax {
    pub(super) fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
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

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        let arg = ColumnarValue::values_to_arrays(args)?
            .into_iter()
            .next()
            .unwrap();
        let mut output_array = Float64Builder::with_capacity(arg.len());
        let rect_array = rect_array_from_array_ref(arg)?;
        for rect in rect_array.iter() {
            output_array.append_option(rect.map(|r| r.max().x()));
        }
        Ok(ColumnarValue::from(
            Arc::new(output_array.finish()) as ArrayRef
        ))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(XMAX_DOC.get_or_init(|| {
            Documentation::builder()
                .with_doc_section(DOC_SECTION_OTHER)
                .with_description("Returns X maxima of a bounding box 2d or 3d or a geometry")
                .with_syntax_example("ST_XMax(geometry)")
                .with_argument("box", "The geometry or box input")
                .with_related_udf("st_xmin")
                .with_related_udf("st_ymin")
                .with_related_udf("st_zmin")
                .with_related_udf("st_xmax")
                .with_related_udf("st_ymax")
                .with_related_udf("st_zmax")
                .build()
                .unwrap()
        }))
    }
}

#[derive(Debug)]
pub(super) struct YMax {
    signature: Signature,
}

impl YMax {
    pub(super) fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
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

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        let arg = ColumnarValue::values_to_arrays(args)?
            .into_iter()
            .next()
            .unwrap();
        let mut output_array = Float64Builder::with_capacity(arg.len());
        let rect_array = rect_array_from_array_ref(arg)?;
        for rect in rect_array.iter() {
            output_array.append_option(rect.map(|r| r.max().y()));
        }
        Ok(ColumnarValue::from(
            Arc::new(output_array.finish()) as ArrayRef
        ))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(YMAX_DOC.get_or_init(|| {
            Documentation::builder()
                .with_doc_section(DOC_SECTION_OTHER)
                .with_description("Returns Y maxima of a bounding box 2d or 3d or a geometry")
                .with_syntax_example("ST_YMax(geometry)")
                .with_argument("box", "The geometry or box input")
                .with_related_udf("st_xmin")
                .with_related_udf("st_ymin")
                .with_related_udf("st_zmin")
                .with_related_udf("st_xmax")
                .with_related_udf("st_ymax")
                .with_related_udf("st_zmax")
                .build()
                .unwrap()
        }))
    }
}
