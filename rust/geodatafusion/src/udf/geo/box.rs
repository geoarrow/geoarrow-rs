//! Box functions

use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow::array::Float64Builder;
use arrow_array::ArrayRef;
use arrow_schema::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::create_udf;
use geo_traits::{CoordTrait, RectTrait};
use geoarrow::algorithm::native::BoundingRectArray;
use geoarrow::array::{GeometryArray, RectArray};
use geoarrow::datatypes::Dimension;
use geoarrow::trait_::ArrayAccessor;
use geoarrow::ArrayBase;

use crate::error::GeoDataFusionResult;
use crate::udf::geo::util::{
    box2d_data_type, box3d_data_type, geometry_data_type, parse_single_arg_to_geometry_array,
};

/// Box2D
///
/// - Returns a BOX2D representing the maximum extents of the geometry.
pub fn box_2d() -> ScalarUDF {
    create_udf(
        "box2d",
        vec![geometry_data_type()],
        box2d_data_type(),
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| Ok(_box2d(args)?)),
    )
}

fn _box2d(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let geom_arr = parse_single_arg_to_geometry_array(args)?;
    Ok(geom_arr.bounding_rect().into_array_ref().into())
}

/// Returns X minima of a bounding box 2d or 3d or a geometry
pub fn xmin() -> ScalarUDF {
    XMin::new().into()
}

/// Returns Y minima of a bounding box 2d or 3d or a geometry
pub fn ymin() -> ScalarUDF {
    YMin::new().into()
}

/// Returns X maxima of a bounding box 2d or 3d or a geometry
pub fn xmax() -> ScalarUDF {
    XMax::new().into()
}

/// Returns Y maxima of a bounding box 2d or 3d or a geometry
pub fn ymax() -> ScalarUDF {
    YMax::new().into()
}

fn rect_array_from_array_ref(array: ArrayRef) -> datafusion::error::Result<RectArray> {
    let data_type = array.data_type();
    if box2d_data_type().equals_datatype(data_type) {
        RectArray::try_from((array.as_ref(), Dimension::XY))
            .map_err(|err| DataFusionError::External(Box::new(err)))
    } else if box3d_data_type().equals_datatype(data_type) {
        RectArray::try_from((array.as_ref(), Dimension::XYZ))
            .map_err(|err| DataFusionError::External(Box::new(err)))
    } else if geometry_data_type().equals_datatype(data_type) {
        let geom_array = GeometryArray::try_from(array.as_ref())
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(geom_array.bounding_rect())
    } else {
        return Err(DataFusionError::Execution(format!(
            "Unsupported input data type: {}",
            data_type
        )));
    }
}

#[derive(Debug)]
struct XMin {
    signature: Signature,
}

impl XMin {
    fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![box2d_data_type(), box3d_data_type(), geometry_data_type()],
                Volatility::Immutable,
            ),
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
struct YMin {
    signature: Signature,
}

impl YMin {
    fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![box2d_data_type(), box3d_data_type(), geometry_data_type()],
                Volatility::Immutable,
            ),
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
struct XMax {
    signature: Signature,
}

impl XMax {
    fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![box2d_data_type(), box3d_data_type(), geometry_data_type()],
                Volatility::Immutable,
            ),
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
struct YMax {
    signature: Signature,
}

impl YMax {
    fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![box2d_data_type(), box3d_data_type(), geometry_data_type()],
                Volatility::Immutable,
            ),
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
