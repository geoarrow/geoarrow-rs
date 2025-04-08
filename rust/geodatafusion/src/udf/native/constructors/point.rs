//! Point constructors

use std::any::Any;
use std::sync::OnceLock;

use arrow::array::AsArray;
use arrow::datatypes::Float64Type;
use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use geo_traits::CoordTrait;
use geoarrow::ArrayBase;
use geoarrow::array::{GeometryArray, PointBuilder};
use geoarrow_schema::{CoordType, Dimension};

use crate::data_types::{POINT2D_TYPE, POINT3D_TYPE};

#[derive(Debug)]
pub(super) struct Point {
    signature: Signature,
}

impl Point {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

static POINT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Point {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_point"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(POINT2D_TYPE().into())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        let mut args = ColumnarValue::values_to_arrays(args)?.into_iter();
        let x = args.next().unwrap();
        let y = args.next().unwrap();

        let x = x.as_primitive::<Float64Type>();
        let y = y.as_primitive::<Float64Type>();

        let mut builder = PointBuilder::with_capacity_and_options(
            Dimension::XY,
            x.len(),
            CoordType::Separated,
            Default::default(),
        );
        for (x, y) in x.iter().zip(y.iter()) {
            if let (Some(x), Some(y)) = (x, y) {
                builder.push_coord(Some(&geo::coord! { x: x, y: y}));
            } else {
                builder.push_null();
            }
        }

        Ok(builder.finish().into_array_ref().into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(POINT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a Point with the given X and Y coordinate values.",
                "ST_Point(-71.104, 42.315)",
            )
            .with_argument("x", "x value")
            .with_argument("y", "y value")
            .with_related_udf("st_makepoint")
            .with_related_udf("st_pointz")
            .build()
        }))
    }
}

#[derive(Debug)]
pub(super) struct MakePoint {
    signature: Signature,
}

impl MakePoint {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Float64, DataType::Float64]),
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

static MAKE_POINT_DOC: OnceLock<Documentation> = OnceLock::new();

struct PointZ {
    x: f64,
    y: f64,
    z: f64,
}

impl CoordTrait for PointZ {
    type T = f64;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xyz
    }

    fn x(&self) -> Self::T {
        self.x
    }

    fn y(&self) -> Self::T {
        self.y
    }

    fn nth_or_panic(&self, n: usize) -> Self::T {
        match n {
            0 => self.x,
            1 => self.y,
            2 => self.z,
            _ => panic!("invalid dimension index"),
        }
    }
}

impl ScalarUDFImpl for MakePoint {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_makepoint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        match arg_types.len() {
            2 => Ok(POINT2D_TYPE().into()),
            3 => Ok(POINT3D_TYPE().into()),
            _ => unreachable!(),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        let mut args = ColumnarValue::values_to_arrays(args)?.into_iter();
        let x = args.next().unwrap();
        let y = args.next().unwrap();
        let z = args.next();

        let x = x.as_primitive::<Float64Type>();
        let y = y.as_primitive::<Float64Type>();

        let dim = if z.is_some() {
            Dimension::XYZ
        } else {
            Dimension::XY
        };
        let mut builder = PointBuilder::with_capacity_and_options(
            dim,
            x.len(),
            CoordType::Separated,
            Default::default(),
        );

        if let Some(z) = z {
            let z = z.as_primitive::<Float64Type>();

            for ((x, y), z) in x.iter().zip(y.iter()).zip(z.iter()) {
                if let (Some(x), Some(y), Some(z)) = (x, y, z) {
                    builder.push_coord(Some(&PointZ { x, y, z }));
                } else {
                    builder.push_null();
                }
            }
        } else {
            for (x, y) in x.iter().zip(y.iter()) {
                if let (Some(x), Some(y)) = (x, y) {
                    builder.push_coord(Some(&geo::coord! { x: x, y: y}));
                } else {
                    builder.push_null();
                }
            }
        }

        Ok(GeometryArray::from(builder.finish())
            .into_array_ref()
            .into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MAKE_POINT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a 2D XY or 3D XYZ Point geometry.",
                "ST_MakePoint(-71.104, 42.315)",
            )
            .with_argument("x", "x value")
            .with_argument("y", "y value")
            .with_argument("z", "z value")
            .with_related_udf("st_point")
            .with_related_udf("st_pointz")
            .build()
        }))
    }
}
