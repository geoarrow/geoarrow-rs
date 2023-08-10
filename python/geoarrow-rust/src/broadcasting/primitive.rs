use arrow2::array::PrimitiveArray;
use geoarrow::algorithm::broadcasting::BroadcastablePrimitive;
use pyo3::prelude::*;

use crate::ffi::from_py_array;

pub struct BroadcastableUint32(pub(crate) BroadcastablePrimitive<u32>);

impl<'a> FromPyObject<'a> for BroadcastableUint32 {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        Python::with_gil(|py| {
            let pa = py.import("pyarrow")?;
            let array = pa.getattr("Array")?;
            if ob.is_instance(array)? {
                let arr = from_py_array(ob)?;
                let arr = arr.as_any().downcast_ref::<PrimitiveArray<u32>>().unwrap();
                Ok(BroadcastableUint32(BroadcastablePrimitive::Array(
                    arr.clone(),
                )))
            } else {
                let val: u32 = ob.extract()?;
                Ok(BroadcastableUint32(BroadcastablePrimitive::Scalar(val)))
            }
        })
    }
}

pub struct BroadcastableFloat(pub(crate) BroadcastablePrimitive<f64>);

impl<'a> FromPyObject<'a> for BroadcastableFloat {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        Python::with_gil(|py| {
            let pa = py.import("pyarrow")?;
            let array = pa.getattr("Array")?;
            if ob.is_instance(array)? {
                let arr = from_py_array(ob)?;
                let arr = arr.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
                Ok(BroadcastableFloat(BroadcastablePrimitive::Array(
                    arr.clone(),
                )))
            } else {
                let val: f64 = ob.extract()?;
                Ok(BroadcastableFloat(BroadcastablePrimitive::Scalar(val)))
            }
        })
    }
}
