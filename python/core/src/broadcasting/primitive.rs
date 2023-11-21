use arrow_array::types::{Float64Type, UInt32Type};
// use arrow_array::{Float64Array, UInt32Array};
use geoarrow::algorithm::broadcasting::BroadcastablePrimitive;
use pyo3::prelude::*;

pub struct BroadcastableUint32(pub(crate) BroadcastablePrimitive<UInt32Type>);

impl<'a> FromPyObject<'a> for BroadcastableUint32 {
    fn extract(_ob: &'a PyAny) -> PyResult<Self> {
        todo!()
        // Python::with_gil(|py| {
        //     let pa = py.import("pyarrow")?;
        //     let array = pa.getattr("Array")?;
        //     if ob.hasattr("__arrow_c_array__")? {
        //         let arr = from_py_array(ob)?;
        //         let arr = arr.as_any().downcast_ref::<UInt32Array>().unwrap();
        //         Ok(BroadcastableUint32(BroadcastablePrimitive::Array(
        //             arr.clone(),
        //         )))
        //     } else {
        //         let val: u32 = ob.extract()?;
        //         Ok(BroadcastableUint32(BroadcastablePrimitive::Scalar(val)))
        //     }
        // })
    }
}

// impl IntoPy<PyResult<PyObject>> for BroadcastableUint32 {
//     fn into_py(self, py: Python<'_>) -> PyResult<PyObject> {
//         match self.0 {
//             BroadcastablePrimitive::Array(arr) => to_py_array(py, todo!()),
//             BroadcastablePrimitive::Scalar(scalar) => Ok(scalar.into_py(py)),
//         }
//     }
// }

pub struct BroadcastableFloat(pub(crate) BroadcastablePrimitive<Float64Type>);

impl<'a> FromPyObject<'a> for BroadcastableFloat {
    fn extract(_ob: &'a PyAny) -> PyResult<Self> {
        todo!()
        // Python::with_gil(|py| {
        //     let pa = py.import("pyarrow")?;
        //     let array = pa.getattr("Array")?;
        //     if ob.hasattr("__arrow_c_array__")? {
        //         let arr = from_py_array(ob)?;
        //         let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        //         Ok(BroadcastableFloat(BroadcastablePrimitive::Array(
        //             arr.clone(),
        //         )))
        //     } else {
        //         let val: f64 = ob.extract()?;
        //         Ok(BroadcastableFloat(BroadcastablePrimitive::Scalar(val)))
        //     }
        // })
    }
}

// impl IntoPy<PyResult<PyObject>> for BroadcastableFloat {
//     fn into_py(self, py: Python<'_>) -> PyResult<PyObject> {
//         match self.0 {
//             BroadcastablePrimitive::Array(arr) => to_py_array(py, todo!()),
//             BroadcastablePrimitive::Scalar(scalar) => Ok(scalar.into_py(py)),
//         }
//     }
// }
