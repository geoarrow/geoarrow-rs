/// Simple macro to generate Python wrappers for geodatafusion UDF structs
/// Usage: impl_udf!(RustStructName, PyWrapperName, "python_name")
#[macro_export]
macro_rules! impl_udf {
    ($base:ident, $py_name:ident, $python_name:literal) => {
        #[::pyo3::pyclass(module = "geodatafusion", name = $python_name, frozen)]
        #[derive(Debug, Clone)]
        pub struct $py_name(::std::sync::Arc<$base>);

        #[::pyo3::pymethods]
        impl $py_name {
            #[new]
            fn new() -> Self {
                $py_name(::std::sync::Arc::new($base::new()))
            }

            fn __datafusion_scalar_udf__<'py>(
                &self,
                py: ::pyo3::Python<'py>,
            ) -> ::pyo3::PyResult<::pyo3::Bound<'py, ::pyo3::types::PyCapsule>> {
                let udf = ::std::sync::Arc::new(
                    ::datafusion::logical_expr::ScalarUDF::new_from_shared_impl(self.0.clone()),
                );
                ::pyo3::types::PyCapsule::new(
                    py,
                    ::datafusion_ffi::udf::FFI_ScalarUDF::from(udf),
                    Some($crate::constants::DATAFUSION_CAPSULE_NAME.into()),
                )
            }
        }
    };
}

/// Simple macro to generate Python wrappers for geodatafusion UDF structs
/// Usage: impl_udf!(RustStructName, PyWrapperName, "python_name")
#[macro_export]
macro_rules! impl_udf_coord_type_arg {
    ($base:ident, $py_name:ident, $python_name:literal) => {
        #[::pyo3::pyclass(module = "geodatafusion", name = $python_name, frozen)]
        #[derive(Debug, Clone)]
        pub struct $py_name(::std::sync::Arc<$base>);

        #[::pyo3::pymethods]
        impl $py_name {
            #[new]
            #[pyo3(signature = (*, coord_type=None))]
            fn new(coord_type: Option<pyo3_geoarrow::PyCoordType>) -> Self {
                let coord_type = coord_type.map(|c| c.into()).unwrap_or_default();
                $py_name(::std::sync::Arc::new($base::new(coord_type)))
            }

            fn __datafusion_scalar_udf__<'py>(
                &self,
                py: ::pyo3::Python<'py>,
            ) -> ::pyo3::PyResult<::pyo3::Bound<'py, ::pyo3::types::PyCapsule>> {
                let udf = ::std::sync::Arc::new(
                    ::datafusion::logical_expr::ScalarUDF::new_from_shared_impl(self.0.clone()),
                );
                ::pyo3::types::PyCapsule::new(
                    py,
                    ::datafusion_ffi::udf::FFI_ScalarUDF::from(udf),
                    Some($crate::constants::DATAFUSION_CAPSULE_NAME.into()),
                )
            }
        }
    };
}
