use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use object_store::path::Error as PathError;
use object_store::{
    BackoffConfig, ClientOptions, DynObjectStore, Error as InnerObjectStoreError, RetryConfig,
};
use pyo3::exceptions::{PyException, PyFileExistsError, PyFileNotFoundError};
use pyo3::prelude::*;
use pyo3::PyErr;
use tokio::runtime::Runtime;

use super::builder::ObjectStoreBuilder;

#[derive(Debug)]
pub enum ObjectStoreError {
    ObjectStore(InnerObjectStoreError),
    Common(String),
    Python(PyErr),
    IO(std::io::Error),
    Task(tokio::task::JoinError),
    Path(PathError),
    InputValue(String),
}

impl fmt::Display for ObjectStoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ObjectStoreError::ObjectStore(e) => write!(f, "ObjectStore error: {:?}", e),
            ObjectStoreError::Python(e) => write!(f, "Python error {:?}", e),
            ObjectStoreError::Path(e) => write!(f, "Path error {:?}", e),
            ObjectStoreError::IO(e) => write!(f, "IOError error {:?}", e),
            ObjectStoreError::Task(e) => write!(f, "Task error {:?}", e),
            ObjectStoreError::Common(e) => write!(f, "{}", e),
            ObjectStoreError::InputValue(e) => write!(f, "Invalid input value: {}", e),
        }
    }
}

impl From<InnerObjectStoreError> for ObjectStoreError {
    fn from(err: InnerObjectStoreError) -> ObjectStoreError {
        ObjectStoreError::ObjectStore(err)
    }
}

impl From<PathError> for ObjectStoreError {
    fn from(err: PathError) -> ObjectStoreError {
        ObjectStoreError::Path(err)
    }
}

impl From<tokio::task::JoinError> for ObjectStoreError {
    fn from(err: tokio::task::JoinError) -> ObjectStoreError {
        ObjectStoreError::Task(err)
    }
}

impl From<std::io::Error> for ObjectStoreError {
    fn from(err: std::io::Error) -> ObjectStoreError {
        ObjectStoreError::IO(err)
    }
}

impl From<PyErr> for ObjectStoreError {
    fn from(err: PyErr) -> ObjectStoreError {
        ObjectStoreError::Python(err)
    }
}

impl From<ObjectStoreError> for PyErr {
    fn from(err: ObjectStoreError) -> PyErr {
        match err {
            ObjectStoreError::Python(py_err) => py_err,
            ObjectStoreError::ObjectStore(store_err) => match store_err {
                InnerObjectStoreError::NotFound { .. } => {
                    PyFileNotFoundError::new_err(store_err.to_string())
                }
                InnerObjectStoreError::AlreadyExists { .. } => {
                    PyFileExistsError::new_err(store_err.to_string())
                }
                _ => PyException::new_err(store_err.to_string()),
            },
            _ => PyException::new_err(err.to_string()),
        }
    }
}

#[pyclass(name = "ClientOptions", module = "geoarrow.rust.core._rust")]
#[derive(Debug, Clone, Default)]
pub struct PyClientOptions {
    #[pyo3(get, set)]
    user_agent: Option<String>,
    #[pyo3(get, set)]
    content_type_map: HashMap<String, String>,
    #[pyo3(get, set)]
    default_content_type: Option<String>,
    // default_headers: Option<HeaderMap>,
    #[pyo3(get, set)]
    proxy_url: Option<String>,
    #[pyo3(get, set)]
    allow_http: bool,
    #[pyo3(get, set)]
    allow_insecure: bool,
    #[pyo3(get, set)]
    timeout: Option<u64>,
    #[pyo3(get, set)]
    connect_timeout: Option<u64>,
    #[pyo3(get, set)]
    pool_idle_timeout: Option<u64>,
    #[pyo3(get, set)]
    pool_max_idle_per_host: Option<usize>,
    #[pyo3(get, set)]
    http2_keep_alive_interval: Option<u64>,
    #[pyo3(get, set)]
    http2_keep_alive_timeout: Option<u64>,
    #[pyo3(get, set)]
    http2_keep_alive_while_idle: bool,
    #[pyo3(get, set)]
    http1_only: bool,
    #[pyo3(get, set)]
    http2_only: bool,
    #[pyo3(get, set)]
    retry_init_backoff: Option<u64>,
    #[pyo3(get, set)]
    retry_max_backoff: Option<u64>,
    #[pyo3(get, set)]
    retry_backoff_base: Option<f64>,
    #[pyo3(get, set)]
    retry_max_retries: Option<usize>,
    #[pyo3(get, set)]
    retry_timeout: Option<u64>,
}

impl PyClientOptions {
    fn client_options(&self) -> Result<ClientOptions, ObjectStoreError> {
        let mut options = ClientOptions::new()
            .with_allow_http(self.allow_http)
            .with_allow_invalid_certificates(self.allow_insecure);
        if let Some(user_agent) = &self.user_agent {
            options = options.with_user_agent(
                user_agent
                    .clone()
                    .try_into()
                    .map_err(|_| ObjectStoreError::InputValue(user_agent.into()))?,
            );
        }
        if let Some(default_content_type) = &self.default_content_type {
            options = options.with_default_content_type(default_content_type);
        }
        if let Some(proxy_url) = &self.proxy_url {
            options = options.with_proxy_url(proxy_url);
        }
        if let Some(timeout) = self.timeout {
            options = options.with_timeout(Duration::from_secs(timeout));
        }
        if let Some(connect_timeout) = self.connect_timeout {
            options = options.with_connect_timeout(Duration::from_secs(connect_timeout));
        }
        if let Some(pool_idle_timeout) = self.pool_idle_timeout {
            options = options.with_pool_idle_timeout(Duration::from_secs(pool_idle_timeout));
        }
        if let Some(pool_max_idle_per_host) = self.pool_max_idle_per_host {
            options = options.with_pool_max_idle_per_host(pool_max_idle_per_host);
        }
        if let Some(http2_keep_alive_interval) = self.http2_keep_alive_interval {
            options = options
                .with_http2_keep_alive_interval(Duration::from_secs(http2_keep_alive_interval));
        }
        if let Some(http2_keep_alive_timeout) = self.http2_keep_alive_timeout {
            options = options
                .with_http2_keep_alive_timeout(Duration::from_secs(http2_keep_alive_timeout));
        }
        if self.http2_keep_alive_while_idle {
            options = options.with_http2_keep_alive_while_idle();
        }
        if self.http1_only {
            options = options.with_http1_only();
        }
        if self.http2_only {
            options = options.with_http2_only();
        }
        Ok(options)
    }

    fn retry_config(&self) -> Result<RetryConfig, ObjectStoreError> {
        let mut backoff = BackoffConfig::default();
        if let Some(init_backoff) = self.retry_init_backoff {
            backoff.init_backoff = Duration::from_secs(init_backoff);
        }
        if let Some(max_backoff) = self.retry_max_backoff {
            backoff.max_backoff = Duration::from_secs(max_backoff);
        }
        if let Some(backoff_base) = self.retry_backoff_base {
            backoff.base = backoff_base;
        }
        let mut config = RetryConfig {
            backoff,
            ..Default::default()
        };
        if let Some(max_retries) = self.retry_max_retries {
            config.max_retries = max_retries;
        }
        if let Some(timeout) = self.retry_timeout {
            config.retry_timeout = Duration::from_secs(timeout);
        }
        Ok(config)
    }
}

impl TryFrom<PyClientOptions> for ClientOptions {
    type Error = ObjectStoreError;

    fn try_from(value: PyClientOptions) -> Result<ClientOptions, Self::Error> {
        let mut options = ClientOptions::new()
            .with_allow_http(value.allow_http)
            .with_allow_invalid_certificates(value.allow_insecure);
        if let Some(user_agent) = value.user_agent {
            options = options.with_user_agent(
                user_agent
                    .clone()
                    .try_into()
                    .map_err(|_| ObjectStoreError::InputValue(user_agent))?,
            );
        }
        if let Some(default_content_type) = value.default_content_type {
            options = options.with_default_content_type(default_content_type);
        }
        if let Some(proxy_url) = value.proxy_url {
            options = options.with_proxy_url(proxy_url);
        }
        if let Some(timeout) = value.timeout {
            options = options.with_timeout(Duration::from_secs(timeout));
        }
        if let Some(connect_timeout) = value.connect_timeout {
            options = options.with_connect_timeout(Duration::from_secs(connect_timeout));
        }
        if let Some(pool_idle_timeout) = value.pool_idle_timeout {
            options = options.with_pool_idle_timeout(Duration::from_secs(pool_idle_timeout));
        }
        if let Some(pool_max_idle_per_host) = value.pool_max_idle_per_host {
            options = options.with_pool_max_idle_per_host(pool_max_idle_per_host);
        }
        if let Some(http2_keep_alive_interval) = value.http2_keep_alive_interval {
            options = options
                .with_http2_keep_alive_interval(Duration::from_secs(http2_keep_alive_interval));
        }
        if let Some(http2_keep_alive_timeout) = value.http2_keep_alive_timeout {
            options = options
                .with_http2_keep_alive_timeout(Duration::from_secs(http2_keep_alive_timeout));
        }
        if value.http2_keep_alive_while_idle {
            options = options.with_http2_keep_alive_while_idle();
        }
        if value.http1_only {
            options = options.with_http1_only();
        }
        if value.http2_only {
            options = options.with_http2_only();
        }
        Ok(options)
    }
}

#[pymethods]
impl PyClientOptions {
    #[new]
    #[pyo3(signature = (
        user_agent = None,
        content_type_map = None,
        default_content_type = None,
        proxy_url = None,
        allow_http = false,
        allow_insecure = false,
        timeout = None,
        connect_timeout = None,
        pool_idle_timeout = None,
        pool_max_idle_per_host = None,
        http2_keep_alive_interval = None,
        http2_keep_alive_timeout = None,
        http2_keep_alive_while_idle = false,
        http1_only = false,
        http2_only = false,
        retry_init_backoff = None,
        retry_max_backoff = None,
        retry_backoff_base = None,
        retry_max_retries = None,
        retry_timeout = None,
    ))]
    /// Create a new ObjectStore instance
    #[allow(clippy::too_many_arguments)]
    fn new(
        user_agent: Option<String>,
        content_type_map: Option<HashMap<String, String>>,
        default_content_type: Option<String>,
        proxy_url: Option<String>,
        allow_http: bool,
        allow_insecure: bool,
        timeout: Option<u64>,
        connect_timeout: Option<u64>,
        pool_idle_timeout: Option<u64>,
        pool_max_idle_per_host: Option<usize>,
        http2_keep_alive_interval: Option<u64>,
        http2_keep_alive_timeout: Option<u64>,
        http2_keep_alive_while_idle: bool,
        http1_only: bool,
        http2_only: bool,
        retry_init_backoff: Option<u64>,
        retry_max_backoff: Option<u64>,
        retry_backoff_base: Option<f64>,
        retry_max_retries: Option<usize>,
        retry_timeout: Option<u64>,
    ) -> Self {
        Self {
            user_agent,
            content_type_map: content_type_map.unwrap_or_default(),
            default_content_type,
            proxy_url,
            allow_http,
            allow_insecure,
            timeout,
            connect_timeout,
            pool_idle_timeout,
            pool_max_idle_per_host,
            http2_keep_alive_interval,
            http2_keep_alive_timeout,
            http2_keep_alive_while_idle,
            http1_only,
            http2_only,
            retry_init_backoff,
            retry_max_backoff,
            retry_backoff_base,
            retry_max_retries,
            retry_timeout,
        }
    }
}

#[pyclass(name = "ObjectStore", subclass, module = "geoarrow.rust.core._rust")]
#[derive(Debug, Clone)]
/// A generic object store interface for uniformly interacting with AWS S3, Google Cloud Storage,
/// Azure Blob Storage and local files.
pub struct PyObjectStore {
    pub inner: Arc<DynObjectStore>,
    pub rt: Arc<Runtime>,
    root_url: String,
    options: Option<HashMap<String, String>>,
}

#[pymethods]
impl PyObjectStore {
    /// Create a new ObjectStore instance
    ///
    /// Args:
    ///     root: the root path of the object store. This may include only the bucket name/hostname
    ///         or optionally an additional prefix to be used for further requests. E.g. it could
    ///         be either `s3://bucket` or `s3://bucket/prefix`.
    ///     options: a dict of options (e.g. authentication settings) for connecting to the object store.
    #[new]
    fn new(
        root: String,
        options: Option<HashMap<String, String>>,
        client_options: Option<PyClientOptions>,
    ) -> PyResult<Self> {
        let client_options = client_options.unwrap_or_default();
        let inner = ObjectStoreBuilder::new(root.clone())
            .with_path_as_prefix(true)
            .with_options(options.clone().unwrap_or_default())
            .with_client_options(client_options.client_options()?)
            .with_retry_config(client_options.retry_config()?)
            .build()
            .map_err(ObjectStoreError::from)?;
        Ok(Self {
            root_url: root,
            inner,
            rt: Arc::new(Runtime::new()?),
            options,
        })
    }

    pub fn __getnewargs__(&self) -> PyResult<(String, Option<HashMap<String, String>>)> {
        Ok((self.root_url.clone(), self.options.clone()))
    }
}
