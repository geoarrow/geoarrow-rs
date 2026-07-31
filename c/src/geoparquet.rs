//! GeoParquet reading and writing.
//!
//! Readers and writers are opaque handles created by an open call and destroyed
//! by close or finish. A batch crosses the boundary as one struct-typed
//! `ArrowArray`/`ArrowSchema` pair, with each column a child of the struct.
#![allow(
    clippy::missing_safety_doc,
    reason = "the shared pointer contract is documented on the module"
)]

use std::ffi::CStr;
use std::fs::File;
use std::os::raw::{c_char, c_void};
use std::slice;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, RecordBatchReader, StructArray};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{DataType, Field, Schema};
use geo_types::{Rect, coord};
use geoarrow_schema::CoordType;
use geoparquet::reader::{GeoParquetReaderBuilder, GeoParquetRecordBatchReader};
use geoparquet::writer::{
    GeoParquetRecordBatchEncoder, GeoParquetWriterEncoding, GeoParquetWriterOptionsBuilder,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::file::reader::ChunkReader;

use crate::error::{Error, GEOARROW_OK, GeoArrowError, GeoArrowErrorCode, catching, finish};
use crate::marshal::{Slot, consume_arrow, write};
use crate::types::coord_type;
use crate::{ArrowArray, ArrowSchema};

/// The Arrow field name given to a batch, which is a struct of the columns.
const BATCH: &str = "geoparquet_batch";

/// Reader settings. Every field is optional: zero, or null for the pointers,
/// selects the default.
#[repr(C)]
pub struct GeoArrowRsGeoParquetReadOptions {
    /// Non-zero to restrict the scan to rows whose bounding box intersects the
    /// `bbox_*` rectangle.
    pub has_bbox: u8,
    pub bbox_xmin: f64,
    pub bbox_ymin: f64,
    pub bbox_xmax: f64,
    pub bbox_ymax: f64,
    /// Array of `projection_n` NUL-terminated column names to read, or null for
    /// every column.
    pub projection_columns: *const *const c_char,
    pub projection_n: u32,
    /// Array of `row_groups_n` row-group indices to scan, or null for all.
    pub row_groups: *const u32,
    pub row_groups_n: u32,
    /// Rows per batch, or 0 for the parquet default.
    pub batch_size: u64,
    /// Non-zero to parse geometry columns into their native geoarrow type
    /// rather than leaving them as `geoarrow.wkb`.
    pub parse_to_native: u8,
    /// Coordinate layout for native parsing, as a
    /// [`crate::types::GeoArrowCoordType`]. Read only when `parse_to_native` is
    /// set.
    pub coord_type: i32,
}

/// Writer settings.
#[repr(C)]
pub struct GeoArrowRsGeoParquetWriteOptions {
    /// 0 writes WKB, which GeoParquet 1.0 readers understand; 1 writes native
    /// geoarrow, which needs 1.1.
    pub encoding: u8,
    /// Rows per row group, or 0 for the parquet default.
    pub row_group_size: u64,
    /// NUL-terminated name of the primary geometry column, or null to let the
    /// writer choose.
    pub primary_column: *const c_char,
    /// Non-zero to write bbox covering columns.
    pub generate_covering: u8,
}

struct Reader {
    inner: GeoParquetRecordBatchReader,
}

struct Writer {
    encoder: GeoParquetRecordBatchEncoder,
    inner: ArrowWriter<File>,
}

/// Entry-point scaffolding shared by the fallible functions here: each listed
/// pointer is rejected when null, the body runs under `catching`, and the
/// result goes out through `finish`.
macro_rules! entry {
    ($($(#[$doc:meta])* fn $c:ident($($p:ident: $t:ty),* $(,)?) requires ($($nn:ident),+) $body:block)*) => {$(
        $(#[$doc])*
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $c(
            $($p: $t,)*
            error: *mut GeoArrowError,
        ) -> GeoArrowErrorCode {
            let result = catching(|| {
                $(if $nn.is_null() {
                    return Err(Error::invalid(concat!(stringify!($nn), " is null")));
                })+
                $body
            });
            finish(result, error)
        }
    )*};
}

fn open<R: ChunkReader + 'static>(
    source: R,
    options: *const GeoArrowRsGeoParquetReadOptions,
) -> Result<Reader, Error> {
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(source)
        .map_err(|e| Error::io(format!("open parquet: {e}")))?;

    let metadata = builder
        .geoparquet_metadata()
        .ok_or_else(|| Error::invalid("file has no GeoParquet metadata (`geo` key missing)"))?
        .map_err(|e| Error::invalid(format!("parse GeoParquet metadata: {e}")))?;

    let options = unsafe { options.as_ref() };
    let parse_to_native = options.is_some_and(|o| o.parse_to_native != 0);
    // `coord_type` is documented as read only when `parse_to_native` is set, and
    // zero anywhere in the struct selects the default (Separated, the GeoArrow
    // default layout, which `geoarrow_schema` needs even for a WKB read).
    let coords = match options {
        Some(o) if parse_to_native && o.coord_type != 0 => coord_type(o.coord_type)?,
        _ => CoordType::Separated,
    };

    // Remembered so the inferred target schema can be pruned to match what the
    // projected reader will actually yield. None means no projection.
    let mut projected: Option<Vec<String>> = None;

    if let Some(options) = options {
        if !options.projection_columns.is_null() && options.projection_n > 0 {
            let names =
                unsafe { strings(options.projection_columns, options.projection_n as usize) }?;
            let schema = builder.parquet_schema().clone();
            let mut roots = Vec::with_capacity(names.len());
            for name in &names {
                let root = schema
                    .root_schema()
                    .get_fields()
                    .iter()
                    .position(|f| f.name() == name)
                    .ok_or_else(|| {
                        Error::invalid(format!("projection column not found: {name:?}"))
                    })?;
                roots.push(root);
            }
            // Root indices keep every leaf under a column; a leaf index would
            // truncate a nested column (any natively encoded geometry) to its
            // first leaf.
            builder = builder.with_projection(ProjectionMask::roots(&schema, roots));
            projected = Some(names);
        }
        if !options.row_groups.is_null() && options.row_groups_n > 0 {
            let groups =
                unsafe { slice::from_raw_parts(options.row_groups, options.row_groups_n as usize) };
            builder = builder.with_row_groups(groups.iter().map(|&i| i as usize).collect());
        }
        if options.batch_size > 0 {
            builder = builder.with_batch_size(options.batch_size as usize);
        }
        if options.has_bbox != 0 {
            let bbox = Rect::new(
                coord! { x: options.bbox_xmin, y: options.bbox_ymin },
                coord! { x: options.bbox_xmax, y: options.bbox_ymax },
            );
            builder = builder
                .with_intersecting_row_filter(bbox, &metadata, None)
                .map_err(|e| Error::invalid(format!("apply bbox filter: {e}")))?;
        }
    }

    // Inferred after projection, so the field count matches the projected read.
    let target = builder
        .geoarrow_schema(&metadata, parse_to_native, coords)
        .map_err(|e| Error::invalid(format!("infer geoarrow schema: {e}")))?;
    let target = match &projected {
        None => target,
        Some(names) => Arc::new(project(&target, names)?),
    };

    let reader = builder
        .build()
        .map_err(|e| Error::io(format!("build parquet reader: {e}")))?;
    Ok(Reader {
        inner: GeoParquetRecordBatchReader::try_new(reader, target)
            .map_err(|e| Error::invalid(format!("wrap geoparquet reader: {e}")))?,
    })
}

/// Keep only the named fields, in the order given.
fn project(schema: &Schema, keep: &[String]) -> Result<Schema, Error> {
    let mut fields = Vec::with_capacity(keep.len());
    for name in keep {
        fields.push(
            schema
                .field_with_name(name)
                .map_err(|e| Error::invalid(format!("project schema: {e}")))?
                .clone(),
        );
    }
    Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// # Safety
/// `p` must be a NUL-terminated string.
unsafe fn utf8<'a>(p: *const c_char, what: &str) -> Result<&'a str, Error> {
    unsafe { CStr::from_ptr(p) }
        .to_str()
        .map_err(|e| Error::invalid(format!("{what} is not valid UTF-8: {e}")))
}

/// # Safety
/// `ptrs` must address `n` readable, NUL-terminated string pointers.
unsafe fn strings(ptrs: *const *const c_char, n: usize) -> Result<Vec<String>, Error> {
    unsafe { slice::from_raw_parts(ptrs, n) }
        .iter()
        .enumerate()
        .map(|(i, &p)| {
            if p.is_null() {
                return Err(Error::invalid(format!("string[{i}] is null")));
            }
            unsafe { CStr::from_ptr(p) }
                .to_str()
                .map(str::to_owned)
                .map_err(|e| Error::invalid(format!("string[{i}] is not valid UTF-8: {e}")))
        })
        .collect()
}

entry! {
    /// # Safety
    /// `path` must be a NUL-terminated UTF-8 string, `options` null or readable,
    /// and `out_reader` a writable pointer slot. On success the slot receives a
    /// handle to release with [`GeoArrowRsGeoParquetReaderClose`].
    fn GeoArrowRsGeoParquetReaderOpenPath(
        path: *const c_char,
        options: *const GeoArrowRsGeoParquetReadOptions,
        out_reader: *mut *mut c_void,
    ) requires (path, out_reader) {
        let path = unsafe { utf8(path, "path") }?;
        let file = File::open(path).map_err(|e| Error::io(format!("open {path}: {e}")))?;
        let reader = open(file, options)?;
        unsafe { *out_reader = Box::into_raw(Box::new(reader)) as *mut c_void };
        Ok(())
    }

    /// Open a reader over an in-memory parquet file. The bytes are copied, so
    /// the caller may free them as soon as this returns.
    ///
    /// # Safety
    /// `bytes` must address `len` readable bytes, or be null when `len` is 0.
    /// `options` may be null. `out_reader` must be a writable pointer slot.
    fn GeoArrowRsGeoParquetReaderOpenBytes(
        bytes: *const u8,
        len: usize,
        options: *const GeoArrowRsGeoParquetReadOptions,
        out_reader: *mut *mut c_void,
    ) requires (out_reader) {
        if bytes.is_null() && len > 0 {
            return Err(Error::invalid("bytes is null but len is not 0"));
        }
        let source = match len {
            0 => bytes::Bytes::new(),
            _ => bytes::Bytes::copy_from_slice(unsafe { slice::from_raw_parts(bytes, len) }),
        };
        let reader = open(source, options)?;
        unsafe { *out_reader = Box::into_raw(Box::new(reader)) as *mut c_void };
        Ok(())
    }

    /// Write the reader's schema, a struct with one child per column, into
    /// `out_schema`. Release it with [`crate::GeoArrowRsSchemaRelease`].
    ///
    /// # Safety
    /// `reader` must be a live handle and `out_schema` a writable, zeroed
    /// [`ArrowSchema`].
    fn GeoArrowRsGeoParquetReaderSchema(
        reader: *mut c_void,
        out_schema: *mut ArrowSchema,
    ) requires (reader, out_schema) {
        let reader = unsafe { &*(reader as *mut Reader) };
        let schema = reader.inner.schema();
        let field = Field::new(BATCH, DataType::Struct(schema.fields().clone()), false)
            .with_metadata(schema.metadata().clone());
        unsafe { std::ptr::write(out_schema as *mut FFI_ArrowSchema, (&field).try_into()?) };
        Ok(())
    }

    /// Read the next batch into the output pair.
    ///
    /// End of stream is reported the way the Arrow C Stream Interface reports
    /// it: the call returns [`GEOARROW_OK`] and leaves `out_array->release`
    /// null. Check that before using the batch.
    ///
    /// # Safety
    /// `reader` must be a live handle, and the output pair writable and zeroed.
    fn GeoArrowRsGeoParquetReaderNext(
        reader: *mut c_void,
        out_array: *mut ArrowArray,
        out_schema: *mut ArrowSchema,
    ) requires (reader, out_array, out_schema) {
        let reader = unsafe { &mut *(reader as *mut Reader) };
        let Some(batch) = reader.inner.next() else {
            unsafe { std::ptr::write_bytes(out_array, 0, 1) };
            unsafe { std::ptr::write_bytes(out_schema, 0, 1) };
            return Ok(());
        };
        let batch = batch.map_err(|e| Error::io(format!("read batch: {e}")))?;
        let data = StructArray::from(batch).to_data();
        let field = Field::new(BATCH, data.data_type().clone(), false);
        unsafe { write(data, field, Slot::new(out_array, out_schema)) }
    }

    /// Open a writer over `path`. `schema` describes all columns as a struct,
    /// typically taken from [`GeoArrowRsGeoParquetReaderSchema`].
    ///
    /// # Safety
    /// `path` must be NUL-terminated UTF-8, `schema` a readable struct-typed
    /// [`ArrowSchema`], `options` null or readable, and `out_writer` a writable
    /// pointer slot. On success, finish the writer with
    /// [`GeoArrowRsGeoParquetWriterFinish`].
    fn GeoArrowRsGeoParquetWriterOpen(
        path: *const c_char,
        schema: *const ArrowSchema,
        options: *const GeoArrowRsGeoParquetWriteOptions,
        out_writer: *mut *mut c_void,
    ) requires (path, schema, out_writer) {
        let path = unsafe { utf8(path, "path") }?;

        let field = Field::try_from(unsafe { &*(schema as *const FFI_ArrowSchema) })?;
        let DataType::Struct(fields) = field.data_type() else {
            return Err(Error::invalid(format!(
                "expected a struct-typed schema, got {:?}",
                field.data_type()
            )));
        };
        let schema = Schema::new_with_metadata(fields.clone(), field.metadata().clone());

        let mut settings = GeoParquetWriterOptionsBuilder::default();
        let mut properties = parquet::file::properties::WriterProperties::builder();
        if let Some(options) = unsafe { options.as_ref() } {
            settings = settings.set_encoding(match options.encoding {
                0 => GeoParquetWriterEncoding::WKB,
                1 => GeoParquetWriterEncoding::GeoArrow,
                n => return Err(Error::invalid(format!("unsupported encoding: {n}"))),
            });
            if !options.primary_column.is_null() {
                let name = unsafe { utf8(options.primary_column, "primary_column") }?;
                settings = settings.set_primary_column(name.to_owned());
            }
            if options.generate_covering != 0 {
                settings = settings.set_generate_covering(true);
            }
            if options.row_group_size > 0 {
                properties =
                    properties.set_max_row_group_row_count(Some(options.row_group_size as usize));
            }
        }

        let encoder = GeoParquetRecordBatchEncoder::try_new(&schema, &settings.build())
            .map_err(|e| Error::invalid(format!("build geoparquet encoder: {e}")))?;
        let file = File::create(path).map_err(|e| Error::io(format!("create {path}: {e}")))?;
        let inner = ArrowWriter::try_new(file, encoder.target_schema(), Some(properties.build()))
            .map_err(|e| Error::io(format!("build parquet writer: {e}")))?;

        unsafe { *out_writer = Box::into_raw(Box::new(Writer { encoder, inner })) as *mut c_void };
        Ok(())
    }

    /// Encode and append a batch. The input pair is consumed.
    ///
    /// # Safety
    /// `writer` must be a live handle and the input pair a writable,
    /// struct-typed Arrow C Data pair.
    fn GeoArrowRsGeoParquetWriterPush(
        writer: *mut c_void,
        in_array: *mut ArrowArray,
        in_schema: *mut ArrowSchema,
    ) requires (writer, in_array, in_schema) {
        let writer = unsafe { &mut *(writer as *mut Writer) };

        let (array, _field) = unsafe { consume_arrow(Slot::new(in_array, in_schema)) }?;
        let Some(columns) = array.as_any().downcast_ref::<StructArray>() else {
            return Err(Error::invalid(format!(
                "expected a struct array, got {:?}",
                array.data_type()
            )));
        };
        // `RecordBatch::from` asserts a zero null count, and an assert in an
        // `extern "C" fn` would abort the caller's process.
        if columns.null_count() > 0 {
            return Err(Error::invalid(
                "batch struct array must not have top-level null rows",
            ));
        }

        let batch = RecordBatch::from(columns.clone());
        let encoded = writer
            .encoder
            .encode_record_batch(&batch)
            .map_err(|e| Error::invalid(format!("encode batch: {e}")))?;
        writer
            .inner
            .write(&encoded)
            .map_err(|e| Error::io(format!("write batch: {e}")))
    }
}

/// Release a reader handle. Null is ignored.
///
/// # Safety
/// `reader` must be null or a handle from an open call that has not been closed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoArrowRsGeoParquetReaderClose(reader: *mut c_void) {
    if !reader.is_null() {
        drop(unsafe { Box::from_raw(reader as *mut Reader) });
    }
}

/// Append the GeoParquet metadata, flush, close the file, and destroy the
/// handle. Null is ignored.
///
/// # Safety
/// `writer` must be null or a handle from
/// [`GeoArrowRsGeoParquetWriterOpen`] that has not been finished.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoArrowRsGeoParquetWriterFinish(
    writer: *mut c_void,
    error: *mut GeoArrowError,
) -> GeoArrowErrorCode {
    if writer.is_null() {
        return GEOARROW_OK;
    }
    let Writer { encoder, mut inner } = *unsafe { Box::from_raw(writer as *mut Writer) };
    let result = catching(|| {
        let metadata = encoder
            .into_keyvalue()
            .map_err(|e| Error::invalid(format!("build geoparquet metadata: {e}")))?;
        inner.append_key_value_metadata(metadata);
        inner
            .close()
            .map(|_| ())
            .map_err(|e| Error::io(format!("close parquet writer: {e}")))
    });
    finish(result, error)
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::ptr::{null, null_mut};

    use arrow_array::cast::AsArray;
    use arrow_array::ffi::{from_ffi, to_ffi};
    use arrow_array::{Int32Array, StructArray, make_array};
    use arrow_buffer::NullBuffer;
    use arrow_schema::Fields;
    use geoarrow_array::{GeoArrowArray, test};
    use geoarrow_schema::{CoordType, Dimension};

    use super::*;
    use crate::error::EINVAL;

    fn sample_batch(nulls: Option<NullBuffer>) -> (StructArray, Field) {
        let geometry = test::point::array(CoordType::Separated, Dimension::XY);
        let geometry_field = geometry.data_type().to_field("geometry", true);
        let ids = Int32Array::from_iter_values(0..geometry.len() as i32);
        let fields = Fields::from(vec![
            geometry_field,
            Field::new("id", DataType::Int32, false),
        ]);
        let arrays = vec![geometry.to_array_ref(), std::sync::Arc::new(ids) as _];
        let batch = StructArray::new(fields.clone(), arrays, nulls);
        let field = Field::new(BATCH, DataType::Struct(fields), true);
        (batch, field)
    }

    fn ffi_pair(
        batch: &StructArray,
        field: &Field,
    ) -> (arrow_data::ffi::FFI_ArrowArray, FFI_ArrowSchema) {
        let (array, _stripped) = to_ffi(&batch.to_data()).unwrap();
        let schema = FFI_ArrowSchema::try_from(field).unwrap();
        (array, schema)
    }

    fn write_fixture(path: &CString, encoding: u8) {
        let (batch, field) = sample_batch(None);
        let (mut in_array, mut in_schema) = ffi_pair(&batch, &field);
        let schema_ffi = FFI_ArrowSchema::try_from(&field).unwrap();
        let options = GeoArrowRsGeoParquetWriteOptions {
            encoding,
            row_group_size: 0,
            primary_column: null(),
            generate_covering: 0,
        };
        let mut writer: *mut c_void = null_mut();
        let rc = unsafe {
            GeoArrowRsGeoParquetWriterOpen(
                path.as_ptr(),
                &schema_ffi as *const FFI_ArrowSchema as *const ArrowSchema,
                &options,
                &mut writer,
                null_mut(),
            )
        };
        assert_eq!(rc, GEOARROW_OK);
        let rc = unsafe {
            GeoArrowRsGeoParquetWriterPush(
                writer,
                &mut in_array as *mut arrow_data::ffi::FFI_ArrowArray as *mut ArrowArray,
                &mut in_schema as *mut FFI_ArrowSchema as *mut ArrowSchema,
                null_mut(),
            )
        };
        assert_eq!(rc, GEOARROW_OK);
        assert_eq!(
            unsafe { GeoArrowRsGeoParquetWriterFinish(writer, null_mut()) },
            GEOARROW_OK
        );
    }

    fn read_rows(path: &CString, options: *const GeoArrowRsGeoParquetReadOptions) -> StructArray {
        let mut reader: *mut c_void = null_mut();
        let mut sink = GeoArrowError { message: [0; 1024] };
        let rc = unsafe {
            GeoArrowRsGeoParquetReaderOpenPath(path.as_ptr(), options, &mut reader, &mut sink)
        };
        assert_eq!(rc, GEOARROW_OK, "open failed: {}", unsafe {
            CStr::from_ptr(sink.message.as_ptr()).to_string_lossy()
        });
        let mut out_array = arrow_data::ffi::FFI_ArrowArray::empty();
        let mut out_schema = FFI_ArrowSchema::empty();
        let rc = unsafe {
            GeoArrowRsGeoParquetReaderNext(
                reader,
                &mut out_array as *mut arrow_data::ffi::FFI_ArrowArray as *mut ArrowArray,
                &mut out_schema as *mut FFI_ArrowSchema as *mut ArrowSchema,
                &mut sink,
            )
        };
        assert_eq!(rc, GEOARROW_OK, "next failed: {}", unsafe {
            CStr::from_ptr(sink.message.as_ptr()).to_string_lossy()
        });
        let data = unsafe { from_ffi(out_array, &out_schema) }.unwrap();
        let array = make_array(data);
        unsafe { GeoArrowRsGeoParquetReaderClose(reader) };
        array.as_struct().clone()
    }

    fn temp_path(name: &str) -> CString {
        let path = std::env::temp_dir().join(format!(
            "geoarrow_rs_{}_{}.parquet",
            name,
            std::process::id()
        ));
        CString::new(path.to_str().unwrap()).unwrap()
    }

    /// A fully zero-initialized options struct is documented to select every
    /// default; it must not be rejected over the unread `coord_type` field.
    #[test]
    fn zero_initialized_read_options_select_defaults() {
        let path = temp_path("zero_options");
        write_fixture(&path, 0);
        let options = GeoArrowRsGeoParquetReadOptions {
            has_bbox: 0,
            bbox_xmin: 0.0,
            bbox_ymin: 0.0,
            bbox_xmax: 0.0,
            bbox_ymax: 0.0,
            projection_columns: null(),
            projection_n: 0,
            row_groups: null(),
            row_groups_n: 0,
            batch_size: 0,
            parse_to_native: 0,
            coord_type: 0,
        };
        let batch = read_rows(&path, &options);
        assert_eq!(batch.num_columns(), 2);
        assert!(batch.len() > 0);
        std::fs::remove_file(path.to_str().unwrap()).ok();
    }

    /// Projecting a natively encoded geometry column must keep every parquet
    /// leaf under it, not truncate the column to its first leaf.
    #[test]
    fn projection_keeps_a_native_geometry_column_whole() {
        let path = temp_path("projection");
        write_fixture(&path, 1);
        let column = CString::new("geometry").unwrap();
        let columns = [column.as_ptr()];
        let options = GeoArrowRsGeoParquetReadOptions {
            has_bbox: 0,
            bbox_xmin: 0.0,
            bbox_ymin: 0.0,
            bbox_xmax: 0.0,
            bbox_ymax: 0.0,
            projection_columns: columns.as_ptr(),
            projection_n: 1,
            row_groups: null(),
            row_groups_n: 0,
            batch_size: 0,
            parse_to_native: 1,
            coord_type: 0,
        };
        let batch = read_rows(&path, &options);
        assert_eq!(batch.num_columns(), 1);
        assert!(batch.len() > 0);
        std::fs::remove_file(path.to_str().unwrap()).ok();
    }

    /// `RecordBatch::from` asserts a zero null count; a top-level-null batch
    /// must come back as EINVAL, not abort the host process.
    #[test]
    fn push_rejects_top_level_null_rows() {
        let path = temp_path("null_rows");
        let (probe, field) = sample_batch(None);
        let nulls = NullBuffer::from_iter((0..probe.len()).map(|i| i != 0));
        let (batch, _) = sample_batch(Some(nulls));
        let (mut in_array, mut in_schema) = ffi_pair(&batch, &field);
        let schema_ffi = FFI_ArrowSchema::try_from(&field).unwrap();
        let mut writer: *mut c_void = null_mut();
        let rc = unsafe {
            GeoArrowRsGeoParquetWriterOpen(
                path.as_ptr(),
                &schema_ffi as *const FFI_ArrowSchema as *const ArrowSchema,
                null(),
                &mut writer,
                null_mut(),
            )
        };
        assert_eq!(rc, GEOARROW_OK);
        let mut sink = GeoArrowError { message: [0; 1024] };
        let rc = unsafe {
            GeoArrowRsGeoParquetWriterPush(
                writer,
                &mut in_array as *mut arrow_data::ffi::FFI_ArrowArray as *mut ArrowArray,
                &mut in_schema as *mut FFI_ArrowSchema as *mut ArrowSchema,
                &mut sink,
            )
        };
        assert_eq!(rc, EINVAL);
        assert_eq!(
            unsafe { GeoArrowRsGeoParquetWriterFinish(writer, null_mut()) },
            GEOARROW_OK
        );
        std::fs::remove_file(path.to_str().unwrap()).ok();
    }
}
