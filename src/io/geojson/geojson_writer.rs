//! Vendored from geozero under the MIT/Apache 2 license until
//! <https://github.com/georust/geozero/pull/208> is merged and released.

use geozero::error::Result;
use geozero::{ColumnValue, CoordDimensions, FeatureProcessor, GeomProcessor, PropertyProcessor};
use std::fmt::Display;
use std::io::Write;

/// GeoJSON writer.
pub struct GeoJsonWriter<W: Write> {
    dims: CoordDimensions,
    pub(crate) out: W,
}

impl<W: Write> GeoJsonWriter<W> {
    pub fn new(out: W) -> Self {
        GeoJsonWriter {
            dims: CoordDimensions::default(),
            out,
        }
    }
    #[allow(dead_code)]
    pub fn with_dims(out: W, dims: CoordDimensions) -> Self {
        GeoJsonWriter { dims, out }
    }
    fn comma(&mut self, idx: usize) -> Result<()> {
        if idx > 0 {
            self.out.write_all(b",")?;
        }
        Ok(())
    }
}

impl<W: Write> FeatureProcessor for GeoJsonWriter<W> {
    fn dataset_begin(&mut self, name: Option<&str>) -> Result<()> {
        self.out.write_all(
            br#"{
"type": "FeatureCollection""#,
        )?;
        if let Some(name) = name {
            write!(self.out, ",\n\"name\": \"{name}\"")?;
        }
        self.out.write_all(
            br#",
"features": ["#,
        )?;
        Ok(())
    }
    fn dataset_end(&mut self) -> Result<()> {
        self.out.write_all(b"]}")?;
        Ok(())
    }
    fn feature_begin(&mut self, idx: u64) -> Result<()> {
        if idx > 0 {
            self.out.write_all(b",\n")?;
        }
        self.out.write_all(br#"{"type": "Feature""#)?;
        Ok(())
    }
    fn feature_end(&mut self, _idx: u64) -> Result<()> {
        self.out.write_all(b"}")?;
        Ok(())
    }
    fn properties_begin(&mut self) -> Result<()> {
        self.out.write_all(br#", "properties": {"#)?;
        Ok(())
    }
    fn properties_end(&mut self) -> Result<()> {
        self.out.write_all(b"}")?;
        Ok(())
    }
    fn geometry_begin(&mut self) -> Result<()> {
        self.out.write_all(br#", "geometry": "#)?;
        Ok(())
    }
    fn geometry_end(&mut self) -> Result<()> {
        Ok(())
    }
}

impl<W: Write> GeomProcessor for GeoJsonWriter<W> {
    fn dimensions(&self) -> CoordDimensions {
        self.dims
    }
    fn xy(&mut self, x: f64, y: f64, idx: usize) -> Result<()> {
        self.comma(idx)?;
        self.out.write_all(format!("[{x},{y}]").as_bytes())?;
        Ok(())
    }
    fn coordinate(
        &mut self,
        x: f64,
        y: f64,
        z: Option<f64>,
        _m: Option<f64>,
        _t: Option<f64>,
        _tm: Option<u64>,
        idx: usize,
    ) -> Result<()> {
        self.comma(idx)?;
        self.out.write_all(format!("[{x},{y}").as_bytes())?;
        if let Some(z) = z {
            self.out.write_all(format!(",{z}").as_bytes())?;
        }
        self.out.write_all(b"]")?;
        Ok(())
    }
    fn empty_point(&mut self, idx: usize) -> Result<()> {
        self.comma(idx)?;
        self.out
            .write_all(br#"{"type": "Point", "coordinates": []}"#)?;
        Ok(())
    }
    fn point_begin(&mut self, idx: usize) -> Result<()> {
        self.comma(idx)?;
        self.out
            .write_all(br#"{"type": "Point", "coordinates": "#)?;
        Ok(())
    }
    fn point_end(&mut self, _idx: usize) -> Result<()> {
        self.out.write_all(b"}")?;
        Ok(())
    }
    fn multipoint_begin(&mut self, _size: usize, idx: usize) -> Result<()> {
        self.comma(idx)?;
        self.out
            .write_all(br#"{"type": "MultiPoint", "coordinates": ["#)?;
        Ok(())
    }
    fn multipoint_end(&mut self, _idx: usize) -> Result<()> {
        self.out.write_all(b"]}")?;
        Ok(())
    }
    fn linestring_begin(&mut self, tagged: bool, _size: usize, idx: usize) -> Result<()> {
        self.comma(idx)?;
        if tagged {
            self.out
                .write_all(br#"{"type": "LineString", "coordinates": ["#)?;
        } else {
            self.out.write_all(b"[")?;
        }
        Ok(())
    }
    fn linestring_end(&mut self, tagged: bool, _idx: usize) -> Result<()> {
        if tagged {
            self.out.write_all(b"]}")?;
        } else {
            self.out.write_all(b"]")?;
        }
        Ok(())
    }
    fn multilinestring_begin(&mut self, _size: usize, idx: usize) -> Result<()> {
        self.comma(idx)?;
        self.out
            .write_all(br#"{"type": "MultiLineString", "coordinates": ["#)?;
        Ok(())
    }
    fn multilinestring_end(&mut self, _idx: usize) -> Result<()> {
        self.out.write_all(b"]}")?;
        Ok(())
    }
    fn polygon_begin(&mut self, tagged: bool, _size: usize, idx: usize) -> Result<()> {
        self.comma(idx)?;
        if tagged {
            self.out
                .write_all(br#"{"type": "Polygon", "coordinates": ["#)?;
        } else {
            self.out.write_all(b"[")?;
        }
        Ok(())
    }
    fn polygon_end(&mut self, tagged: bool, _idx: usize) -> Result<()> {
        if tagged {
            self.out.write_all(b"]}")?;
        } else {
            self.out.write_all(b"]")?;
        }
        Ok(())
    }
    fn multipolygon_begin(&mut self, _size: usize, idx: usize) -> Result<()> {
        self.comma(idx)?;
        self.out
            .write_all(br#"{"type": "MultiPolygon", "coordinates": ["#)?;
        Ok(())
    }
    fn multipolygon_end(&mut self, _idx: usize) -> Result<()> {
        self.out.write_all(b"]}")?;
        Ok(())
    }
    fn geometrycollection_begin(&mut self, _size: usize, idx: usize) -> Result<()> {
        self.comma(idx)?;
        self.out
            .write_all(br#"{"type": "GeometryCollection", "geometries": ["#)?;
        Ok(())
    }
    fn geometrycollection_end(&mut self, _idx: usize) -> Result<()> {
        self.out.write_all(b"]}")?;
        Ok(())
    }
}

fn write_num_prop<W: Write>(mut out: W, colname: &str, v: &dyn Display) -> Result<()> {
    let colname = colname.replace('\"', "\\\"");
    out.write_all(format!(r#""{colname}": {v}"#).as_bytes())?;
    Ok(())
}

fn write_str_prop<W: Write>(mut out: W, colname: &str, v: &str) -> Result<()> {
    let colname = colname.replace('\"', "\\\"");
    let value = v.replace('\"', "\\\"");
    out.write_all(format!(r#""{colname}": "{value}""#).as_bytes())?;
    Ok(())
}

fn write_json_prop<W: Write>(mut out: W, colname: &str, v: &str) -> Result<()> {
    let colname = colname.replace('\"', "\\\"");
    out.write_all(format!(r#""{colname}": {v}"#).as_bytes())?;
    Ok(())
}

impl<W: Write> PropertyProcessor for GeoJsonWriter<W> {
    fn property(&mut self, i: usize, colname: &str, colval: &ColumnValue) -> Result<bool> {
        if i > 0 {
            self.out.write_all(b", ")?;
        }
        match colval {
            ColumnValue::Byte(v) => write_num_prop(&mut self.out, colname, &v)?,
            ColumnValue::UByte(v) => write_num_prop(&mut self.out, colname, &v)?,
            ColumnValue::Bool(v) => write_num_prop(&mut self.out, colname, &v)?,
            ColumnValue::Short(v) => write_num_prop(&mut self.out, colname, &v)?,
            ColumnValue::UShort(v) => write_num_prop(&mut self.out, colname, &v)?,
            ColumnValue::Int(v) => write_num_prop(&mut self.out, colname, &v)?,
            ColumnValue::UInt(v) => write_num_prop(&mut self.out, colname, &v)?,
            ColumnValue::Long(v) => write_num_prop(&mut self.out, colname, &v)?,
            ColumnValue::ULong(v) => write_num_prop(&mut self.out, colname, &v)?,
            ColumnValue::Float(v) => write_num_prop(&mut self.out, colname, &v)?,
            ColumnValue::Double(v) => write_num_prop(&mut self.out, colname, &v)?,
            ColumnValue::String(v) | ColumnValue::DateTime(v) => {
                write_str_prop(&mut self.out, colname, v)?;
            }
            ColumnValue::Json(v) => write_json_prop(&mut self.out, colname, v)?,
            ColumnValue::Binary(_v) => (),
        };
        Ok(false)
    }
}

// Note: we excluded the upstream geozero geojson writer tests
