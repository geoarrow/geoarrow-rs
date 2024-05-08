use geojson::{Feature, FeatureReader};
use geojson::{GeoJson as GeoGeoJson, Geometry, Value};
use geozero::error::{GeozeroError, Result};
use geozero::{
    ColumnValue, FeatureProcessor, GeomProcessor, GeozeroDatasource, GeozeroGeometry,
    PropertyProcessor,
};
use serde_json::map::Map;
use serde_json::value::Value as JsonValue;
use std::io::Read;

/// GeoJSON String.
#[derive(Debug)]
pub struct GeoJsonString(pub String);

impl GeozeroGeometry for GeoJsonString {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> Result<()> {
        read_geojson_geom(&mut self.0.as_bytes(), processor)
    }
}

impl GeozeroDatasource for GeoJsonString {
    fn process<P: FeatureProcessor>(&mut self, processor: &mut P) -> Result<()> {
        read_geojson(&mut self.0.as_bytes(), processor)
    }
}

/// GeoJSON String slice.
pub struct GeoJson<'a>(pub &'a str);

impl GeozeroGeometry for GeoJson<'_> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> Result<()> {
        read_geojson_geom(&mut self.0.as_bytes(), processor)
    }
}

impl GeozeroDatasource for GeoJson<'_> {
    fn process<P: FeatureProcessor>(&mut self, processor: &mut P) -> Result<()> {
        read_geojson(&mut self.0.as_bytes(), processor)
    }
}

/// GeoJSON Reader.
pub struct GeoJsonReader<R: Read>(pub R);

impl<R: Read> GeozeroDatasource for GeoJsonReader<R> {
    fn process<P: FeatureProcessor>(&mut self, processor: &mut P) -> Result<()> {
        read_geojson(&mut self.0, processor)
    }
}

/// Read and process GeoJSON.
pub fn read_geojson<R: Read, P: FeatureProcessor>(mut reader: R, processor: &mut P) -> Result<()> {
    let mut geojson_str = String::new();
    reader.read_to_string(&mut geojson_str)?;
    let geojson = geojson_str.parse::<GeoGeoJson>()?;
    process_geojson(&geojson, processor)
}

#[allow(dead_code)]
pub fn read_geojson_fc<R: Read, P: FeatureProcessor>(reader: R, processor: &mut P) -> Result<()> {
    for (idx, feature) in FeatureReader::from_reader(reader).features().enumerate() {
        process_geojson_feature(&feature?, idx, processor)?;
    }

    Ok(())
}

/// Read and process GeoJSON geometry.
pub fn read_geojson_geom<R: Read, P: GeomProcessor>(
    reader: &mut R,
    processor: &mut P,
) -> Result<()> {
    let mut geojson_str = String::new();
    reader.read_to_string(&mut geojson_str)?;
    let geojson = geojson_str.parse::<GeoGeoJson>()?;
    process_geojson_geom(&geojson, processor)
}

/// Process top-level GeoJSON items
fn process_geojson<P: FeatureProcessor>(gj: &GeoGeoJson, processor: &mut P) -> Result<()> {
    match *gj {
        GeoGeoJson::FeatureCollection(ref collection) => {
            processor.dataset_begin(None)?;
            for (idx, feature) in collection.features.iter().enumerate() {
                processor.feature_begin(idx as u64)?;
                if let Some(ref properties) = feature.properties {
                    processor.properties_begin()?;
                    process_properties(properties, processor)?;
                    processor.properties_end()?;
                }
                if let Some(ref geometry) = feature.geometry {
                    processor.geometry_begin()?;
                    process_geojson_geom_n(geometry, idx, processor)?;
                    processor.geometry_end()?;
                }
                processor.feature_end(idx as u64)?;
            }
            processor.dataset_end()
        }
        GeoGeoJson::Feature(ref feature) => process_geojson_feature(feature, 0, processor),
        GeoGeoJson::Geometry(ref geometry) => process_geojson_geom_n(geometry, 0, processor),
    }
}

/// Process top-level GeoJSON items
fn process_geojson_feature<P: FeatureProcessor>(
    feature: &Feature,
    idx: usize,
    processor: &mut P,
) -> Result<()> {
    processor.dataset_begin(None)?;
    if feature.geometry.is_some() || feature.properties.is_some() {
        processor.feature_begin(idx as u64)?;
        if let Some(ref properties) = feature.properties {
            processor.properties_begin()?;
            process_properties(properties, processor)?;
            processor.properties_end()?;
        }
        if let Some(ref geometry) = feature.geometry {
            processor.geometry_begin()?;
            process_geojson_geom_n(geometry, idx, processor)?;
            processor.geometry_end()?;
        }
        processor.feature_end(idx as u64)?;
    }
    processor.dataset_end()
}

/// Process top-level GeoJSON items (geometry only)
fn process_geojson_geom<P: GeomProcessor>(gj: &GeoGeoJson, processor: &mut P) -> Result<()> {
    match *gj {
        GeoGeoJson::FeatureCollection(ref collection) => {
            for (idx, geometry) in collection
                .features
                .iter()
                // Only pass on non-empty geometries, doing so by reference
                .filter_map(|feature| feature.geometry.as_ref())
                .enumerate()
            {
                process_geojson_geom_n(geometry, idx, processor)?;
            }
        }
        GeoGeoJson::Feature(ref feature) => {
            if let Some(ref geometry) = feature.geometry {
                process_geojson_geom_n(geometry, 0, processor)?;
            }
        }
        GeoGeoJson::Geometry(ref geometry) => {
            process_geojson_geom_n(geometry, 0, processor)?;
        }
    }
    Ok(())
}

/// Process GeoJSON geometries
pub(crate) fn process_geojson_geom_n<P: GeomProcessor>(
    geom: &Geometry,
    idx: usize,
    processor: &mut P,
) -> Result<()> {
    match geom.value {
        Value::Point(ref geometry) => {
            processor.point_begin(idx)?;
            process_coord(geometry, processor.multi_dim(), 0, processor)?;
            processor.point_end(idx)
        }
        Value::MultiPoint(ref geometry) => {
            processor.multipoint_begin(geometry.len(), idx)?;
            let multi_dim = processor.multi_dim();
            for (idxc, point_type) in geometry.iter().enumerate() {
                process_coord(point_type, multi_dim, idxc, processor)?;
            }
            processor.multipoint_end(idx)
        }
        Value::LineString(ref geometry) => process_linestring(geometry, true, idx, processor),
        Value::MultiLineString(ref geometry) => {
            processor.multilinestring_begin(geometry.len(), idx)?;
            for (idx2, linestring_type) in geometry.iter().enumerate() {
                process_linestring(linestring_type, false, idx2, processor)?;
            }
            processor.multilinestring_end(idx)
        }
        Value::Polygon(ref geometry) => process_polygon(geometry, true, idx, processor),
        Value::MultiPolygon(ref geometry) => {
            processor.multipolygon_begin(geometry.len(), idx)?;
            for (idx2, polygon_type) in geometry.iter().enumerate() {
                process_polygon(polygon_type, false, idx2, processor)?;
            }
            processor.multipolygon_end(idx)
        }
        Value::GeometryCollection(ref collection) => {
            processor.geometrycollection_begin(collection.len(), idx)?;
            for (idx2, geometry) in collection.iter().enumerate() {
                process_geojson_geom_n(geometry, idx2, processor)?;
            }
            processor.geometrycollection_end(idx)
        }
    }
}

/// Process GeoJSON properties
pub(crate) fn process_properties<P: PropertyProcessor>(
    properties: &Map<String, JsonValue>,
    processor: &mut P,
) -> Result<()> {
    for (i, (key, value)) in properties.iter().enumerate() {
        // Could we provide a stable property index?
        match value {
            JsonValue::String(v) => processor.property(i, key, &ColumnValue::String(v))?,
            JsonValue::Number(v) => {
                if v.is_f64() {
                    processor.property(i, key, &ColumnValue::Double(v.as_f64().unwrap()))?
                } else if v.is_i64() {
                    processor.property(i, key, &ColumnValue::Long(v.as_i64().unwrap()))?
                } else if v.is_u64() {
                    processor.property(i, key, &ColumnValue::ULong(v.as_u64().unwrap()))?
                } else {
                    unreachable!()
                }
            }
            JsonValue::Bool(v) => processor.property(i, key, &ColumnValue::Bool(*v))?,
            JsonValue::Array(v) => {
                let json_string =
                    serde_json::to_string(v).map_err(|_err| GeozeroError::Property(key.clone()))?;
                processor.property(i, key, &ColumnValue::Json(&json_string))?
            }
            JsonValue::Object(v) => {
                let json_string =
                    serde_json::to_string(v).map_err(|_err| GeozeroError::Property(key.clone()))?;
                processor.property(i, key, &ColumnValue::Json(&json_string))?
            }
            // For null values omit the property
            JsonValue::Null => false,
        };
    }
    Ok(())
}

type Position = Vec<f64>;
type PointType = Position;
type LineStringType = Vec<Position>;
type PolygonType = Vec<Vec<Position>>;

fn process_coord<P: GeomProcessor>(
    point_type: &PointType,
    multi_dim: bool,
    idx: usize,
    processor: &mut P,
) -> Result<()> {
    if multi_dim {
        processor.coordinate(
            point_type[0],
            point_type[1],
            point_type.get(2).copied(),
            None,
            None,
            None,
            idx,
        )
    } else {
        processor.xy(point_type[0], point_type[1], idx)
    }
}

fn process_linestring<P: GeomProcessor>(
    linestring_type: &LineStringType,
    tagged: bool,
    idx: usize,
    processor: &mut P,
) -> Result<()> {
    processor.linestring_begin(tagged, linestring_type.len(), idx)?;
    let multi_dim = processor.multi_dim();
    for (idxc, point_type) in linestring_type.iter().enumerate() {
        process_coord(point_type, multi_dim, idxc, processor)?;
    }
    processor.linestring_end(tagged, idx)
}

fn process_polygon<P: GeomProcessor>(
    polygon_type: &PolygonType,
    tagged: bool,
    idx: usize,
    processor: &mut P,
) -> Result<()> {
    processor.polygon_begin(tagged, polygon_type.len(), idx)?;
    for (idx2, linestring_type) in polygon_type.iter().enumerate() {
        process_linestring(linestring_type, false, idx2, processor)?;
    }
    processor.polygon_end(tagged, idx)
}

// Note: we excluded the upstream geozero geojson reader tests
