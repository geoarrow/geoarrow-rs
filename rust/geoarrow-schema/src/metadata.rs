use arrow_schema::ArrowError;
use serde::{Deserialize, Serialize};

use crate::{Crs, Edges};

/// A GeoArrow metadata object following the extension metadata [defined by the GeoArrow
/// specification](https://geoarrow.org/extension-types).
///
/// This is serialized to JSON when a [`geoarrow`](self) array is exported to an [`arrow`] array and
/// deserialized when imported from an [`arrow`] array.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Metadata {
    // Raise the underlying crs fields to this level.
    // https://serde.rs/attr-flatten.html
    #[serde(flatten)]
    crs: Crs,

    /// If present, instructs consumers that edges follow a spherical path rather than a planar
    /// one. If this value is omitted, edges will be interpreted as planar.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edges: Option<Edges>,
}

impl Metadata {
    /// Creates a new [`Metadata`] object.
    pub fn new(crs: Crs, edges: Option<Edges>) -> Self {
        Self { crs, edges }
    }

    /// Returns true if the metadata should be serialized.
    fn should_serialize(&self) -> bool {
        self.crs.should_serialize() || self.edges.is_some()
    }

    pub(crate) fn serialize(&self) -> Option<String> {
        if self.should_serialize() {
            Some(serde_json::to_string(&self).unwrap())
        } else {
            None
        }
    }

    pub(crate) fn deserialize(metadata: Option<&str>) -> Result<Self, ArrowError> {
        if let Some(ext_meta) = metadata {
            Ok(serde_json::from_str(ext_meta)
                .map_err(|err| ArrowError::ExternalError(Box::new(err)))?)
        } else {
            Ok(Default::default())
        }
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use serde_json::Value;

    use super::*;

    const EPSG_4326_WKT: &str = r#"GEOGCRS["WGS 84",ENSEMBLE["World Geodetic System 1984 ensemble",MEMBER["World Geodetic System 1984 (Transit)"],MEMBER["World Geodetic System 1984 (G730)"],MEMBER["World Geodetic System 1984 (G873)"],MEMBER["World Geodetic System 1984 (G1150)"],MEMBER["World Geodetic System 1984 (G1674)"],MEMBER["World Geodetic System 1984 (G1762)"],MEMBER["World Geodetic System 1984 (G2139)"],ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1]],ENSEMBLEACCURACY[2.0]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],CS[ellipsoidal,2],AXIS["geodetic latitude (Lat)",north,ORDER[1],ANGLEUNIT["degree",0.0174532925199433]],AXIS["geodetic longitude (Lon)",east,ORDER[2],ANGLEUNIT["degree",0.0174532925199433]],USAGE[SCOPE["Horizontal component of 3D system."],AREA["World."],BBOX[-90,-180,90,180]],ID["EPSG",4326]]"#;

    const EPSG_4326_PROJJSON: &str = r#"{"$schema":"https://proj.org/schemas/v0.7/projjson.schema.json","type":"GeographicCRS","name":"WGS 84","datum_ensemble":{"name":"World Geodetic System 1984 ensemble","members":[{"name":"World Geodetic System 1984 (Transit)","id":{"authority":"EPSG","code":1166}},{"name":"World Geodetic System 1984 (G730)","id":{"authority":"EPSG","code":1152}},{"name":"World Geodetic System 1984 (G873)","id":{"authority":"EPSG","code":1153}},{"name":"World Geodetic System 1984 (G1150)","id":{"authority":"EPSG","code":1154}},{"name":"World Geodetic System 1984 (G1674)","id":{"authority":"EPSG","code":1155}},{"name":"World Geodetic System 1984 (G1762)","id":{"authority":"EPSG","code":1156}},{"name":"World Geodetic System 1984 (G2139)","id":{"authority":"EPSG","code":1309}}],"ellipsoid":{"name":"WGS 84","semi_major_axis":6378137,"inverse_flattening":298.257223563},"accuracy":"2.0","id":{"authority":"EPSG","code":6326}},"coordinate_system":{"subtype":"ellipsoidal","axis":[{"name":"Geodetic latitude","abbreviation":"Lat","direction":"north","unit":"degree"},{"name":"Geodetic longitude","abbreviation":"Lon","direction":"east","unit":"degree"}]},"scope":"Horizontal component of 3D system.","area":"World.","bbox":{"south_latitude":-90,"west_longitude":-180,"north_latitude":90,"east_longitude":180},"id":{"authority":"EPSG","code":4326}}"#;

    #[test]
    fn test_crs_authority_code() {
        let crs = Crs::from_authority_code("EPSG:4326".to_string());
        let metadata = Metadata::new(crs, Some(Edges::Spherical));

        let expected = r#"{"crs":"EPSG:4326","crs_type":"authority_code","edges":"spherical"}"#;
        assert_eq!(metadata.serialize().as_deref(), Some(expected));
    }

    #[test]
    fn test_crs_authority_code_no_edges() {
        let crs = Crs::from_authority_code("EPSG:4326".to_string());
        let metadata = Metadata::new(crs, None);

        let expected = r#"{"crs":"EPSG:4326","crs_type":"authority_code"}"#;
        assert_eq!(metadata.serialize().as_deref(), Some(expected));
    }

    #[test]
    fn test_crs_wkt() {
        let crs = Crs::from_wkt2_2019(EPSG_4326_WKT.to_string());
        let metadata = Metadata::new(crs, None);

        let expected = r#"{"crs":"GEOGCRS[\"WGS 84\",ENSEMBLE[\"World Geodetic System 1984 ensemble\",MEMBER[\"World Geodetic System 1984 (Transit)\"],MEMBER[\"World Geodetic System 1984 (G730)\"],MEMBER[\"World Geodetic System 1984 (G873)\"],MEMBER[\"World Geodetic System 1984 (G1150)\"],MEMBER[\"World Geodetic System 1984 (G1674)\"],MEMBER[\"World Geodetic System 1984 (G1762)\"],MEMBER[\"World Geodetic System 1984 (G2139)\"],ELLIPSOID[\"WGS 84\",6378137,298.257223563,LENGTHUNIT[\"metre\",1]],ENSEMBLEACCURACY[2.0]],PRIMEM[\"Greenwich\",0,ANGLEUNIT[\"degree\",0.0174532925199433]],CS[ellipsoidal,2],AXIS[\"geodetic latitude (Lat)\",north,ORDER[1],ANGLEUNIT[\"degree\",0.0174532925199433]],AXIS[\"geodetic longitude (Lon)\",east,ORDER[2],ANGLEUNIT[\"degree\",0.0174532925199433]],USAGE[SCOPE[\"Horizontal component of 3D system.\"],AREA[\"World.\"],BBOX[-90,-180,90,180]],ID[\"EPSG\",4326]]","crs_type":"wkt2:2019"}"#;

        assert_eq!(metadata.serialize().as_deref(), Some(expected));
    }

    #[test]
    fn test_projjson() {
        let crs = Crs::from_projjson(Value::from_str(EPSG_4326_PROJJSON).unwrap());
        let metadata = Metadata::new(crs, None);

        let expected = r#"{"crs":{"$schema":"https://proj.org/schemas/v0.7/projjson.schema.json","type":"GeographicCRS","name":"WGS 84","datum_ensemble":{"name":"World Geodetic System 1984 ensemble","members":[{"name":"World Geodetic System 1984 (Transit)","id":{"authority":"EPSG","code":1166}},{"name":"World Geodetic System 1984 (G730)","id":{"authority":"EPSG","code":1152}},{"name":"World Geodetic System 1984 (G873)","id":{"authority":"EPSG","code":1153}},{"name":"World Geodetic System 1984 (G1150)","id":{"authority":"EPSG","code":1154}},{"name":"World Geodetic System 1984 (G1674)","id":{"authority":"EPSG","code":1155}},{"name":"World Geodetic System 1984 (G1762)","id":{"authority":"EPSG","code":1156}},{"name":"World Geodetic System 1984 (G2139)","id":{"authority":"EPSG","code":1309}}],"ellipsoid":{"name":"WGS 84","semi_major_axis":6378137,"inverse_flattening":298.257223563},"accuracy":"2.0","id":{"authority":"EPSG","code":6326}},"coordinate_system":{"subtype":"ellipsoidal","axis":[{"name":"Geodetic latitude","abbreviation":"Lat","direction":"north","unit":"degree"},{"name":"Geodetic longitude","abbreviation":"Lon","direction":"east","unit":"degree"}]},"scope":"Horizontal component of 3D system.","area":"World.","bbox":{"south_latitude":-90,"west_longitude":-180,"north_latitude":90,"east_longitude":180},"id":{"authority":"EPSG","code":4326}},"crs_type":"projjson"}"#;
        assert_eq!(
            Value::from_str(&metadata.serialize().unwrap()).unwrap(),
            Value::from_str(expected).unwrap()
        );
    }

    #[test]
    fn test_unknown_crs() {
        let crs = Crs::from_unknown_crs_type("CRS".to_string());
        let metadata = Metadata::new(crs, None);

        let expected = r#"{"crs":"CRS"}"#;
        assert_eq!(metadata.serialize().as_deref(), Some(expected));
    }

    #[test]
    fn test_empty_metadata() {
        let metadata = Metadata::default();
        assert_eq!(metadata.serialize().as_deref(), None);
    }
}
