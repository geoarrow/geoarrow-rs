use std::fs::File;
use std::path::Path;

use arrow_array::RecordBatchReader;
use arrow_schema::ArrowError;
use geoarrow_array::GeoArrowType;
use geoarrow_array::array::from_arrow_array;
use geoarrow_schema::CrsType;
use serde_json::json;

use crate::GeoParquetRecordBatchReaderBuilder;
use crate::test::geoarrow_data_example_crs_files;

/// Read a GeoParquet file and return the WKT and geometry arrays; columns 0 and 1.
fn read_gpq_file(path: impl AsRef<Path>) -> GeoArrowType {
    println!("reading path: {:?}", path.as_ref());
    let file = File::open(path).unwrap();
    let reader = GeoParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let schema = reader.schema();
    let batches = reader
        .collect::<std::result::Result<Vec<_>, ArrowError>>()
        .unwrap();
    assert_eq!(batches.len(), 1);

    let batch = batches[0].clone();

    let geo_arr = from_arrow_array(batch.column(0), schema.field(0)).unwrap();

    geo_arr.data_type().clone()
}

#[test]
fn vermont_4326() {
    let expected = json!({
      "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
      "type": "GeographicCRS",
      "name": "WGS 84",
      "datum_ensemble": {
        "name": "World Geodetic System 1984 ensemble",
        "members": [
          {
            "name": "World Geodetic System 1984 (Transit)",
            "id": {
              "authority": "EPSG",
              "code": 1166
            }
          },
          {
            "name": "World Geodetic System 1984 (G730)",
            "id": {
              "authority": "EPSG",
              "code": 1152
            }
          },
          {
            "name": "World Geodetic System 1984 (G873)",
            "id": {
              "authority": "EPSG",
              "code": 1153
            }
          },
          {
            "name": "World Geodetic System 1984 (G1150)",
            "id": {
              "authority": "EPSG",
              "code": 1154
            }
          },
          {
            "name": "World Geodetic System 1984 (G1674)",
            "id": {
              "authority": "EPSG",
              "code": 1155
            }
          },
          {
            "name": "World Geodetic System 1984 (G1762)",
            "id": {
              "authority": "EPSG",
              "code": 1156
            }
          },
          {
            "name": "World Geodetic System 1984 (G2139)",
            "id": {
              "authority": "EPSG",
              "code": 1309
            }
          },
          {
            "name": "World Geodetic System 1984 (G2296)",
            "id": {
              "authority": "EPSG",
              "code": 1383
            }
          }
        ],
        "ellipsoid": {
          "name": "WGS 84",
          "semi_major_axis": 6378137,
          "inverse_flattening": 298.257223563
        },
        "accuracy": "2.0",
        "id": {
          "authority": "EPSG",
          "code": 6326
        }
      },
      "coordinate_system": {
        "subtype": "ellipsoidal",
        "axis": [
          {
            "name": "Geodetic latitude",
            "abbreviation": "Lat",
            "direction": "north",
            "unit": "degree"
          },
          {
            "name": "Geodetic longitude",
            "abbreviation": "Lon",
            "direction": "east",
            "unit": "degree"
          }
        ]
      },
      "scope": "Horizontal component of 3D system.",
      "area": "World.",
      "bbox": {
        "south_latitude": -90,
        "west_longitude": -180,
        "north_latitude": 90,
        "east_longitude": 180
      },
      "id": {
        "authority": "EPSG",
        "code": 4326
      }
    });

    let path = geoarrow_data_example_crs_files().join("example-crs_vermont-4326_geo.parquet");
    let data_type = read_gpq_file(path);
    let crs = data_type.metadata().crs();
    assert_eq!(crs.crs_type().unwrap(), CrsType::Projjson);
    assert_eq!(crs.crs_value().unwrap(), &expected);
}

#[test]
fn vermont_custom() {
    let expected = json!({
      "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
      "type": "ProjectedCRS",
      "name": "unknown",
      "base_crs": {
        "name": "unknown",
        "datum": {
          "type": "GeodeticReferenceFrame",
          "name": "Unknown based on WGS 84 ellipsoid",
          "ellipsoid": {
            "name": "WGS 84",
            "semi_major_axis": 6378137,
            "inverse_flattening": 298.257223563,
            "id": {
              "authority": "EPSG",
              "code": 7030
            }
          }
        },
        "coordinate_system": {
          "subtype": "ellipsoidal",
          "axis": [
            {
              "name": "Longitude",
              "abbreviation": "lon",
              "direction": "east",
              "unit": "degree"
            },
            {
              "name": "Latitude",
              "abbreviation": "lat",
              "direction": "north",
              "unit": "degree"
            }
          ]
        }
      },
      "conversion": {
        "name": "unknown",
        "method": {
          "name": "Orthographic",
          "id": {
            "authority": "EPSG",
            "code": 9840
          }
        },
        "parameters": [
          {
            "name": "Latitude of natural origin",
            "value": 43.88,
            "unit": "degree",
            "id": {
              "authority": "EPSG",
              "code": 8801
            }
          },
          {
            "name": "Longitude of natural origin",
            "value": -72.69,
            "unit": "degree",
            "id": {
              "authority": "EPSG",
              "code": 8802
            }
          },
          {
            "name": "False easting",
            "value": 0,
            "unit": "metre",
            "id": {
              "authority": "EPSG",
              "code": 8806
            }
          },
          {
            "name": "False northing",
            "value": 0,
            "unit": "metre",
            "id": {
              "authority": "EPSG",
              "code": 8807
            }
          }
        ]
      },
      "coordinate_system": {
        "subtype": "Cartesian",
        "axis": [
          {
            "name": "Easting",
            "abbreviation": "E",
            "direction": "east",
            "unit": "metre"
          },
          {
            "name": "Northing",
            "abbreviation": "N",
            "direction": "north",
            "unit": "metre"
          }
        ]
      }
    });
    let path = geoarrow_data_example_crs_files().join("example-crs_vermont-custom_geo.parquet");
    let data_type = read_gpq_file(path);
    let crs = data_type.metadata().crs();
    assert_eq!(crs.crs_type().unwrap(), CrsType::Projjson);
    assert_eq!(crs.crs_value().unwrap(), &expected);
}

#[test]
fn vermont_utm() {
    let expected = json!({
      "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
      "type": "ProjectedCRS",
      "name": "WGS 84 / UTM zone 18N",
      "base_crs": {
        "name": "WGS 84",
        "datum_ensemble": {
          "name": "World Geodetic System 1984 ensemble",
          "members": [
            {
              "name": "World Geodetic System 1984 (Transit)",
              "id": {
                "authority": "EPSG",
                "code": 1166
              }
            },
            {
              "name": "World Geodetic System 1984 (G730)",
              "id": {
                "authority": "EPSG",
                "code": 1152
              }
            },
            {
              "name": "World Geodetic System 1984 (G873)",
              "id": {
                "authority": "EPSG",
                "code": 1153
              }
            },
            {
              "name": "World Geodetic System 1984 (G1150)",
              "id": {
                "authority": "EPSG",
                "code": 1154
              }
            },
            {
              "name": "World Geodetic System 1984 (G1674)",
              "id": {
                "authority": "EPSG",
                "code": 1155
              }
            },
            {
              "name": "World Geodetic System 1984 (G1762)",
              "id": {
                "authority": "EPSG",
                "code": 1156
              }
            },
            {
              "name": "World Geodetic System 1984 (G2139)",
              "id": {
                "authority": "EPSG",
                "code": 1309
              }
            },
            {
              "name": "World Geodetic System 1984 (G2296)",
              "id": {
                "authority": "EPSG",
                "code": 1383
              }
            }
          ],
          "ellipsoid": {
            "name": "WGS 84",
            "semi_major_axis": 6378137,
            "inverse_flattening": 298.257223563
          },
          "accuracy": "2.0",
          "id": {
            "authority": "EPSG",
            "code": 6326
          }
        },
        "coordinate_system": {
          "subtype": "ellipsoidal",
          "axis": [
            {
              "name": "Geodetic latitude",
              "abbreviation": "Lat",
              "direction": "north",
              "unit": "degree"
            },
            {
              "name": "Geodetic longitude",
              "abbreviation": "Lon",
              "direction": "east",
              "unit": "degree"
            }
          ]
        },
        "id": {
          "authority": "EPSG",
          "code": 4326
        }
      },
      "conversion": {
        "name": "UTM zone 18N",
        "method": {
          "name": "Transverse Mercator",
          "id": {
            "authority": "EPSG",
            "code": 9807
          }
        },
        "parameters": [
          {
            "name": "Latitude of natural origin",
            "value": 0,
            "unit": "degree",
            "id": {
              "authority": "EPSG",
              "code": 8801
            }
          },
          {
            "name": "Longitude of natural origin",
            "value": -75,
            "unit": "degree",
            "id": {
              "authority": "EPSG",
              "code": 8802
            }
          },
          {
            "name": "Scale factor at natural origin",
            "value": 0.9996,
            "unit": "unity",
            "id": {
              "authority": "EPSG",
              "code": 8805
            }
          },
          {
            "name": "False easting",
            "value": 500000,
            "unit": "metre",
            "id": {
              "authority": "EPSG",
              "code": 8806
            }
          },
          {
            "name": "False northing",
            "value": 0,
            "unit": "metre",
            "id": {
              "authority": "EPSG",
              "code": 8807
            }
          }
        ]
      },
      "coordinate_system": {
        "subtype": "Cartesian",
        "axis": [
          {
            "name": "Easting",
            "abbreviation": "E",
            "direction": "east",
            "unit": "metre"
          },
          {
            "name": "Northing",
            "abbreviation": "N",
            "direction": "north",
            "unit": "metre"
          }
        ]
      },
      "scope": "Navigation and medium accuracy spatial referencing.",
      "area": "Between 78\u{00b0}W and 72\u{00b0}W, northern hemisphere between equator and 84\u{00b0}N, onshore and offshore. Bahamas. Canada - Nunavut; Ontario; Quebec. Colombia. Cuba. Ecuador. Greenland. Haiti. Jamaica. Panama. Turks and Caicos Islands. United States (USA). Venezuela.",
      "bbox": {
        "south_latitude": 0,
        "west_longitude": -78,
        "north_latitude": 84,
        "east_longitude": -72
      },
      "id": {
        "authority": "EPSG",
        "code": 32618
      }
    });
    let path = geoarrow_data_example_crs_files().join("example-crs_vermont-utm_geo.parquet");
    let data_type = read_gpq_file(path);
    let crs = data_type.metadata().crs();
    assert_eq!(crs.crs_type().unwrap(), CrsType::Projjson);
    assert_eq!(crs.crs_value().unwrap(), &expected);
}

#[test]
fn vermont_crs84() {
    let expected = json!({
      "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
      "type": "GeographicCRS",
      "name": "WGS 84 (CRS84)",
      "datum_ensemble": {
        "name": "World Geodetic System 1984 ensemble",
        "members": [
          {
            "name": "World Geodetic System 1984 (Transit)",
            "id": {
              "authority": "EPSG",
              "code": 1166
            }
          },
          {
            "name": "World Geodetic System 1984 (G730)",
            "id": {
              "authority": "EPSG",
              "code": 1152
            }
          },
          {
            "name": "World Geodetic System 1984 (G873)",
            "id": {
              "authority": "EPSG",
              "code": 1153
            }
          },
          {
            "name": "World Geodetic System 1984 (G1150)",
            "id": {
              "authority": "EPSG",
              "code": 1154
            }
          },
          {
            "name": "World Geodetic System 1984 (G1674)",
            "id": {
              "authority": "EPSG",
              "code": 1155
            }
          },
          {
            "name": "World Geodetic System 1984 (G1762)",
            "id": {
              "authority": "EPSG",
              "code": 1156
            }
          },
          {
            "name": "World Geodetic System 1984 (G2139)",
            "id": {
              "authority": "EPSG",
              "code": 1309
            }
          },
          {
            "name": "World Geodetic System 1984 (G2296)",
            "id": {
              "authority": "EPSG",
              "code": 1383
            }
          }
        ],
        "ellipsoid": {
          "name": "WGS 84",
          "semi_major_axis": 6378137,
          "inverse_flattening": 298.257223563
        },
        "accuracy": "2.0",
        "id": {
          "authority": "EPSG",
          "code": 6326
        }
      },
      "coordinate_system": {
        "subtype": "ellipsoidal",
        "axis": [
          {
            "name": "Geodetic longitude",
            "abbreviation": "Lon",
            "direction": "east",
            "unit": "degree"
          },
          {
            "name": "Geodetic latitude",
            "abbreviation": "Lat",
            "direction": "north",
            "unit": "degree"
          }
        ]
      },
      "scope": "Not known.",
      "area": "World.",
      "bbox": {
        "south_latitude": -90,
        "west_longitude": -180,
        "north_latitude": 90,
        "east_longitude": 180
      },
      "id": {
        "authority": "OGC",
        "code": "CRS84"
      }
    });
    let path = geoarrow_data_example_crs_files().join("example-crs_vermont-crs84_geo.parquet");
    let data_type = read_gpq_file(path);
    let crs = data_type.metadata().crs();
    assert_eq!(crs.crs_type().unwrap(), CrsType::Projjson);
    assert_eq!(crs.crs_value().unwrap(), &expected);
}
