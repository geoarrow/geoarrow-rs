#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![doc(
    html_logo_url = "https://github.com/geoarrow.png",
    html_favicon_url = "https://github.com/geoarrow.png?size=32"
)]

pub mod file_format;

pub use file_format::{GeoJsonFileFactory, GeoJsonFormat, GeoJsonFormatFactory};

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::fs;
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::catalog::MemTable;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use geoarrow_array::GeoArrowArray;
    use geoarrow_array::builder::PointBuilder;
    use geoarrow_schema::{Dimension, PointType};
    use wkt::wkt;

    use super::*;

    fn sample_table() -> (Vec<RecordBatch>, Arc<Schema>) {
        let mut builder = PointBuilder::new(PointType::new(Dimension::XY, Default::default()));
        builder.push_point(Some(&wkt!(POINT(1.0 2.0))));
        builder.push_point(Some(&wkt!(POINT(3.0 4.0))));
        let geometry = builder.finish();

        let fields = vec![
            Arc::new(Field::new("id", DataType::Int32, false)),
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(geometry.data_type().to_field("geometry", true)),
        ];
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as _,
                Arc::new(StringArray::from(vec!["Point A", "Point B"])) as _,
                geometry.into_array_ref(),
            ],
        )
        .unwrap();

        (vec![batch], schema)
    }

    #[tokio::test]
    async fn test_write_geojsonlines_sink() {
        let file_format = Arc::new(GeoJsonFileFactory::new()); // Lines format
        let state = SessionStateBuilder::new()
            .with_file_formats(vec![file_format])
            .build();
        let ctx = SessionContext::new_with_state(state);

        let (batches, schema) = sample_table();
        let mem_table = Arc::new(MemTable::try_new(schema.clone(), vec![batches]).unwrap());
        ctx.register_table("mem_table", mem_table).unwrap();

        let file_path = temp_dir().join("test_geojsonlines_sink.geojsonl");

        ctx.sql(&format!("COPY mem_table TO '{}';", file_path.display()))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Read the file and verify it's valid GeoJSON Lines
        let contents = fs::read_to_string(&file_path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();

        // Should have 2 lines for 2 features
        assert_eq!(lines.len(), 2);

        // Parse each line as a GeoJSON Feature
        for (i, line) in lines.iter().enumerate() {
            let feature: geojson::Feature = line.parse().unwrap();
            let expected_id = i + 1;
            let expected_name = format!("Point {}", if i == 0 { "A" } else { "B" });

            assert_eq!(
                feature.id,
                Some(geojson::feature::Id::Number(serde_json::Number::from(
                    expected_id
                )))
            );
            assert_eq!(
                feature.properties.as_ref().unwrap().get("name").unwrap(),
                &serde_json::Value::String(expected_name)
            );

            // Check geometry
            if let Some(ref geometry) = feature.geometry {
                if let geojson::Value::Point(coords) = &geometry.value {
                    let expected_x = if i == 0 { 1.0 } else { 3.0 };
                    let expected_y = if i == 0 { 2.0 } else { 4.0 };
                    assert_eq!(coords[0], expected_x);
                    assert_eq!(coords[1], expected_y);
                } else {
                    panic!("Expected Point geometry");
                }
            } else {
                panic!("Expected geometry");
            }
        }

        fs::remove_file(&file_path).unwrap();
    }

    #[tokio::test]
    async fn test_write_geojson_with_id_column() {
        // Test with explicit ID column
        let mut builder = PointBuilder::new(PointType::new(Dimension::XY, Default::default()));
        builder.push_point(Some(&wkt!(POINT(5.0 6.0))));
        let geometry = builder.finish();

        let fields = vec![
            Arc::new(Field::new("id", DataType::Int32, false)),
            Arc::new(geometry.data_type().to_field("geometry", true)),
            Arc::new(Field::new("value", DataType::Int32, false)),
        ];
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![42])) as _,
                geometry.into_array_ref(),
                Arc::new(Int32Array::from(vec![100])) as _,
            ],
        )
        .unwrap();

        let file_format = Arc::new(GeoJsonFileFactory::new());
        let state = SessionStateBuilder::new()
            .with_file_formats(vec![file_format])
            .build();
        let ctx = SessionContext::new_with_state(state);

        let mem_table = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
        ctx.register_table("mem_table", mem_table).unwrap();

        let file_path = temp_dir().join("test_geojson_with_id.geojsonl");

        ctx.sql(&format!("COPY mem_table TO '{}';", file_path.display()))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Read and validate - should be a single line since it's one feature
        let contents = fs::read_to_string(&file_path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 1);

        // Parse the single line as a GeoJSON Feature
        let feature: geojson::Feature = lines[0].parse().unwrap();

        // Check the id field is at feature level
        assert_eq!(
            feature.id,
            Some(geojson::feature::Id::Number(serde_json::Number::from(42)))
        );

        // Check properties only contains "value", not "id"
        let props = feature.properties.as_ref().unwrap();
        assert_eq!(
            props.get("value").unwrap(),
            &serde_json::Value::Number(serde_json::Number::from(100))
        );
        assert!(props.get("id").is_none()); // id should not be in properties

        fs::remove_file(&file_path).unwrap();
    }
}
