// import * as geoarrow from "../../pkg/node";
// import { tableFromIPC } from "apache-arrow";
// import { readFileSync } from "fs";
// import { expect, it } from "vitest";

// geoarrow.set_panic_hook();

// it("read GeoParquet", () => {
//   const path = "../fixtures/geoparquet/nybb.parquet";
//   const buffer = new Uint8Array(readFileSync(path));
//   const wasmTable = geoarrow.readGeoParquet(buffer);
//   const arrowIPCBuffer = wasmTable.intoIPCStream();
//   const arrowJsTable = tableFromIPC(arrowIPCBuffer);
//   const geometryIdx = arrowJsTable.schema.fields.findIndex(
//     (field) => field.name === "geometry"
//   );
//   const geometryField = arrowJsTable.schema.fields[geometryIdx];
//   const geometryFieldMetadata = geometryField.metadata;
//   expect(geometryFieldMetadata.get("ARROW:extension:name")).toStrictEqual(
//     "geoarrow.multipolygon"
//   );
// });

import { it } from "vitest";

it("hello world", () => {});
