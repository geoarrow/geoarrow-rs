import * as geoarrow from "../../pkg/node";
import { tableFromIPC } from "apache-arrow";
import { readFileSync } from "fs";
import { expect, it } from "vitest";

geoarrow.set_panic_hook();

it("writeGeoParquet -> readGeoParquet round-trips the geometry column", () => {
  const buffer = new Uint8Array(
    readFileSync("../fixtures/geoparquet/nybb.parquet"),
  );
  const table = geoarrow.readGeoParquet(buffer);
  const bytes = geoarrow.writeGeoParquet(table); // consumes `table`
  const roundTripped = geoarrow.readGeoParquet(new Uint8Array(bytes));

  const jsTable = tableFromIPC(roundTripped.intoIPCStream());
  const geometryField = jsTable.schema.fields.find(
    (field) => field.name === "geometry",
  );
  expect(geometryField).toBeDefined();
  expect(geometryField!.metadata.get("ARROW:extension:name")).toStrictEqual(
    "geoarrow.multipolygon",
  );
});

it("writeGeoParquet honors the encoding option and rejects invalid options", () => {
  const buffer = new Uint8Array(
    readFileSync("../fixtures/geoparquet/nybb.parquet"),
  );

  // The chosen encoding lands in the written footer's geo metadata. (Reading
  // a native-encoded file back is not asserted here: the geoparquet reader
  // currently parses native columns against the interleaved target type
  // rather than the separated type files store — an upstream limitation
  // noted in geoparquet's reader/parse.rs.)
  const footerText = (bytes: Uint8Array) =>
    Buffer.from(bytes).toString("latin1");
  const nativeBytes = geoarrow.writeGeoParquet(geoarrow.readGeoParquet(buffer), {
    encoding: "native",
  });
  expect(footerText(nativeBytes)).toMatch(/"encoding"\s*:\s*"multipolygon"/);

  const defaultBytes = geoarrow.writeGeoParquet(geoarrow.readGeoParquet(buffer));
  expect(footerText(defaultBytes)).toMatch(/"encoding"\s*:\s*"WKB"/);

  // Unknown option KEYS are ignored, as is conventional for JS options
  // objects; the TypeScript interface is the typo guard.
  expect(() =>
    geoarrow.writeGeoParquet(geoarrow.readGeoParquet(buffer), {
      encoding: "hexwkb" as never,
    }),
  ).toThrow(/unknown encoding/);
});
