import * as geoarrow from "../../pkg/node";
import { tableFromIPC } from "apache-arrow";
import { readFileSync } from "fs";
import { createServer, type Server } from "http";
import type { AddressInfo } from "net";
import { afterAll, beforeAll, expect, it } from "vitest";

geoarrow.set_panic_hook();

// Serve the fixtures over HTTP with byte ranges, thus the reader runs against a
// real store and not against the network. frag_a and frag_b name one file twice,
// which gives a dataset two fragments.
const nybb = readFileSync("../fixtures/geoparquet/nybb.parquet");
const covering = readFileSync("../fixtures/geoparquet/nybb_wkb_covering.parquet");
const routes: Record<string, Buffer> = {
  "/nybb.parquet": nybb,
  "/nybb_wkb_covering.parquet": covering,
  "/frag_a.parquet": nybb,
  "/frag_b.parquet": nybb,
  // The key is the decoded path, thus only an encoded request reaches this file.
  "/nybb copy.parquet": nybb,
};

// Read the bbox out of the footer JSON, apart from the Rust reader, thus
// `fileBbox` has an oracle to meet.
function bboxFromFooterJson(buf: Buffer): number[] {
  const text = buf.toString("latin1");
  const m = /"bbox"\s*:\s*\[([-\d.eE+,\s]+)\]/.exec(text);
  expect(m, "fixture footer should embed a geo bbox").toBeTruthy();
  return m![1].split(",").map((s) => Number(s.trim()));
}

// The values cross two JSON parsers, thus the last bit is not stable. At a
// magnitude near 1e6, a tolerance below 1e-6 would only make the test flaky.
function expectBboxClose(got: ArrayLike<number>, want: ArrayLike<number>) {
  expect(got.length).toBe(want.length);
  for (let i = 0; i < want.length; i++) {
    expect(Math.abs(got[i] - want[i])).toBeLessThan(1e-3);
  }
}

let server: Server;
let baseUrl: string;
let rangeRequestCount = 0;

beforeAll(async () => {
  server = createServer((req, res) => {
    // decodeURIComponent: serve by decoded name so encoded request paths only
    // resolve when the client encodes exactly once.
    const pathname = decodeURIComponent(
      new URL(req.url ?? "/", "http://x").pathname,
    );
    const buf = routes[pathname];
    if (!buf) {
      res.writeHead(404);
      res.end();
      return;
    }
    if (req.method === "HEAD") {
      res.writeHead(200, {
        "Content-Length": String(buf.length),
        "Accept-Ranges": "bytes",
      });
      res.end();
      return;
    }
    const range = req.headers["range"];
    if (!range) {
      res.writeHead(200, {
        "Content-Length": String(buf.length),
        "Accept-Ranges": "bytes",
      });
      res.end(buf);
      return;
    }
    rangeRequestCount += 1;
    // "bytes=start-end" or suffix "bytes=-N"
    const m = /bytes=(\d*)-(\d*)/.exec(range)!;
    let start: number;
    let end: number; // inclusive
    if (m[1] === "") {
      // Suffix range: clamp N > file size to the whole file, per RFC 7233.
      start = Math.max(0, buf.length - Number(m[2]));
      end = buf.length - 1;
    } else {
      start = Number(m[1]);
      end = m[2] === "" ? buf.length - 1 : Number(m[2]);
    }
    const chunk = buf.subarray(start, end + 1);
    res.writeHead(206, {
      "Content-Range": `bytes ${start}-${end}/${buf.length}`,
      "Content-Length": String(chunk.length),
      "Accept-Ranges": "bytes",
    });
    res.end(chunk);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  baseUrl = `http://127.0.0.1:${port}`;
});

afterAll(() => {
  server?.close();
});

it("ParquetFile.open -> read pulls a remote GeoParquet file over HTTP range", async () => {
  const before = rangeRequestCount;
  const file = await geoarrow.ParquetFile.open(`${baseUrl}/nybb.parquet`);
  expect(file.numRows).toBeGreaterThan(0);

  const table = await file.read();
  const jsTable = tableFromIPC(table.intoIPCStream());
  const geometryField = jsTable.schema.fields.find(
    (field) => field.name === "geometry",
  );
  expect(geometryField).toBeDefined();
  expect(geometryField!.metadata.get("ARROW:extension:name")).toStrictEqual(
    "geoarrow.multipolygon",
  );
  // A reader that fell back to whole-file GETs would pass every assertion above
  // and leave this counter at zero.
  expect(rangeRequestCount).toBeGreaterThan(before);
});

it("ParquetFile.open resolves a percent-encoded filename exactly once", async () => {
  // A reader that leaves the path encoded lets the client encode the `%` again
  // and ask for "nybb%2520copy.parquet", which the server does not hold.
  const file = await geoarrow.ParquetFile.open(
    `${baseUrl}/nybb%20copy.parquet`,
  );
  expect(file.numRows).toBeGreaterThan(0);
  const table = await file.read();
  const jsTable = tableFromIPC(table.intoIPCStream());
  expect(jsTable.numRows).toBe(file.numRows);
});

it("ParquetFile.open rejects URLs with query parameters or credentials", async () => {
  await expect(
    geoarrow.ParquetFile.open(`${baseUrl}/nybb.parquet?X-Signature=abc`),
  ).rejects.toThrow(/query parameters and credentials are not supported/);
  await expect(
    geoarrow.ParquetFile.open(
      `http://user:pass@127.0.0.1:1/nybb.parquet`,
    ),
  ).rejects.toThrow(/query parameters and credentials are not supported/);
});

it("ParquetFile.read(bbox) errors on a file without covering metadata", async () => {
  const file = await geoarrow.ParquetFile.open(`${baseUrl}/nybb.parquet`);
  await expect(file.read([0, 0, 1, 1])).rejects.toThrow(/geospatial statistics/i);
});

it("ParquetFile.read(bbox) prunes to the intersecting rows", async () => {
  const file = await geoarrow.ParquetFile.open(
    `${baseUrl}/nybb_wkb_covering.parquet`,
  );
  const total = file.numRows;
  expect(total).toBeGreaterThan(0);

  const bbox = file.fileBbox()!; // [xmin, ymin, xmax, ymax], geometry CRS
  expect(bbox).toBeDefined();
  expectBboxClose(bbox, bboxFromFooterJson(covering));

  const full = tableFromIPC((await file.read(bbox)).intoIPCStream());
  expect(full.numRows).toBe(total);

  const [xmin, ymin, xmax, ymax] = bbox;
  const w = xmax - xmin;
  const h = ymax - ymin;
  const disjoint = [xmax + w, ymax + h, xmax + 2 * w, ymax + 2 * h];
  const none = tableFromIPC((await file.read(disjoint)).intoIPCStream());
  expect(none.numRows).toBe(0);

  // A box over half the extent keeps some rows and not others, which the two
  // cases above cannot tell apart.
  const west = [xmin, ymin, xmin + w / 2, ymax];
  const partial = tableFromIPC((await file.read(west)).intoIPCStream());
  expect(partial.numRows).toBeGreaterThan(0);
  expect(partial.numRows).toBeLessThan(total);
});

it("ParquetFile.read accepts the 6-element 3D bbox form and rejects malformed bboxes", async () => {
  const file = await geoarrow.ParquetFile.open(
    `${baseUrl}/nybb_wkb_covering.parquet`,
  );
  const total = file.numRows;
  const [xmin, ymin, xmax, ymax] = file.fileBbox()!;

  // A reader that took the first four of the six values would build a rect that
  // holds nothing.
  const full3d = tableFromIPC(
    (await file.read([xmin, ymin, 0, xmax, ymax, 0])).intoIPCStream(),
  );
  expect(full3d.numRows).toBe(total);

  await expect(file.read([xmin, ymin, xmax])).rejects.toThrow(
    /bbox must have 4 elements/,
  );
  await expect(file.read([xmax, ymin, xmin, ymax])).rejects.toThrow(
    /inverted bbox/,
  );
});

it("writeGeoParquet({generateCovering: true}) output round-trips through read(bbox)", async () => {
  const table = geoarrow.readGeoParquet(new Uint8Array(nybb));
  const written = geoarrow.writeGeoParquet(table, { generateCovering: true });
  const writtenBuf = Buffer.from(written);

  expectBboxClose(bboxFromFooterJson(writtenBuf), bboxFromFooterJson(nybb));

  routes["/written_covering.parquet"] = writtenBuf;
  const file = await geoarrow.ParquetFile.open(
    `${baseUrl}/written_covering.parquet`,
  );
  const total = file.numRows;
  expect(total).toBeGreaterThan(0);
  const bbox = file.fileBbox()!;
  const full = tableFromIPC((await file.read(bbox)).intoIPCStream());
  expect(full.numRows).toBe(total);
});

it("ParquetDataset.read concatenates fragments into one Table", async () => {
  const single = await geoarrow.ParquetFile.open(`${baseUrl}/nybb.parquet`);
  const n = single.numRows;

  const dataset = await geoarrow.ParquetDataset.open(baseUrl, [
    "frag_a.parquet",
    "frag_b.parquet",
  ]);
  expect(dataset.numRows).toBe(2 * n);

  const table = tableFromIPC((await dataset.read()).intoIPCStream());
  expect(table.numRows).toBe(2 * n);
  const geometryField = table.schema.fields.find(
    (field) => field.name === "geometry",
  );
  expect(geometryField!.metadata.get("ARROW:extension:name")).toStrictEqual(
    "geoarrow.multipolygon",
  );
});
