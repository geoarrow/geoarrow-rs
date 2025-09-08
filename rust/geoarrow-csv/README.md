# geoarrow-csv

Provides a Reader and Writer for taking a CSV file that has a geometry column encoded in [Well-Known Text](https://libgeos.org/specifications/wkt/).


## Reading a CSV file
Use `CsvReader` to take a `BufReader` that has access to a CSV file or string,
then `CsvReaderOptions` to specify the name of the column containing geometry and the type to convert to.

```rust
let csv_file = std::fs::File::open("test.csv");


