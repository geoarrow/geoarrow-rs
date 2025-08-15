# geoarrow-rs

The geoarrow-rs repository contains a Rust implementation, Python bindings, and JavaScript (WebAssembly) bindings of the [GeoArrow](https://geoarrow.org) memory specification for efficiently storing geospatial vector geometries, connected to geospatial algorithms implemented by the [GeoRust community](https://georust.org/).

<div class="grid cards" markdown>

-   :material-language-rust:{ .lg .middle } **Rust core library**

    ---

    Create your own Rust library or application on top of the `geoarrow-*` Rust crates.

    [:octicons-arrow-right-24: Documentation](./rust.md)

-   :material-language-python:{ .lg .middle } **Python bindings**

    ---

    Performant, easy-to-use Python bindings to the Rust core.

    [:octicons-arrow-right-24: Documentation](python/latest/)

-   :material-language-javascript:{ .lg .middle } **JavaScript bindings**

    ---

    Use GeoArrow from JavaScript in Web applications or in Node.

    [:octicons-arrow-right-24: Documentation](js)

-   :material-book-open-variant:{ .lg .middle } **GeoArrow specification**

    ---

    Read the GeoArrow specification.

    [:octicons-arrow-right-24: Specification](https://geoarrow.org)

</div>

## Motivation

GeoArrow provides a way to share geospatial vector data between programs _at no cost_ and without copies, so that an ecosystem of libraries can share data without serialization overhead. Removing this overhead enables faster code in high-level, interpreted languages such as Python and JavaScript.
