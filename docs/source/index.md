# `geoarrow-rs`

A Rust implementation and Python and JavaScript (WebAssembly) bindings of the [GeoArrow](https://geoarrow.org) memory specification for efficiently storing geospatial vector geometries, connected to geospatial algorithms implemented by the [GeoRust community](https://georust.org/).

## Motivation

GeoArrow provides a way to share geospatial vector data between programs _at no cost_ and without copies, so that an ecosystem of libraries can share data without serialization overhead. Removing this overhead enables faster code in high-level, interpreted languages such as Python and JavaScript.


<div class="grid cards" markdown>

-   :material-language-rust:{ .lg .middle } **Rust core library**

    ---

    Create your own Rust library or application on top of the `geoarrow` Rust crate.

    [:octicons-arrow-right-24: Documentation](https://docs.rs/geoarrow/latest/geoarrow/)

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
