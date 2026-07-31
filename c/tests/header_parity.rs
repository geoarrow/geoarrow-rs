//! The committed header must declare every `GeoArrowRs` symbol in the sources,
//! and the C surface must cover every op the js bindings export.

use std::collections::BTreeSet;

const HEADER: &str = include_str!("../geoarrow_rs.h");
const SOURCES: &[&str] = &[
    include_str!("../src/lib.rs"),
    include_str!("../src/compute.rs"),
    include_str!("../src/conversion.rs"),
    include_str!("../src/error.rs"),
    include_str!("../src/geoparquet.rs"),
    include_str!("../src/marshal.rs"),
    include_str!("../src/schema.rs"),
    include_str!("../src/types.rs"),
];
const JS_ALGORITHM: &str = include_str!("../../js/src/algorithm.rs");

/// Every `GeoArrowRs`-prefixed identifier in `text`, the bare prefix excluded.
fn symbols(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut rest = text;
    while let Some(pos) = rest.find("GeoArrowRs") {
        let tail = &rest[pos..];
        let end = tail
            .char_indices()
            .find(|(_, c)| !c.is_ascii_alphanumeric() && *c != '_')
            .map(|(i, _)| i)
            .unwrap_or(tail.len());
        if end > "GeoArrowRs".len() {
            out.insert(tail[..end].to_string());
        }
        rest = &tail[end..];
    }
    out
}

/// The op names the js crate exports: `name => rust;` table entries plus
/// `js_name` attributes on plain functions.
fn js_ops(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("#[wasm_bindgen(js_name = ")
            && let Some(name) = rest.strip_suffix(")]")
            && name.chars().all(|c| c.is_ascii_alphanumeric())
        {
            out.insert(name.to_string());
        } else if let Some((lhs, rhs)) = line.split_once(" => ")
            && !lhs.is_empty()
            && lhs.chars().all(|c| c.is_ascii_alphanumeric())
            && rhs.ends_with(';')
        {
            out.insert(lhs.to_string());
        }
    }
    out
}

/// Guards the macro-expansion step of header generation: a regenerated header
/// that silently dropped generated entry points fails here.
#[test]
fn header_declares_every_source_symbol() {
    let header = symbols(HEADER);
    for src in SOURCES {
        for name in symbols(src) {
            assert!(header.contains(&name), "`{name}` is not in geoarrow_rs.h");
        }
    }
}

#[test]
fn c_surface_covers_the_js_ops() {
    let ops = js_ops(JS_ALGORITHM);
    assert!(ops.len() >= 40, "js op scan broke: found {}", ops.len());
    let c: BTreeSet<String> = symbols(HEADER)
        .iter()
        .map(|s| s.to_ascii_lowercase())
        .collect();
    for op in ops {
        let want = format!("geoarrowrs{}", op.to_ascii_lowercase());
        assert!(c.contains(&want), "js op `{op}` has no C entry point");
    }
}
