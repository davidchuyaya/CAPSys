//! Convert a Hydro IR (plus `hydro_optimize::ILPAnalysisInputs`) into the
//! per-operator JSON format consumed by the CAPSys Go search.
//!
//! See [`converter::convert`] for the main entry point, and
//! [`types::CapsysOperator`] for the output record.
//!
//! ```ignore
//! use hydro_to_capsys::{convert, operators_to_json, ConvertOptions, LocationInfo};
//!
//! let ops = convert(&mut ir, &analysis_inputs, &location_info, &ConvertOptions::default());
//! std::fs::write("job_graph.json", operators_to_json(&ops)?)?;
//! ```

pub mod converter;
pub mod types;

pub use converter::{ConvertOptions, LocationInfo, convert};
pub use types::{CapsysOperator, operators_to_json};
