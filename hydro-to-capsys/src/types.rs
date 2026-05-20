//! JSON schema matching the per-operator records that CAPSys (Go) consumes.
//!
//! See `scripts/caps-go/caps_single/structs.go` and `utils.go` for the
//! canonical definition. The fields are:
//!
//! - `id`: single-byte identifier (we emit a one-character ASCII string; Go
//!   reads `str[0]`).
//! - `name`: human-readable tag.
//! - `parallelism`: number of task instances for this operator.
//! - `compute` / `state` / `network`: per-task resource demand (floats).
//! - `outboundtype`: `"FORWARD"`, `"REBALANCE"`, `"HASH"`, or `""` for sinks.
//! - `downNode` / `upNode`: downstream / upstream operator ids.

use serde::{Deserialize, Serialize};

/// One operator record in the CAPSys job-graph JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapsysOperator {
    pub id: String,
    pub name: String,
    pub parallelism: usize,
    pub state: f64,
    pub compute: f64,
    pub network: f64,
    pub outboundtype: String,
    #[serde(rename = "downNode")]
    pub down_node: Vec<String>,
    #[serde(rename = "upNode")]
    pub up_node: Vec<String>,
}

/// Convenience: serialize a slice of operators into CAPSys-ready JSON.
pub fn operators_to_json(ops: &[CapsysOperator]) -> serde_json::Result<String> {
    serde_json::to_string_pretty(ops)
}
