//! Walk a Hydro IR together with `ILPAnalysisInputs` and emit the per-operator
//! records expected by the CAPSys Go search (`scripts/caps-go/caps_single`).
//!
//! The mapping is:
//!
//! - Each Hydro op (one per `op_id`) becomes one `CapsysOperator`.
//! - Graph edges come from `hydro_optimize::rewrites::op_id_to_parents`, which
//!   already handles cycle backedges and the Tee/Partition indirections.
//! - Per-task resource costs come from `ILPAnalysisInputs::per_op_load`
//!   (for regular ops) or from `NetworkCostTable::network_cost` (for
//!   `HydroNode::Network` ops that don't have direct SAR measurements).
//! - `parallelism` is the cluster size of the op's root `LocationId`, looked
//!   up from `LocationInfo`.
//! - `outboundtype` is inferred from whether downstream ops live at the same
//!   location (`FORWARD`) or a different one (`REBALANCE`). `HASH` is not
//!   inferred automatically yet; callers that know their partitioning scheme
//!   can post-process the output.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use hydro_lang::compile::ir::{HydroNode, HydroRoot, traverse_dfir};
use hydro_lang::location::dynamic::LocationId;
use hydro_optimize::decouple_analysis::ILPAnalysisInputs;
use hydro_optimize::deploy_and_analyze::ReusableClusters;
use hydro_optimize::parse_results::SarStats;
use hydro_optimize::repair::{cycle_source_to_sink_parent, inject_id};
use hydro_optimize::rewrites::op_id_to_parents;

use crate::types::CapsysOperator;

/// Printable ASCII subset (sans `"` and `\`) used to assign stable single-byte
/// operator ids. 93 characters — enough for most Hydro programs. Callers with
/// more ops will want to fuse first.
const ID_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!#$%&'()*+,-./:;<=>?@[]^_`{|}~";

/// Per-root-`LocationId` parallelism + a human-friendly name. Build with
/// [`LocationInfo::from_clusters`] (when you already have a
/// [`ReusableClusters`]) or construct directly.
#[derive(Debug, Clone, Default)]
pub struct LocationInfo {
    /// Map from root `LocationId` (see [`LocationId::root`]) to `(name, size)`.
    pub entries: HashMap<LocationId, (String, usize)>,
}

impl LocationInfo {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, location: LocationId, name: impl Into<String>, size: usize) {
        self.entries
            .insert(location.root().clone(), (name.into(), size));
    }

    pub fn with(mut self, location: LocationId, name: impl Into<String>, size: usize) -> Self {
        self.insert(location, name, size);
        self
    }

    /// Populate location entries from a `ReusableClusters` (the same helper
    /// `hydro_optimize` uses to track known clusters).
    pub fn from_clusters<I>(clusters: &ReusableClusters, locations: I) -> Self
    where
        I: IntoIterator<Item = LocationId>,
    {
        let mut info = Self::default();
        for loc in locations {
            if let Some((name, size)) = clusters.location_name_and_num(&loc) {
                info.insert(loc, name, size);
            }
        }
        info
    }

    pub fn lookup(&self, location: &LocationId) -> Option<&(String, usize)> {
        self.entries.get(location.root())
    }
}

/// Knobs for [`convert`]. Sensible defaults are used if you call
/// `ConvertOptions::default()`.
#[derive(Debug, Clone)]
pub struct ConvertOptions {
    /// Fallback parallelism when a location is not found in `LocationInfo`.
    pub default_parallelism: usize,
    /// Drop ops whose per-task `compute + state + network` is exactly zero
    /// (unless they are structural — i.e. they have at least one child). This
    /// keeps the CAPSys graph tidy when Hydro has added many sugar/metadata
    /// nodes without measured load.
    pub drop_zero_cost_leaves: bool,
    /// If set, operator names are truncated to this many characters. Hydro's
    /// `print_root` embeds entire closure bodies, which bloats the JSON.
    pub name_max_len: Option<usize>,
}

impl Default for ConvertOptions {
    fn default() -> Self {
        Self {
            default_parallelism: 1,
            drop_zero_cost_leaves: false,
            name_max_len: Some(64),
        }
    }
}

/// Per-op intermediate record we build up during the IR walk, before filtering
/// / serialization.
#[derive(Debug, Clone)]
struct RawOp {
    name: String,
    location: LocationId,
    is_network: bool,
}

/// Top-level entry point: walk the IR and return a Vec of `CapsysOperator`s.
///
/// `ir` is taken as `&mut` because `traverse_dfir` (Hydro's canonical walker)
/// wants mutable access. The IR is not structurally modified.
pub fn convert(
    ir: &mut [HydroRoot],
    inputs: &ILPAnalysisInputs,
    location_info: &LocationInfo,
    options: &ConvertOptions,
) -> Vec<CapsysOperator> {
    // 1. Make sure every IR node has a fresh op_id stamped on its metadata
    //    (required by op_id_to_parents / cycle_source_to_sink_parent). This
    //    matches what `hydro_optimize`'s pipeline does before its own
    //    analyses.
    inject_id(ir);

    // 2. Resolve cycle source → sink-input mapping and pull per-op parents.
    let cycles = cycle_source_to_sink_parent(ir);
    let parents_map = op_id_to_parents(ir, None, &cycles);

    // Subsequent steps were renumbered; original numbering left in prose.
    // 3. Walk the IR once more to collect per-op display metadata. Roots are
    //    treated as extra sink-style ops.
    let raw_ops_cell: RefCell<HashMap<usize, RawOp>> = RefCell::new(HashMap::new());
    traverse_dfir(
        ir,
        |root, op_id| {
            raw_ops_cell.borrow_mut().insert(
                *op_id,
                RawOp {
                    name: pretty_root_name(root),
                    location: root.input_metadata().location_id.clone(),
                    is_network: false,
                },
            );
        },
        |node, op_id| {
            raw_ops_cell.borrow_mut().insert(
                *op_id,
                RawOp {
                    name: pretty_node_name(node),
                    location: node.metadata().location_id.clone(),
                    is_network: matches!(node, HydroNode::Network { .. }),
                },
            );
        },
    );
    let raw_ops = raw_ops_cell.into_inner();

    // 3. Build children map (inverse of parents_map), skipping edges whose
    //    parent or child is missing a RawOp entry (shouldn't happen, but be
    //    defensive).
    let children_map = invert_parents(&parents_map);

    // 4. Optionally drop zero-cost leaves to shrink the graph before assigning
    //    single-char ids (we only have ID_CHARS.len() of those).
    let retained_ids: Vec<usize> = raw_ops
        .keys()
        .copied()
        .filter(|op_id| {
            if !options.drop_zero_cost_leaves {
                return true;
            }
            let has_children = children_map
                .get(op_id)
                .map(|c| !c.is_empty())
                .unwrap_or(false);
            let has_parents = parents_map
                .get(op_id)
                .map(|p| !p.is_empty())
                .unwrap_or(false);
            let has_cost = cost_for_op(*op_id, raw_ops.get(op_id).unwrap(), inputs).is_some();
            has_cost || has_children || has_parents
        })
        .collect();

    // Deterministic ordering: sort by op_id so that assigned CAPSys ids are
    // reproducible for a given IR.
    let mut retained_sorted = retained_ids;
    retained_sorted.sort_unstable();

    assert!(
        retained_sorted.len() <= ID_CHARS.len(),
        "hydro_to_capsys: {} operators exceeds the {}-char CAPSys id budget; \
         consider fusing or setting ConvertOptions::drop_zero_cost_leaves",
        retained_sorted.len(),
        ID_CHARS.len()
    );

    let op_id_to_capsys_id: HashMap<usize, String> = retained_sorted
        .iter()
        .enumerate()
        .map(|(i, op_id)| (*op_id, std::str::from_utf8(&[ID_CHARS[i]]).unwrap().to_string()))
        .collect();

    // 5. Emit a CapsysOperator per retained op.
    let mut out: Vec<CapsysOperator> = Vec::with_capacity(retained_sorted.len());
    for op_id in &retained_sorted {
        let raw = raw_ops.get(op_id).expect("retained id must have RawOp");
        let (parallelism, _location_name) = match location_info.lookup(&raw.location) {
            Some((n, p)) => (*p, Some(n.clone())),
            None => (options.default_parallelism, None),
        };

        let sar = cost_for_op(*op_id, raw, inputs).unwrap_or_default();
        let name = truncate_name(&raw.name, options.name_max_len);

        let up_node: Vec<String> = parents_map
            .get(op_id)
            .into_iter()
            .flat_map(|ps| ps.iter())
            .filter_map(|pid| op_id_to_capsys_id.get(pid).cloned())
            .collect();

        let children = children_map
            .get(op_id)
            .cloned()
            .unwrap_or_default();
        let outboundtype =
            determine_outbound_type(&raw.location, &children, &raw_ops);

        let down_node: Vec<String> = children
            .iter()
            .filter_map(|cid| op_id_to_capsys_id.get(cid).cloned())
            .collect();

        out.push(CapsysOperator {
            id: op_id_to_capsys_id[op_id].clone(),
            name,
            parallelism,
            state: sar.memory,
            compute: sar.cpu,
            network: sar.network,
            outboundtype: outboundtype.to_string(),
            down_node,
            up_node,
        });
    }

    out
}

/// Compute the per-task cost for a single op. Returns `None` if nothing is
/// known (so callers can decide whether to drop or zero-fill).
fn cost_for_op(op_id: usize, raw: &RawOp, inputs: &ILPAnalysisInputs) -> Option<SarStats> {
    if let Some(load) = inputs.per_op_load.get(&op_id).copied() {
        return Some(load);
    }
    if raw.is_network {
        let count = inputs.op_counts.get(&op_id).copied().unwrap_or(0);
        let size = inputs.op_output_sizes.get(&op_id).copied().unwrap_or(0);
        if count > 0 && size > 0 {
            return Some(inputs.network_cost_table.network_cost(count, size));
        }
    }
    None
}

/// Pick `FORWARD` when every downstream child roots at the same location,
/// `REBALANCE` when any child lives on a different root location, and `""`
/// when the op has no children (i.e. a sink).
fn determine_outbound_type(
    my_location: &LocationId,
    children: &[usize],
    raw_ops: &HashMap<usize, RawOp>,
) -> &'static str {
    if children.is_empty() {
        return "";
    }
    let mine = my_location.root();
    let any_different = children
        .iter()
        .filter_map(|c| raw_ops.get(c))
        .any(|child| child.location.root() != mine);
    if any_different { "REBALANCE" } else { "FORWARD" }
}

fn invert_parents(parents_map: &HashMap<usize, Vec<usize>>) -> HashMap<usize, Vec<usize>> {
    let mut out: HashMap<usize, Vec<usize>> = HashMap::new();
    let mut seen: HashMap<usize, HashSet<usize>> = HashMap::new();
    for (&child, parents) in parents_map {
        for &p in parents {
            if seen.entry(p).or_default().insert(child) {
                out.entry(p).or_default().push(child);
            }
        }
    }
    // Stable ordering for reproducibility.
    for v in out.values_mut() {
        v.sort_unstable();
    }
    out
}

fn pretty_node_name(node: &HydroNode) -> String {
    // Hydro's `print_root` on a node embeds the whole closure, which is
    // readable but long. For CAPSys we want short tags; fall back to the
    // variant name for most ops and keep the short-form for sources / sinks
    // where the payload is informative.
    match node {
        HydroNode::Placeholder => "Placeholder".into(),
        HydroNode::Cast { .. } => "Cast".into(),
        HydroNode::ObserveNonDet { .. } => "ObserveNonDet".into(),
        HydroNode::Source { .. } => node.print_root(),
        HydroNode::SingletonSource { .. } => "SingletonSource".into(),
        HydroNode::CycleSource { cycle_id, .. } => format!("CycleSource({cycle_id})"),
        HydroNode::Tee { .. } => "Tee".into(),
        HydroNode::Partition { .. } => "Partition".into(),
        HydroNode::Network { .. } => "Network".into(),
        HydroNode::Map { .. } => "Map".into(),
        HydroNode::Filter { .. } => "Filter".into(),
        HydroNode::FilterMap { .. } => "FilterMap".into(),
        HydroNode::FlatMap { .. } => "FlatMap".into(),
        HydroNode::Fold { .. } => "Fold".into(),
        HydroNode::FoldKeyed { .. } => "FoldKeyed".into(),
        HydroNode::Reduce { .. } => "Reduce".into(),
        HydroNode::ReduceKeyed { .. } => "ReduceKeyed".into(),
        HydroNode::Unique { .. } => "Unique".into(),
        HydroNode::Sort { .. } => "Sort".into(),
        HydroNode::Counter { .. } => "Counter".into(),
        HydroNode::Chain { .. } => "Chain".into(),
        HydroNode::MergeOrdered { .. } => "MergeOrdered".into(),
        HydroNode::Inspect { .. } => "Inspect".into(),
        HydroNode::Enumerate { .. } => "Enumerate".into(),
        HydroNode::CrossProduct { .. } => "CrossProduct".into(),
        HydroNode::CrossSingleton { .. } => "CrossSingleton".into(),
        HydroNode::Join { .. } => "Join".into(),
        HydroNode::Difference { .. } => "Difference".into(),
        HydroNode::AntiJoin { .. } => "AntiJoin".into(),
        HydroNode::DeferTick { .. } => "DeferTick".into(),
        HydroNode::BeginAtomic { .. } => "BeginAtomic".into(),
        HydroNode::EndAtomic { .. } => "EndAtomic".into(),
        HydroNode::Batch { .. } => "Batch".into(),
        HydroNode::YieldConcat { .. } => "YieldConcat".into(),
        _ => node.print_root(),
    }
}

fn pretty_root_name(root: &HydroRoot) -> String {
    match root {
        HydroRoot::ForEach { .. } => "ForEach".into(),
        HydroRoot::SendExternal { .. } => "SendExternal".into(),
        HydroRoot::DestSink { .. } => "DestSink".into(),
        HydroRoot::CycleSink { cycle_id, .. } => format!("CycleSink({cycle_id})"),
        HydroRoot::EmbeddedOutput { .. } => "EmbeddedOutput".into(),
        HydroRoot::Null { .. } => "Null".into(),
    }
}

fn truncate_name(name: &str, max: Option<usize>) -> String {
    match max {
        None => name.to_owned(),
        Some(n) if name.chars().count() <= n => name.to_owned(),
        Some(n) => {
            let mut out: String = name.chars().take(n).collect();
            out.push('…');
            out
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn printable_ids_are_unique_and_json_safe() {
        let set: HashSet<&u8> = ID_CHARS.iter().collect();
        assert_eq!(set.len(), ID_CHARS.len());
        // Disallow characters that would need JSON escaping.
        assert!(!ID_CHARS.contains(&b'"'));
        assert!(!ID_CHARS.contains(&b'\\'));
    }
}
