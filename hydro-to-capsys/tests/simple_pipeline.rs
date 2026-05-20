//! Integration-style test: build a small Hydro pipeline, feed it through
//! `hydro_to_capsys::convert`, and assert on the JSON shape.
//!
//! These tests pull in `hydro_lang`/`stageleft` as dev-dependencies, so they
//! only compile when dev deps are built (i.e. `cargo test` — not
//! `cargo check --lib`).

use std::collections::HashMap;

use hydro_lang::compile::ir::deep_clone;
use hydro_lang::deploy::HydroDeploy;
use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::prelude::*;
use hydro_optimize::decouple_analysis::ILPAnalysisInputs;
use hydro_optimize::parse_results::{NetworkCostTable, SarStats};
use hydro_to_capsys::{ConvertOptions, LocationInfo, convert, operators_to_json};
use stageleft::q;

/// Build a pipeline spanning two clusters and convert it. The shape is:
///
/// ```text
/// source_cluster: source_iter -> map -> broadcast (network) -> sink_cluster: values -> for_each
/// ```
#[test]
fn convert_simple_two_cluster_pipeline() {
    let mut builder = FlowBuilder::new();
    let source_cluster = builder.cluster::<()>();
    let sink_cluster = builder.cluster::<()>();

    let source_loc = source_cluster.id();
    let sink_loc = sink_cluster.id();

    source_cluster
        .source_iter(q!(0..100))
        .map(q!(|x| (x, x * 2)))
        .broadcast(&sink_cluster, TCP.fail_stop().bincode(), nondet!(/** test */))
        .values()
        .assume_ordering::<TotalOrder>(nondet!(/** test */))
        .assume_retries::<ExactlyOnce>(nondet!(/** test */))
        .for_each(q!(|x| {
            std::hint::black_box(x);
        }));

    let built = builder.with_default_optimize::<HydroDeploy>();
    let mut ir = deep_clone(built.ir());

    // Synthetic inputs: pretend every op has a small measured load. This is
    // the same shape `hydro_optimize`'s real ILP pipeline uses.
    let mut per_op_load: HashMap<usize, SarStats> = HashMap::new();
    for i in 0..64 {
        per_op_load.insert(
            i,
            SarStats {
                cpu: 10.0,
                cpu_user: 8.0,
                network: 0.0,
                memory: 1.0,
                io: 0.0,
            },
        );
    }

    let inputs = ILPAnalysisInputs {
        op_counts: HashMap::new(),
        op_output_sizes: HashMap::new(),
        // `network_cost` isn't exercised here (op_counts + op_output_sizes are
        // empty), but we still need a non-empty table so the ceiling lookup
        // doesn't panic if something does call it.
        network_cost_table: NetworkCostTable::from_calibration(vec![(1, SarStats::default())]),
        per_op_load,
        consider_partitioning: false,
        cluster_size: 2,
    };

    let location_info = LocationInfo::default()
        .with(source_loc, "source_cluster", 2)
        .with(sink_loc, "sink_cluster", 4);

    let ops = convert(&mut ir, &inputs, &location_info, &ConvertOptions::default());

    assert!(!ops.is_empty(), "expected at least one CAPSys operator");
    let json = operators_to_json(&ops).expect("serialize");
    println!("{}", json);

    // Structural checks:
    //  - every id is exactly one character
    //  - every id in up_node / down_node refers to a known op
    let known: std::collections::HashSet<&str> = ops.iter().map(|op| op.id.as_str()).collect();
    for op in &ops {
        assert_eq!(op.id.chars().count(), 1, "id must be 1 char: {:?}", op.id);
        for u in &op.up_node {
            assert!(known.contains(u.as_str()), "unknown upstream {u}");
        }
        for d in &op.down_node {
            assert!(known.contains(d.as_str()), "unknown downstream {d}");
        }
    }

    // At least one op should live on each cluster.
    let parallelisms: std::collections::HashSet<usize> =
        ops.iter().map(|o| o.parallelism).collect();
    assert!(
        parallelisms.contains(&2) && parallelisms.contains(&4),
        "expected both cluster parallelisms to appear, got {:?}",
        parallelisms
    );

    // At least one cross-cluster edge should be REBALANCE (since the network
    // spans two clusters) and at least one op should be a sink.
    let has_rebalance = ops.iter().any(|o| o.outboundtype == "REBALANCE");
    let has_sink = ops.iter().any(|o| o.outboundtype.is_empty());
    assert!(has_rebalance, "expected at least one REBALANCE op");
    assert!(has_sink, "expected at least one sink op (empty outbound)");
}
