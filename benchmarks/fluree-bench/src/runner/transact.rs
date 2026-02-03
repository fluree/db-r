//! Pipelined transaction runner with back-pressure handling.

use std::sync::Arc;
use std::time::{Duration, Instant};

use fluree_db_api::FlureeClient;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::datagen;
use crate::metrics::collector::{BackPressureEvent, MetricsCollector};
use crate::setup;
use crate::IngestArgs;

/// Output from a single commit task.
struct TxnResult {
    batch_idx: usize,
    entity_count: usize,
    flake_count: usize,
    elapsed: Duration,
    back_pressure: Option<BackPressureEvent>,
}

/// Maximum back-pressure retry delay.
const MAX_BACKOFF: Duration = Duration::from_secs(5);
/// Initial back-pressure retry delay.
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);

/// Run the ingest benchmark.
pub async fn run(
    fluree: &Arc<FlureeClient>,
    args: &IngestArgs,
    collector: &mut MetricsCollector,
) -> Result<(), Box<dyn std::error::Error>> {
    let alias = setup::normalize_alias(&args.common.ledger);
    let idx_cfg = setup::index_config(&args.common);

    let units = datagen::compute_units(args.common.data_size_mb);
    let entities_per_unit = datagen::supply_chain::ENTITIES_PER_UNIT;

    tracing::info!(
        units,
        entities_per_unit,
        batch_size = args.batch_size,
        concurrency = args.concurrency,
        "Starting ingest"
    );

    let inflight = Arc::new(Semaphore::new(args.concurrency));
    let mut tasks: JoinSet<TxnResult> = JoinSet::new();
    let mut batch_idx: usize = 0;

    for unit_id in 0..units {
        let mut offset = 0;
        while offset < entities_per_unit {
            let count = args.batch_size.min(entities_per_unit - offset);
            let batch_json = datagen::supply_chain::generate_batch(unit_id, offset, count);
            offset += count;
            batch_idx += 1;

            let permit = inflight
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore closed");

            let fluree = Arc::clone(fluree);
            let alias = alias.clone();
            let idx_cfg = idx_cfg.clone();
            let current_batch = batch_idx;
            let entity_count = count;

            tasks.spawn(async move {
                let t0 = Instant::now();
                let mut back_pressure: Option<BackPressureEvent> = None;
                let mut delay = INITIAL_BACKOFF;
                let mut retries = 0u32;
                let bp_start = Instant::now();

                let result = loop {
                    let res = fluree
                        .graph(&alias)
                        .transact()
                        .insert(&batch_json)
                        .index_config(idx_cfg.clone())
                        .commit()
                        .await;

                    match &res {
                        Err(e) if is_back_pressure(e) => {
                            retries += 1;
                            if retries == 1 {
                                tracing::warn!(
                                    batch = current_batch,
                                    entities = entity_count,
                                    error = %e,
                                    "Back-pressure: novelty at max, waiting for indexer to catch up"
                                );
                            } else {
                                tracing::warn!(
                                    batch = current_batch,
                                    retries,
                                    blocked_ms = bp_start.elapsed().as_millis() as u64,
                                    next_delay_ms = delay.as_millis() as u64,
                                    "Back-pressure: still waiting (retry #{retries})"
                                );
                            }
                            tokio::time::sleep(delay).await;
                            delay = (delay * 2).min(MAX_BACKOFF);
                        }
                        _ => {
                            if retries > 0 {
                                let bp_duration = bp_start.elapsed();
                                tracing::info!(
                                    batch = current_batch,
                                    retries,
                                    blocked_ms = bp_duration.as_millis() as u64,
                                    "Back-pressure resolved, commit proceeding"
                                );
                                back_pressure = Some(BackPressureEvent {
                                    duration: bp_duration,
                                    retries,
                                });
                            }
                            break res;
                        }
                    }
                };

                drop(permit);

                let elapsed = t0.elapsed();
                match result {
                    Ok(r) => TxnResult {
                        batch_idx: current_batch,
                        entity_count,
                        flake_count: r.receipt.flake_count,
                        elapsed,
                        back_pressure,
                    },
                    Err(e) => {
                        tracing::error!(batch = current_batch, error = %e, "Commit failed");
                        TxnResult {
                            batch_idx: current_batch,
                            entity_count,
                            flake_count: 0,
                            elapsed,
                            back_pressure,
                        }
                    }
                }
            });

            // Eagerly drain completed tasks.
            drain_completed(&mut tasks, collector);
        }
    }

    // Drain remaining tasks.
    while let Some(join_result) = tasks.join_next().await {
        if let Ok(txn) = join_result {
            record_result(txn, collector);
        }
    }

    Ok(())
}

fn drain_completed(tasks: &mut JoinSet<TxnResult>, collector: &mut MetricsCollector) {
    while let Some(join_result) = tasks.try_join_next() {
        if let Ok(txn) = join_result {
            record_result(txn, collector);
        }
    }
}

fn record_result(txn: TxnResult, collector: &mut MetricsCollector) {
    tracing::info!(
        batch = txn.batch_idx,
        entities = txn.entity_count,
        flakes = txn.flake_count,
        ms = txn.elapsed.as_millis() as u64,
        bp_retries = txn.back_pressure.as_ref().map(|bp| bp.retries).unwrap_or(0),
        "Batch committed"
    );

    collector.record_txn(txn.elapsed, txn.flake_count, txn.entity_count);
    if let Some(bp) = txn.back_pressure {
        collector.record_back_pressure(bp);
    }
}

/// Check if an API error is a back-pressure error (novelty at max).
fn is_back_pressure(err: &fluree_db_api::ApiError) -> bool {
    match err {
        fluree_db_api::ApiError::Transact(te) => matches!(
            te,
            fluree_db_transact::TransactError::NoveltyAtMax
                | fluree_db_transact::TransactError::NoveltyWouldExceed { .. }
        ),
        _ => false,
    }
}
