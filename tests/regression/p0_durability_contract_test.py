#!/usr/bin/env python3
"""Fast structural guards for the P0 durability/HA acknowledgement contract."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def function_body(source: str, signature: str) -> str:
    start = source.index(signature)
    opening = source.index("{", start)
    depth = 0
    for pos in range(opening, len(source)):
        char = source[pos]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[opening : pos + 1]
    raise AssertionError(f"unterminated function: {signature}")


def ordered(body: str, *needles: str) -> None:
    positions = [body.index(needle) for needle in needles]
    assert positions == sorted(positions), (needles, positions)


aof = (ROOT / "src/aof_batch.c").read_text()
append = function_body(aof, "uint64_t AOF_append_sealed")
ordered(
    append,
    "seq = sealed_claim_slot_sequence()",
    "batch_id = seq + 1",
    "slot->batch_id = batch_id",
    "atomic_store_explicit(&slot->state, 2",
)
assert "next_batch_id" not in aof

sync = function_body(aof, "static void *sealed_sync_thread")
ordered(
    sync,
    "sealed_writev_full",
    "sealed_fsync_full",
    "last_synced_batch_id",
)
assert "sealed_shared_failed()" in sync

replay = function_body(aof, "size_t AOF_sealed_replay")
assert "ftruncate(local_fd, (off_t)last_good)" in replay
assert "return corrupt ? SIZE_MAX : applied" in replay

gc = function_body(aof, "int AOF_sealed_gc")
assert "RF_EFFECTIVE_WORKER_COUNT" in gc
assert "ENOTSUP" in gc

persistence = (ROOT / "src/persistence_zp.c").read_text()
init = function_body(persistence, "void Persistence_zp_init")
assert "g_aof_enabled = 1" in init
assert "aof_flush_ms > 0" not in init

cluster = (ROOT / "src/cluster.c").read_text()
worker = function_body(cluster, "static void run_worker")
ordered(worker, "if (!Persistence_is_ready())", "RAMForge_HA_init()")
assert "continuing without HA" not in worker
assert "exit(EXIT_FAILURE)" in worker

http = (ROOT / "src/http_server.c").read_text()
write_gate = function_body(http, "static int is_write_path")
assert 'http_path_equals(url, "/broker/commit")' in write_gate
assert 'http_path_ends_with(url, "/produce")' in write_gate
read_gate = function_body(http, "static int is_linearizable_read_path")
assert 'http_path_starts_with(url, "/kv/")' in read_gate
process_request = function_body(http, "static void process_request")
assert "(result == -4) ? 400" in process_request

routes = (ROOT / "src/app_routes.c").read_text()
timeout_parser = function_body(routes, "static int requested_sync_timeout_ms")
assert "query_has_parameter" in timeout_parser
assert "value == 0" in timeout_parser
assert "value > (uint64_t)INT_MAX" in timeout_parser
linearizable_read = function_body(routes, "static int linearizable_read_gate")
ordered(
    linearizable_read,
    "RAMForge_HA_is_leader",
    "RAMForge_HA_has_fresh_quorum",
    "RAMForge_HA_linearizable_read_barrier",
    "rf_wait_ha_read_barrier",
)
assert "linearizable_read_gate(req, res)" in function_body(
    routes, "int kv_get_handler"
)

broker = (ROOT / "src/rf_broker.c").read_text()
replay_broker = function_body(broker, "void rf_broker_replay_aof")
assert "if (!data)" in replay_broker
assert "sz != sizeof(ha_read_barrier_rec_t)" in replay_broker
assert "sizeof(aof_kv_put_t) + (size_t)h->value_len != sz" in replay_broker
assert "sz != sizeof(aof_kv_del_t)" in replay_broker
assert "sz != sizeof(aof_topic_rec_t)" in replay_broker
assert "replay_plain_batch_atomic(data, messages_size)" in replay_broker
assert "g_replay_apply_error = 1" in replay_broker
commit = function_body(broker, "int rf_commit_checked")
ordered(
    commit,
    "rf_mutation_lock",
    "rf_persist_apply_record",
    "rf_offset_load",
    "rf_mutation_unlock",
)
assert "RAMForge_HA_replicate_write" not in commit
assert "AOF_append_sealed" not in commit

truncate = function_body(broker, "int rf_delete_before_checked")
ordered(truncate, "rf_mutation_lock", "rf_persist_apply_record", "rf_mutation_unlock")
assert "rf_invalidate_slot" not in truncate

retention = function_body(broker, "int rf_set_retention_checked")
ordered(retention, "rf_mutation_lock", "rf_persist_apply_record", "rf_mutation_unlock")

ha_replication = (ROOT / "src/ramforge_ha_replication.c").read_text()
apply_record = function_body(ha_replication, "int HA_apply_committed_entries")
ordered(
    apply_record,
    "AOF_append_sealed",
    "AOF_sealed_wait",
    "RAMForge_HA_replay_record",
    "rf_bus_publish_wait_except",
    "HA_update_applied_index",
)
assert "ha_store_append_log" in function_body(
    ha_replication, "uint64_t HA_append_local_entry"
)
assert "HA_begin_election_durable" in function_body(
    ha_replication, "static void start_election"
)

ha_net = (ROOT / "src/ramforge_ha_net.c").read_text()
append_entries = function_body(ha_net, "static void handle_append_entries")
ordered(
    append_entries,
    "HA_step_down_to_term",
    "HA_follower_store_entries",
    "HA_update_commit_index",
    "HA_apply_committed_entries",
    "send_append_response_to(h, match_index, 1)",
)
vote_request = function_body(ha_net, "static void handle_vote_request")
assert "ha_candidate_log_is_up_to_date" in vote_request
assert "HA_grant_vote_durable" in vote_request

ha_store = (ROOT / "src/ramforge_ha_store.c").read_text()
assert "fdatasync(store->state_fd)" in ha_store
assert "fdatasync(store->log_fd)" in ha_store
assert "header.term < previous_term" in ha_store

tls = (ROOT / "src/ramforge_ha_tls.c").read_text()
identity = function_body(tls, "static int cert_matches_node_id")
assert "X509_check_host(cert, node_id" in identity
assert "adv_host" not in identity
assert "X509_check_ip" not in identity

print("PASS P0 durability/HA source contracts")
