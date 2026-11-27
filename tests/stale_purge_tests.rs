use redborder_vortex::state::store_manager::keys_to_purge;
use redborder_vortex::state::{
    ClusterBackend, InMemoryClusterBackend, StateStore, MOBILITY_STORE, SENSOR_PSQL_STORE,
    WLC_PSQL_STORE,
};
use serde_json::{json, Map, Value};
use std::collections::HashSet;
use std::sync::Arc;

fn rec(v: Value) -> Map<String, Value> {
    v.as_object().unwrap().clone()
}

fn set(keys: &[&str]) -> HashSet<String> {
    keys.iter().map(|s| s.to_string()).collect()
}

#[test]
fn keys_to_purge_returns_current_minus_valid() {
    let cur = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let mut out = keys_to_purge(cur, &set(&["b"]));
    out.sort();
    assert_eq!(out, vec!["a".to_string(), "c".to_string()]);
    assert!(keys_to_purge(vec!["x".to_string()], &set(&["x", "y"])).is_empty());
}

#[test]
fn standalone_missing_record_is_purged() {
    let st = StateStore::new_standalone();
    st.put_record(WLC_PSQL_STORE, "aa:aa", rec(json!({"ap_name": "old"}))).unwrap();
    st.put_record(WLC_PSQL_STORE, "bb:bb", rec(json!({"ap_name": "keep"}))).unwrap();

    let purged = st.purge_missing(WLC_PSQL_STORE, Some(&set(&["bb:bb"]))).unwrap();
    assert_eq!(purged, vec!["aa:aa".to_string()]);
    assert!(st.get_record(WLC_PSQL_STORE, "aa:aa").is_none());
    assert!(st.get_record(WLC_PSQL_STORE, "bb:bb").is_some());
}

#[test]
fn query_error_does_not_purge() {
    let st = StateStore::new_standalone();
    st.put_record(SENSOR_PSQL_STORE, "u1", rec(json!({"sensor_name": "s"}))).unwrap();
    let purged = st.purge_missing(SENSOR_PSQL_STORE, None).unwrap();
    assert!(purged.is_empty());
    assert!(st.get_record(SENSOR_PSQL_STORE, "u1").is_some());
}

#[test]
fn empty_valid_set_purges_whole_store() {
    let st = StateStore::new_standalone();
    st.put_record(SENSOR_PSQL_STORE, "u1", rec(json!({"sensor_name": "s"}))).unwrap();
    st.purge_missing(SENSOR_PSQL_STORE, Some(&HashSet::new())).unwrap();
    assert!(st.get_store(SENSOR_PSQL_STORE).is_empty());
}

#[test]
fn purge_removes_from_backend_and_peer_via_evict_message() {
    let backend = Arc::new(InMemoryClusterBackend::new());
    let a = StateStore::with_backend(backend.clone());
    let b = StateStore::with_backend(backend.clone());
    a.put_record(WLC_PSQL_STORE, "aa:aa", rec(json!({"ap_name": "old"}))).unwrap();
    b.sync_from_backend().unwrap();
    assert!(b.get_record(WLC_PSQL_STORE, "aa:aa").is_some());

    let purged = a.purge_missing(WLC_PSQL_STORE, Some(&set(&[]))).unwrap();
    assert_eq!(purged, vec!["aa:aa".to_string()]);
    assert!(backend.hgetall_store(WLC_PSQL_STORE).unwrap().is_empty());

    // The Redis backend publishes this "evict" payload; peers apply it locally.
    let payload = json!({"op": "evict", "store": WLC_PSQL_STORE, "keys": purged}).to_string();
    redborder_vortex::state::process_pubsub_payload(&b, &payload);
    assert!(b.get_record(WLC_PSQL_STORE, "aa:aa").is_none());
}

#[test]
fn purge_catches_key_only_present_in_backend() {
    let backend = Arc::new(InMemoryClusterBackend::new());
    backend.hset_record(WLC_PSQL_STORE, "zz:zz", &rec(json!({"ap_name": "x"}))).unwrap();
    let st = StateStore::with_backend(backend.clone());
    st.purge_missing(WLC_PSQL_STORE, Some(&set(&[]))).unwrap();
    assert!(backend.hgetall_store(WLC_PSQL_STORE).unwrap().is_empty());
}

#[test]
fn purge_missing_works_on_any_store() {
    let st = StateStore::new_standalone();
    st.put_record(MOBILITY_STORE, "m1", rec(json!({"a": 1}))).unwrap();
    st.purge_missing(MOBILITY_STORE, Some(&set(&[]))).unwrap();
    assert!(st.get_record(MOBILITY_STORE, "m1").is_none());
}
