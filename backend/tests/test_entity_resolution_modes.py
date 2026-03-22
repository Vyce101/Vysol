import asyncio
import json
from pathlib import Path

from core import entity_resolution_engine as engine
from core import graph_store


def _prepare_world(tmp_path: Path, monkeypatch, world_id: str = "world-entity-resolution") -> tuple[Path, graph_store.GraphStore]:
    world_root = tmp_path / world_id
    world_root.mkdir(parents=True, exist_ok=True)
    meta_path = world_root / "meta.json"
    graph_path = world_root / "world_graph.gexf"
    meta_path.write_text(json.dumps({"ingestion_status": "complete"}), encoding="utf-8")

    monkeypatch.setattr(engine, "world_meta_path", lambda _: meta_path)
    monkeypatch.setattr(graph_store, "world_graph_path", lambda _: graph_path)
    monkeypatch.setattr(engine, "load_settings", lambda: {})

    engine._abort_events.clear()
    engine._sse_queues.clear()
    engine._sse_locks.clear()
    engine._states.clear()
    engine._state_locks.clear()
    engine._active_runs.clear()

    return meta_path, graph_store.GraphStore(world_id)


def test_exact_only_mode_stops_after_normalized_match_pass(tmp_path, monkeypatch):
    world_id = "world-exact-only"
    _, store = _prepare_world(tmp_path, monkeypatch, world_id)
    store.graph.add_node("node-a", display_name="Alice", description="Primary", claims=[], source_chunks=[])
    store.graph.add_node("node-b", display_name="ALICE", description="Duplicate", claims=[], source_chunks=[])
    store.graph.add_node("node-c", display_name="Bob", description="Other", claims=[], source_chunks=[])
    store.save()

    async def _fail_choose(*args, **kwargs):
        raise AssertionError("Chooser should not run in exact-only mode.")

    async def _fail_combine(*args, **kwargs):
        raise AssertionError("Combiner should not run in exact-only mode.")

    rebuild_calls: list[list[str]] = []

    async def _fake_rebuild_unique_node_index(_world_id, active_store, batch_size, cooldown_seconds, abort_event=None):
        rebuild_calls.append(sorted(active_store.graph.nodes()))
        assert batch_size == 32
        assert cooldown_seconds == 0.0
        return object()

    monkeypatch.setattr(engine, "_choose_matches", _fail_choose)
    monkeypatch.setattr(engine, "_combine_entities", _fail_combine)
    monkeypatch.setattr(engine, "_rebuild_unique_node_index", _fake_rebuild_unique_node_index)

    asyncio.run(engine.start_entity_resolution(world_id, 50, False, True, "exact_only"))

    status = engine.get_resolution_status(world_id)
    events = engine.drain_sse_events(world_id)
    reloaded = graph_store.GraphStore(world_id)

    assert status["status"] == "complete"
    assert status["resolution_mode"] == "exact_only"
    assert status["resolved_entities"] == 2
    assert status["unresolved_entities"] == 1
    assert status["auto_resolved_pairs"] == 1
    assert reloaded.get_node_count() == 2
    assert rebuild_calls == [["node-a", "node-c"]]
    assert all(event.get("phase") not in {"candidate_search", "chooser", "combiner"} for event in events)


def test_exact_then_ai_mode_runs_chooser_and_combiner(tmp_path, monkeypatch):
    world_id = "world-exact-then-ai"
    _, store = _prepare_world(tmp_path, monkeypatch, world_id)
    store.graph.add_node("node-a", display_name="Alice", description="Primary", claims=[], source_chunks=[])
    store.graph.add_node("node-b", display_name="Alicia", description="Possible duplicate", claims=[], source_chunks=[])
    store.graph.add_node("node-c", display_name="Bob", description="Other", claims=[], source_chunks=[])
    store.save()

    rebuild_calls: list[list[str]] = []
    refreshed_merges: list[tuple[str, list[str]]] = []
    fake_unique_node_store = object()

    async def _fake_rebuild_unique_node_index(_world_id, active_store, batch_size, cooldown_seconds, abort_event=None):
        rebuild_calls.append(sorted(active_store.graph.nodes()))
        assert batch_size == 32
        assert cooldown_seconds == 0.0
        return fake_unique_node_store

    async def _fake_refresh_unique_node_index_after_merge(
        _vector_store,
        _active_store,
        winner_id,
        loser_ids,
        batch_size,
        cooldown_seconds,
        abort_event=None,
    ):
        refreshed_merges.append((winner_id, list(loser_ids)))
        assert batch_size == 32
        assert cooldown_seconds == 0.0

    def _fake_query_candidates(active_store, unique_node_vector_store, anchor_id, remaining_ids, _top_k):
        assert active_store.world_id == world_id
        assert unique_node_vector_store is fake_unique_node_store
        assert anchor_id == "node-a"
        assert "node-b" in remaining_ids
        candidate = engine._node_snapshot(active_store, "node-b")
        assert candidate is not None
        candidate["score"] = 0.12
        return [candidate]

    async def _fake_choose(anchor, candidates):
        assert anchor["node_id"] == "node-a"
        assert candidates[0]["node_id"] == "node-b"
        return ["node-b"], "Matched by test chooser"

    async def _fake_combine(nodes):
        assert {node["node_id"] for node in nodes} == {"node-a", "node-b"}
        return "Alice Combined", "Merged entity"

    monkeypatch.setattr(engine, "_query_candidates", _fake_query_candidates)
    monkeypatch.setattr(engine, "_choose_matches", _fake_choose)
    monkeypatch.setattr(engine, "_combine_entities", _fake_combine)
    monkeypatch.setattr(engine, "_rebuild_unique_node_index", _fake_rebuild_unique_node_index)
    monkeypatch.setattr(engine, "_refresh_unique_node_index_after_merge", _fake_refresh_unique_node_index_after_merge)

    asyncio.run(engine.start_entity_resolution(world_id, 25, False, True, "exact_then_ai"))

    status = engine.get_resolution_status(world_id)
    events = engine.drain_sse_events(world_id)
    reloaded = graph_store.GraphStore(world_id)

    assert status["status"] == "complete"
    assert status["resolution_mode"] == "exact_then_ai"
    assert status["resolved_entities"] == 3
    assert status["unresolved_entities"] == 0
    assert reloaded.get_node_count() == 2
    assert rebuild_calls == [["node-a", "node-b", "node-c"]]
    assert refreshed_merges == [("node-a", ["node-b"])]
    assert any(event.get("phase") == "chooser" for event in events)
    assert any(event.get("phase") == "combiner" for event in events)


def test_exact_then_ai_rebuilds_unique_index_after_exact_pass_before_candidate_search(tmp_path, monkeypatch):
    world_id = "world-exact-pass-index-refresh"
    _, store = _prepare_world(tmp_path, monkeypatch, world_id)
    store.graph.add_node("node-a", display_name="Alice", description="Primary", claims=[], source_chunks=[])
    store.graph.add_node("node-b", display_name="ALICE", description="Duplicate", claims=[], source_chunks=[])
    store.graph.add_node("node-c", display_name="Alicia", description="Possible duplicate", claims=[], source_chunks=[])
    store.graph.add_node("node-d", display_name="Bob", description="Other", claims=[], source_chunks=[])
    store.save()

    rebuild_calls: list[list[str]] = []
    fake_unique_node_store = object()

    async def _fake_rebuild_unique_node_index(_world_id, active_store, batch_size, cooldown_seconds, abort_event=None):
        rebuild_calls.append(sorted(active_store.graph.nodes()))
        assert batch_size == 32
        assert cooldown_seconds == 0.0
        return fake_unique_node_store

    def _fake_query_candidates(active_store, unique_node_vector_store, anchor_id, remaining_ids, _top_k):
        assert unique_node_vector_store is fake_unique_node_store
        assert sorted(active_store.graph.nodes()) == ["node-a", "node-c", "node-d"]
        assert anchor_id == "node-c"
        return []

    async def _fail_choose(*args, **kwargs):
        raise AssertionError("Chooser should not run when no candidates are returned.")

    async def _fail_combine(*args, **kwargs):
        raise AssertionError("Combiner should not run when no candidates are returned.")

    monkeypatch.setattr(engine, "_rebuild_unique_node_index", _fake_rebuild_unique_node_index)
    monkeypatch.setattr(engine, "_query_candidates", _fake_query_candidates)
    monkeypatch.setattr(engine, "_choose_matches", _fail_choose)
    monkeypatch.setattr(engine, "_combine_entities", _fail_combine)

    asyncio.run(engine.start_entity_resolution(world_id, 25, False, True, "exact_then_ai"))

    status = engine.get_resolution_status(world_id)
    reloaded = graph_store.GraphStore(world_id)

    assert status["status"] == "complete"
    assert reloaded.get_node_count() == 3
    assert rebuild_calls == [["node-a", "node-c", "node-d"]]


def test_legacy_metadata_without_resolution_mode_maps_safely(tmp_path, monkeypatch):
    world_id = "world-legacy-mode"
    meta_path, _ = _prepare_world(tmp_path, monkeypatch, world_id)
    meta_path.write_text(
        json.dumps(
            {
                "entity_resolution_status": "idle",
                "entity_resolution_phase": "waiting",
                "entity_resolution_exact_pass": False,
            }
        ),
        encoding="utf-8",
    )

    status = engine.get_resolution_status(world_id)

    assert status["resolution_mode"] == "ai_only"
    assert status["include_normalized_exact_pass"] is False


def test_entity_resolution_status_exposes_embedding_controls(tmp_path, monkeypatch):
    world_id = "world-embed-controls-status"
    meta_path, _ = _prepare_world(tmp_path, monkeypatch, world_id)
    meta_path.write_text(
        json.dumps(
            {
                "entity_resolution_status": "idle",
                "entity_resolution_phase": "waiting",
                "entity_resolution_embedding_batch_size": 7,
                "entity_resolution_embedding_cooldown_seconds": 1.5,
            }
        ),
        encoding="utf-8",
    )

    status = engine.get_resolution_status(world_id)

    assert status["embedding_batch_size"] == 7
    assert status["embedding_cooldown_seconds"] == 1.5


def test_entity_resolution_start_uses_custom_embedding_controls(tmp_path, monkeypatch):
    world_id = "world-custom-embed-controls"
    _, store = _prepare_world(tmp_path, monkeypatch, world_id)
    store.graph.add_node("node-a", display_name="Alice", description="Primary", claims=[], source_chunks=[])
    store.graph.add_node("node-b", display_name="ALICE", description="Duplicate", claims=[], source_chunks=[])
    store.save()

    rebuild_calls: list[tuple[list[str], int, float]] = []

    async def _fake_rebuild_unique_node_index(_world_id, active_store, batch_size, cooldown_seconds, abort_event=None):
        rebuild_calls.append((sorted(active_store.graph.nodes()), batch_size, cooldown_seconds))
        return object()

    monkeypatch.setattr(engine, "_rebuild_unique_node_index", _fake_rebuild_unique_node_index)

    asyncio.run(engine.start_entity_resolution(world_id, 50, False, True, "exact_only", 5, 1.25))

    status = engine.get_resolution_status(world_id)

    assert rebuild_calls == [(["node-a"], 5, 1.25)]
    assert status["embedding_batch_size"] == 5
    assert status["embedding_cooldown_seconds"] == 1.25


def test_upsert_unique_node_snapshots_obeys_cooldown_and_abort(monkeypatch):
    class _FakeVectorStore:
        def __init__(self):
            self.embed_calls: list[list[str]] = []
            self.upsert_calls: list[list[str]] = []

        def embed_texts(self, texts, api_key):
            self.embed_calls.append(list(texts))
            return [[0.1] for _ in texts]

        def upsert_documents_embeddings(self, document_ids, texts, metadatas, embeddings):
            self.upsert_calls.append(list(document_ids))

    vector_store = _FakeVectorStore()
    node_snapshots = [
        {"node_id": "node-a", "display_name": "Alice", "description": "", "normalized_name": "alice"},
        {"node_id": "node-b", "display_name": "Bob", "description": "", "normalized_name": "bob"},
        {"node_id": "node-c", "display_name": "Cara", "description": "", "normalized_name": "cara"},
    ]
    abort_event = engine.threading.Event()
    sleep_calls: list[float] = []

    async def _fake_sleep_with_abort(expected_event, seconds):
        sleep_calls.append(seconds)
        expected_event.set()
        raise asyncio.CancelledError()

    monkeypatch.setattr(engine, "_get_embedding_api_key", lambda: "test-key")
    monkeypatch.setattr(engine, "_sleep_with_abort", _fake_sleep_with_abort)

    try:
        asyncio.run(engine._upsert_unique_node_snapshots(vector_store, node_snapshots, 2, 0.5, abort_event))
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError("Expected the cooldown abort to cancel the batch loop.")

    assert len(vector_store.embed_calls) == 1
    assert len(vector_store.upsert_calls) == 1
    assert sleep_calls == [0.5]
