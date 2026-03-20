import json

from core import chat_store


def test_chat_store_recovers_orphan_backup(tmp_path, monkeypatch):
    world_id = "w1"
    chats_dir = tmp_path / world_id / "chats"
    chats_dir.mkdir(parents=True, exist_ok=True)

    bak_path = chats_dir / "chat-abc.bak1"
    payload = {
        "id": "chat-abc",
        "title": "Recovered",
        "created_at": "2026-03-20T00:00:00+00:00",
        "updated_at": "2026-03-20T01:00:00+00:00",
        "messages": [{"role": "user", "content": "hello"}],
    }
    with open(bak_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)

    monkeypatch.setattr(chat_store, "world_dir", lambda wid: tmp_path / wid)
    store = chat_store.ChatStore(world_id)

    recovered = store.get_chat("chat-abc")
    assert recovered is not None
    assert recovered["title"] == "Recovered"
    assert len(recovered["messages"]) == 1
    assert (chats_dir / "chat-abc.json").exists()
