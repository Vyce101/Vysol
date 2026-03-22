import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .config import world_dir


class ChatVersionConflictError(RuntimeError):
    pass


class ChatStore:
    def __init__(self, world_id: str):
        self.world_id = world_id
        self.chats_dir = world_dir(world_id) / "chats"
        self.chats_dir.mkdir(parents=True, exist_ok=True)
        self._recover_temp_files()

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _get_path(self, chat_id: str) -> Path:
        return self.chats_dir / f"{chat_id}.json"

    def _normalize_message(self, payload: dict) -> dict:
        if not isinstance(payload, dict):
            payload = {}

        normalized = dict(payload)
        normalized["role"] = payload.get("role", "model")
        normalized["content"] = payload.get("content", "")
        normalized["message_id"] = payload.get("message_id") or payload.get("messageId") or str(uuid.uuid4())
        normalized["status"] = payload.get("status") or "complete"
        return normalized

    def _normalize_chat(self, payload: dict | None, chat_id: str, *, now: str | None = None) -> dict:
        data = dict(payload or {})
        now_value = now or self._now_iso()

        raw_messages = data.get("messages", [])
        if not isinstance(raw_messages, list):
            raw_messages = []

        version = data.get("version", 0)
        if not isinstance(version, int) or version < 0:
            version = 0

        return {
            **data,
            "id": data.get("id", chat_id),
            "title": data.get("title", "New Chat"),
            "created_at": data.get("created_at", now_value),
            "updated_at": data.get("updated_at", now_value),
            "version": version,
            "messages": [self._normalize_message(msg) for msg in raw_messages],
        }

    def _read_json_file(self, path: Path) -> dict | None:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except Exception:
            return None
        return None

    def _parse_updated_at(self, data: dict | None, fallback: datetime) -> datetime:
        if not data:
            return fallback
        raw = data.get("updated_at")
        if not isinstance(raw, str):
            return fallback
        try:
            value = datetime.fromisoformat(raw)
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        except Exception:
            return fallback

    def _next_backup_path(self, canonical: Path) -> Path:
        idx = 1
        while True:
            candidate = canonical.with_suffix(f".bak{idx}")
            if not candidate.exists():
                return candidate
            idx += 1

    def _recover_temp_files(self) -> None:
        """
        Recover interrupted atomic writes:
        - If only *.tmp.json exists, promote it to *.json.
        - If both exist, keep the newest copy and preserve the replaced file as .bakN.
        - If only orphan *.bakN files exist for a chat id, restore newest backup to *.json.
        """
        for tmp in self.chats_dir.glob("*.tmp.json"):
            canonical = tmp.with_name(tmp.name.replace(".tmp.json", ".json"))
            try:
                if not canonical.exists():
                    os.replace(str(tmp), str(canonical))
                    continue

                tmp_data = self._read_json_file(tmp)
                canonical_data = self._read_json_file(canonical)

                tmp_mtime = datetime.fromtimestamp(tmp.stat().st_mtime, tz=timezone.utc)
                canonical_mtime = datetime.fromtimestamp(canonical.stat().st_mtime, tz=timezone.utc)
                tmp_updated = self._parse_updated_at(tmp_data, tmp_mtime)
                canonical_updated = self._parse_updated_at(canonical_data, canonical_mtime)

                if tmp_updated > canonical_updated:
                    backup = self._next_backup_path(canonical)
                    os.replace(str(canonical), str(backup))
                    os.replace(str(tmp), str(canonical))
                else:
                    os.remove(tmp)
            except OSError:
                # Best-effort recovery only.
                pass

        # Recover orphaned backups (no canonical .json present).
        backups_by_canonical: dict[Path, list[Path]] = {}
        for bak in self.chats_dir.glob("*.bak*"):
            base = bak.name.split(".bak", 1)[0]
            if not base:
                continue
            canonical = self.chats_dir / f"{base}.json"
            backups_by_canonical.setdefault(canonical, []).append(bak)

        for canonical, backups in backups_by_canonical.items():
            if canonical.exists() or not backups:
                continue
            try:
                best_path = None
                best_updated = None
                for candidate in backups:
                    data = self._read_json_file(candidate)
                    c_mtime = datetime.fromtimestamp(candidate.stat().st_mtime, tz=timezone.utc)
                    c_updated = self._parse_updated_at(data, c_mtime)
                    if best_updated is None or c_updated > best_updated:
                        best_updated = c_updated
                        best_path = candidate

                if not best_path:
                    continue

                best_data = self._read_json_file(best_path)
                if not best_data:
                    continue

                if "id" not in best_data:
                    best_data["id"] = canonical.stem
                if "messages" not in best_data or not isinstance(best_data["messages"], list):
                    best_data["messages"] = []
                if "created_at" not in best_data:
                    best_data["created_at"] = self._now_iso()
                if "updated_at" not in best_data:
                    best_data["updated_at"] = self._now_iso()
                if "version" not in best_data:
                    best_data["version"] = 0

                tmp = canonical.with_suffix(".tmp.json")
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(best_data, f, indent=2)
                os.replace(str(tmp), str(canonical))
            except OSError:
                pass

    def list_chats(self) -> list[dict]:
        chats = []
        for file in self.chats_dir.glob("*.json"):
            if file.name.endswith(".tmp.json") or ".bak" in file.name:
                continue
            try:
                with open(file, "r", encoding="utf-8") as f:
                    data = self._normalize_chat(json.load(f), file.stem)
                    chats.append({
                        "id": data.get("id", file.stem),
                        "title": data.get("title", "New Chat"),
                        "created_at": data.get("created_at"),
                        "updated_at": data.get("updated_at"),
                        "version": data.get("version", 0),
                    })
            except Exception:
                pass
        # Sort by updated_at descending
        chats.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
        # Deduplicate by id to protect UI keys against corrupted/duplicated files.
        deduped = []
        seen_ids = set()
        for chat in chats:
            chat_id = chat.get("id")
            if chat_id in seen_ids:
                continue
            seen_ids.add(chat_id)
            deduped.append(chat)
        return deduped

    def create_chat(self, title: str = "New Chat") -> dict:
        chat_id = str(uuid.uuid4())
        now = self._now_iso()
        data = {
            "id": chat_id,
            "title": title,
            "created_at": now,
            "updated_at": now,
            "version": 0,
            "messages": [],
        }
        return self.save_chat(chat_id, data)

    def get_chat(self, chat_id: str) -> dict | None:
        path = self._get_path(chat_id)
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return self._normalize_chat(json.load(f), chat_id)
        except Exception:
            return None

    def _write_chat(self, path: Path, data: dict) -> dict:
        tmp = path.with_suffix(".tmp.json")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(str(tmp), str(path))
        return data

    def save_chat(self, chat_id: str, data: dict, *, expected_version: int | None = None) -> dict:
        existing = self.get_chat(chat_id)
        current_version = existing.get("version", 0) if existing else 0
        if expected_version is not None and current_version != expected_version:
            raise ChatVersionConflictError(
                f"Chat {chat_id} has version {current_version}, expected {expected_version}."
            )

        now = self._now_iso()
        normalized = self._normalize_chat(data, chat_id, now=now)
        normalized["version"] = current_version + 1
        normalized["updated_at"] = now

        path = self._get_path(chat_id)
        return self._write_chat(path, normalized)

    def rename_chat(self, chat_id: str, title: str, *, expected_version: int | None = None) -> dict | None:
        existing = self.get_chat(chat_id)
        if not existing:
            return None

        normalized_title = str(title).strip()
        if not normalized_title:
            raise ValueError("Chat title cannot be empty.")

        current_version = existing.get("version", 0)
        if expected_version is not None and current_version != expected_version:
            raise ChatVersionConflictError(
                f"Chat {chat_id} has version {current_version}, expected {expected_version}."
            )

        preserved_updated_at = existing.get("updated_at", self._now_iso())
        renamed = self._normalize_chat(existing, chat_id, now=preserved_updated_at)
        renamed["title"] = normalized_title
        renamed["version"] = current_version + 1
        renamed["updated_at"] = preserved_updated_at

        return self._write_chat(self._get_path(chat_id), renamed)

    def delete_chat(self, chat_id: str) -> bool:
        path = self._get_path(chat_id)
        if path.exists():
            try:
                os.remove(path)
                return True
            except OSError:
                pass
        return False
