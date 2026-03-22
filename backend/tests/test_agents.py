import asyncio
from types import SimpleNamespace

from core import agents


def test_call_agent_retries_timeout_on_next_key(monkeypatch):
    class DummyKM:
        def __init__(self):
            self.calls = 0
            self.reported: list[tuple[int, str]] = []

        async def await_active_key(self, *, jitter_seconds: float = 0.25):
            keys = [("k1", 0), ("k2", 1)]
            key = keys[min(self.calls, len(keys) - 1)]
            self.calls += 1
            return key

        def report_error(self, key_index: int, error_type: str) -> None:
            self.reported.append((key_index, error_type))

    class DummyAioModels:
        def __init__(self, api_key: str):
            self.api_key = api_key

        async def generate_content(self, *, model, contents, config):
            if self.api_key == "k1":
                raise RuntimeError("request timed out")
            return SimpleNamespace(
                candidates=[SimpleNamespace(content=object())],
                text='{"nodes": [], "edges": []}',
                usage_metadata=SimpleNamespace(prompt_token_count=11, candidates_token_count=7),
                prompt_feedback=None,
            )

    class DummyClient:
        def __init__(self, api_key: str):
            self.aio = SimpleNamespace(models=DummyAioModels(api_key))

    dummy_km = DummyKM()

    monkeypatch.setattr(agents, "get_key_manager", lambda: dummy_km)
    monkeypatch.setattr(agents, "load_settings", lambda: {"disable_safety_filters": False})
    monkeypatch.setattr(agents, "load_prompt", lambda key: "SYSTEM")
    monkeypatch.setattr(agents.genai, "Client", DummyClient)

    parsed, usage = asyncio.run(
        agents._call_agent(
            prompt_key="graph_architect_prompt",
            user_content="chunk text",
            model_name="gemini-test",
            temperature=0.1,
        )
    )

    assert parsed == {"nodes": [], "edges": []}
    assert usage == {"input_tokens": 11, "output_tokens": 7}
    assert dummy_km.reported == [(0, "timeout")]
