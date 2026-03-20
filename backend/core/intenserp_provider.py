"""IntenseRP Next chat provider — streams via OpenAI-compatible HTTP API."""

from __future__ import annotations

import json
import logging
from typing import Generator

import httpx

from .config import load_settings

logger = logging.getLogger(__name__)


def stream_intenserp_chat(
    messages_payload: list[dict],
    nodes_used: list | None = None,
    settings: dict | None = None,
) -> Generator[str, None, None]:
    """
    Stream a chat response from IntenseRP Next as SSE events.

    Yields: 'data: {"token": "..."}\\n\\n' per token
    Final:  'data: {"event": "done", "nodes_used": [...]}\\n\\n'
    """
    if settings is None:
        settings = load_settings()

    base_url = settings.get("intenserp_base_url", "http://127.0.0.1:7777/v1").rstrip("/")
    model_id = settings.get("intenserp_model_id", "glm-chat")
    nodes_used = nodes_used or []

    payload = {
        "model": model_id,
        "messages": messages_payload,
        "stream": True,
    }

    try:
        with httpx.stream(
            "POST",
            f"{base_url}/chat/completions",
            json=payload,
            timeout=120.0,
            headers={"Content-Type": "application/json"},
        ) as response:
            response.raise_for_status()

            for line in response.iter_lines():
                if not line:
                    continue
                if line.startswith("data:"):
                    data_str = line[5:].strip()
                    if data_str == "[DONE]":
                        break
                    try:
                        data = json.loads(data_str)
                        choices = data.get("choices", [])
                        if choices:
                            delta = choices[0].get("delta", {})
                            content = delta.get("content")
                            if content:
                                yield f"data: {json.dumps({'token': content})}\n\n"
                    except (json.JSONDecodeError, KeyError):
                        continue

    except httpx.ConnectError:
        yield f"data: {json.dumps({'event': 'error', 'message': 'Cannot connect to IntenseRP Next. Is it running at ' + base_url + '?'})}\n\n"
        return
    except httpx.HTTPStatusError as e:
        yield f"data: {json.dumps({'event': 'error', 'message': f'IntenseRP Next returned HTTP {e.response.status_code}'})}\n\n"
        return
    except Exception as e:
        logger.exception("IntenseRP stream error")
        yield f"data: {json.dumps({'event': 'error', 'message': str(e)})}\n\n"
        return

    # Final done event with graph nodes.
    yield f"data: {json.dumps({'event': 'done', 'nodes_used': nodes_used})}\n\n"
