"""Entity-resolution endpoints for post-ingestion merge workflows."""

from __future__ import annotations

import asyncio
import json
import threading

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from core.config import world_meta_path
from core.entity_resolution_engine import (
    abort_entity_resolution,
    begin_entity_resolution_run,
    drain_sse_events,
    get_resolution_current,
    get_resolution_status,
    start_entity_resolution,
)

router = APIRouter()


class EntityResolutionStartRequest(BaseModel):
    top_k: int = 50
    review_mode: bool = False
    include_normalized_exact_pass: bool = True


def _load_meta(world_id: str) -> dict:
    path = world_meta_path(world_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="World not found")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _run_entity_resolution_in_thread(
    world_id: str,
    top_k: int,
    review_mode: bool,
    include_normalized_exact_pass: bool,
) -> None:
    asyncio.run(
        start_entity_resolution(
            world_id,
            top_k,
            review_mode,
            include_normalized_exact_pass,
        )
    )


@router.post("/{world_id}/entity-resolution/start")
async def entity_resolution_start(world_id: str, req: EntityResolutionStartRequest):
    meta = _load_meta(world_id)
    if meta.get("ingestion_status") == "in_progress":
        raise HTTPException(status_code=409, detail="Finish ingestion before resolving entities.")
    current_status = get_resolution_status(world_id)
    if current_status.get("status") == "in_progress":
        raise HTTPException(status_code=409, detail="Entity resolution is already in progress.")

    state = begin_entity_resolution_run(
        world_id,
        max(1, req.top_k),
        req.review_mode,
        req.include_normalized_exact_pass,
    )
    thread = threading.Thread(
        target=_run_entity_resolution_in_thread,
        args=(
            world_id,
            max(1, req.top_k),
            req.review_mode,
            req.include_normalized_exact_pass,
        ),
        daemon=True,
        name=f"entity-resolution-{world_id}",
    )
    thread.start()
    return {"status": "accepted", "world_id": world_id, "state": state}


@router.post("/{world_id}/entity-resolution/abort")
async def entity_resolution_abort(world_id: str):
    _load_meta(world_id)
    abort_entity_resolution(world_id)
    return {"ok": True}


@router.get("/{world_id}/entity-resolution/status")
async def entity_resolution_status(world_id: str):
    _load_meta(world_id)
    return get_resolution_status(world_id)


@router.get("/{world_id}/entity-resolution/current")
async def entity_resolution_current(world_id: str):
    _load_meta(world_id)
    return get_resolution_current(world_id)


@router.get("/{world_id}/entity-resolution/events")
async def entity_resolution_events(world_id: str):
    _load_meta(world_id)

    async def event_generator():
        while True:
            events = drain_sse_events(world_id)
            for event in events:
                yield f"data: {json.dumps(event)}\n\n"
                if event.get("event") in ("complete", "aborted", "error"):
                    return

            status = get_resolution_status(world_id)
            if status.get("status") in ("complete", "aborted", "error") and not events:
                yield f"data: {json.dumps({'event': status.get('status'), **status})}\n\n"
                return

            await asyncio.sleep(0.5)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
