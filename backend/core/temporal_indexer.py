"""Stamps [B{n}:C{i}] prefix on every chunk for temporal origin tracking."""

from __future__ import annotations

from pydantic import BaseModel


class TemporalChunk(BaseModel):
    """A chunk with its [B:C] prefix and metadata."""
    prefixed_text: str
    raw_text: str
    primary_text: str
    overlap_text: str
    book_number: int
    chunk_index: int
    source_id: str
    world_id: str
    char_start: int
    char_end: int
    display_label: str  # e.g. "Book 1 › Chunk 42"


def stamp_chunks(
    chunks: list[dict],   # each has: text, char_start, char_end, index
    book_number: int,
    source_id: str,
    world_id: str,
) -> list[TemporalChunk]:
    """Prepend [B{n}:C{i}] to each chunk and return TemporalChunk objects."""
    result = []
    for chunk in chunks:
        idx = chunk["index"]
        prefix = f"[B{book_number}:C{idx}] "
        raw_text = str(chunk.get("text") or "")
        primary_text = str(chunk.get("primary_text") or raw_text)
        overlap_text = str(chunk.get("overlap_text") or "")
        result.append(TemporalChunk(
            prefixed_text=prefix + raw_text,
            raw_text=raw_text,
            primary_text=primary_text,
            overlap_text=overlap_text,
            book_number=book_number,
            chunk_index=idx,
            source_id=source_id,
            world_id=world_id,
            char_start=chunk["char_start"],
            char_end=chunk["char_end"],
            display_label=f"Book {book_number} › Chunk {idx}",
        ))
    return result
