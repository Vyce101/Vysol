"""Recursive text chunker with configurable split hierarchy."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ChunkMeta:
    """Metadata for a single chunk."""
    text: str
    char_start: int
    char_end: int
    index: int


class RecursiveChunker:
    """Split text into overlapping chunks using a hierarchical separator strategy."""

    SEPARATORS = ["\n\n", "\n", ". ", " ", ""]  # hierarchy: paragraph → line → sentence → word → hard cut

    def __init__(self, chunk_size: int = 4000, overlap: int = 150):
        self.chunk_size = chunk_size
        self.overlap = overlap

    def chunk(self, text: str) -> list[ChunkMeta]:
        """Split text into chunks and return metadata."""
        if not text.strip():
            return []

        raw_chunks = self._split_recursive(text, self.SEPARATORS)

        # Apply overlap and build metadata
        results: list[ChunkMeta] = []
        char_cursor = 0

        for i, chunk_text in enumerate(raw_chunks):
            # Find actual position in original text
            start = text.find(chunk_text[:50], max(0, char_cursor - self.overlap))
            if start == -1:
                start = char_cursor
            end = start + len(chunk_text)
            char_cursor = end

            results.append(ChunkMeta(
                text=chunk_text.strip(),
                char_start=start,
                char_end=end,
                index=i,
            ))

        # Apply overlap: prepend tail of previous chunk to next
        if self.overlap > 0 and len(results) > 1:
            overlapped: list[ChunkMeta] = [results[0]]
            for i in range(1, len(results)):
                prev_text = results[i - 1].text
                overlap_text = prev_text[-self.overlap:] if len(prev_text) > self.overlap else prev_text
                # Find a clean break point in the overlap
                space_idx = overlap_text.find(" ")
                if space_idx > 0:
                    overlap_text = overlap_text[space_idx + 1:]
                new_text = overlap_text + " " + results[i].text
                overlapped.append(ChunkMeta(
                    text=new_text.strip(),
                    char_start=results[i].char_start - len(overlap_text),
                    char_end=results[i].char_end,
                    index=i,
                ))
            results = overlapped

        return results

    def _split_recursive(self, text: str, separators: list[str]) -> list[str]:
        """Recursively split text using the separator hierarchy."""
        if not text.strip():
            return []

        if len(text) <= self.chunk_size:
            return [text.strip()]

        if not separators:
            # Hard cut — last resort
            chunks = []
            for i in range(0, len(text), self.chunk_size):
                chunk = text[i:i + self.chunk_size].strip()
                if chunk:
                    chunks.append(chunk)
            return chunks

        sep = separators[0]
        remaining_seps = separators[1:]

        if sep == "":
            return self._split_recursive(text, remaining_seps)

        parts = text.split(sep)
        if len(parts) <= 1:
            # Separator not found, try next level
            return self._split_recursive(text, remaining_seps)

        chunks: list[str] = []
        current = ""

        for part in parts:
            candidate = (current + sep + part) if current else part
            if len(candidate) <= self.chunk_size:
                current = candidate
            else:
                if current.strip():
                    chunks.append(current.strip())
                # If this single part exceeds chunk_size, split it further
                if len(part) > self.chunk_size:
                    sub_chunks = self._split_recursive(part, remaining_seps)
                    chunks.extend(sub_chunks)
                    current = ""
                else:
                    current = part

        if current.strip():
            chunks.append(current.strip())

        return chunks
