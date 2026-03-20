# Features

## Graph Provenance

Every edge extracted into the graph is temporally indexed to its source document and chunk as BN:CN with the N' being numbers.

Each document completed during ingestion is assigned a book number, starting at 1 and incrementing in input order. Chunk numbering resets on every document completion.

This means every relationship in the graph carries:

- which book it came from
- which chunk within that book it came from

What this gives you:

- full source traceability for any extracted relationship
- the ability to see when a relationship was first established across a multi-document ingest
- a foundation for spotting contradictions between sources
- auditability for workflows where knowing the origin of extracted information matters
- an ai model with better temporal understanding

## How Entity Resolution Works

Entity resolution happens in two stages.

First:

- the app does an exact normalized-name pass
- this can auto-resolve obvious duplicates without spending chooser model calls

Then:

- the app has a master list of all entities and chooses Top K most similiar entities via vector search
- the chooser model decides which candidates are actually the same entity as the anchor
- the combiner model merges the chosen group into one canonical result
- all entities that were merged are removed from the master list
- repeat

## Context X-Ray

Every chat message saves a full record of exactly what was sent to the model.

Context X-Ray lets you open any message and see:

- the system prompt
- entry nodes selected for graph expansion
- all nodes and edges included in context
- RAG chunks retrieved
- chat history sent

Each record has two views:

- Byte View: the exact raw content sent, nothing hidden or reformatted
- Clean View: a readable formatted version of the same data

X-Ray records are saved per message so you can go back and inspect any point
in a conversation, not just the most recent one.
