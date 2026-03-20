import asyncio
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from typing import List
from models.api_models import IngestRequest
from core.ingestion_engine import IngestionEngine

async def run_pipeline():
    world_id = "test_world"
    source_id = "test_source"
    
    # We will just patch the chunker to return some test data
    engine = IngestionEngine(world_id)
    
    # Let's bypass the actual source processing and just test the agent pipeline
    chunk_text = "Rudeus Greyrat trained under Roxy Migurdia. Roxy is a Water King tier adept."
    
    print("Testing pipeline with claims ENABLED")
    # Setting mock settings directly or testing the agents
    from core.config import load_settings
    settings = load_settings()
    settings["enable_claims"] = True
    settings["extract_entity_types"] = True
    settings["entity_architect_prompt"] = "You are an AI extracting entities. Output strictly JSON: {\"nodes\": [{\"node_id\": \"rudeus_greyrat\", \"display_name\": \"Rudeus Greyrat\", \"entity_type\": \"Person\", \"description\": \"A young pupil\"}, {\"node_id\": \"roxy_migurdia\", \"display_name\": \"Roxy Migurdia\", \"entity_type\": \"Person\", \"description\": \"Water King tier adept\"}]}"
    settings["relationship_architect_prompt"] = "You are an AI extracting relationships. Output strictly JSON: {\"edges\": [{\"source_node_id\": \"rudeus_greyrat\", \"target_node_id\": \"roxy_migurdia\", \"label\": \"trained under\", \"description\": \"Rudeus trained under Roxy\", \"strength\": 8}]}"
    settings["claim_architect_prompt"] = "You are an AI extracting claims. Output strictly JSON: {\"claims\": [{\"node_id\": \"roxy_migurdia\", \"text\": \"Roxy is a Water King tier adept\", \"source_book\": 1, \"source_chunk\": 1, \"sequence_id\": 1}]}"
    settings["scribe_prompt"] = "You are an AI merging data. Output strictly JSON: {\"merged_nodes\": [{\"node_id\": \"rudeus_greyrat\", \"display_name\": \"Rudeus Greyrat\", \"entity_type\": \"Person\", \"description\": \"A young pupil\"}, {\"node_id\": \"roxy_migurdia\", \"display_name\": \"Roxy Migurdia\", \"entity_type\": \"Person\", \"description\": \"Water King tier adept\"}], \"merged_edges\": [{\"source_node_id\": \"rudeus_greyrat\", \"target_node_id\": \"roxy_migurdia\", \"label\": \"trained under\", \"description\": \"Rudeus trained under Roxy\", \"strength\": 8}], \"merged_claims\": [{\"node_id\": \"roxy_migurdia\", \"text\": \"Roxy is a Water King tier adept\", \"source_book\": 1, \"source_chunk\": 1, \"sequence_id\": 1}]}"

    # Manually run agents
    nodes = await engine._run_entity_architect(chunk_text, settings, source_id, 1, 1, 1)
    print("Entities:", nodes)
    
    edges = await engine._run_relationship_architect(chunk_text, nodes, settings, source_id, 1, 1, 1)
    print("Relationships:", edges)
    
    claims = await engine._run_claim_architect(chunk_text, settings, source_id, 1, 1, 1)
    print("Claims:", claims)
    
    merged_nodes, merged_edges, merged_claims = await engine._run_scribe(
        chunk_text, nodes, edges, claims, settings, source_id, 1, 1, 1
    )
    
    print("Merged nodes:", merged_nodes)
    print("Merged edges:", merged_edges)
    print("Merged claims:", merged_claims)

if __name__ == "__main__":
    asyncio.run(run_pipeline())
