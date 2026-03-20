import sys
import os
import threading
import time
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from core.ingestion_engine import start_ingestion, abort_ingestion, _abort_events, _save_meta
from core.config import world_dir, world_meta_path, world_sources_dir

def setup_fake_world(world_id):
    w_dir = world_dir(world_id)
    w_dir.mkdir(parents=True, exist_ok=True)
    
    s_dir = world_sources_dir(world_id)
    s_dir.mkdir(parents=True, exist_ok=True)
    
    source_file = s_dir / "test.txt"
    source_file.write_text("This is a test chunk that will take some time to process.", encoding="utf-8")
    
    meta = {
        "world_id": world_id,
        "name": "Test Restart World",
        "ingestion_status": "pending",
        "sources": [
            {
                "source_id": "src1",
                "original_filename": "test.txt",
                "vault_filename": "test.txt",
                "book_number": 1,
                "display_name": "Test Source",
                "status": "pending",
                "chunk_count": 0,
                "ingested_at": None
            }
        ]
    }
    _save_meta(world_id, meta)

class MockAgent:
    def run(self, *args, **kwargs):
        print(f"[{threading.current_thread().name}] Agent starting...")
        time.sleep(2)
        print(f"[{threading.current_thread().name}] Agent finished.")
        # Return a mock output with nodes/edges
        output = MagicMock()
        output.nodes = []
        output.edges = []
        return output, {"usage": 0}

@patch("core.ingestion_engine.EntityArchitectAgent", return_value=MockAgent())
@patch("core.ingestion_engine.RelationshipArchitectAgent", return_value=MockAgent())
@patch("core.ingestion_engine.VectorStore")
@patch("core.ingestion_engine.GraphStore")
def test_simulated_restart(mock_gs_class, mock_vs_class, mock_ra, mock_ea):
    world_id = "test_restart_bug"
    setup_fake_world(world_id)
    
    # Mock GraphStore instance
    mock_gs = mock_gs_class.return_value
    
    print("\n--- Starting Ingestion 1 ---")
    t1 = threading.Thread(target=start_ingestion, args=(world_id, False), name="Task-1")
    t1.start()
    
    time.sleep(0.5) # Wait for it to start
    
    print("\n--- Aborting Ingestion 1 ---")
    abort_ingestion(world_id)
    
    print("\n--- Starting Ingestion 2 (Start Over) ---")
    # This should clear the SSE queue and set a new abort event
    t2 = threading.Thread(target=start_ingestion, args=(world_id, False), name="Task-2")
    t2.start()
    
    t1.join()
    t2.join()
    
    print("\n--- Test Complete ---")
    # Verify that GS was only saved if it was the current task
    # In this mock setup, Task-1 should have been aborted/invalidated before it reached save()
    # Task-2 should have reached save().
    
    # We can check how many times save() was called. 
    # If the bug was fixed, only Task-2 (or the one that wasn't aborted) should call save().
    # Actually, Task-1 will break out of the loop.
    
    print(f"GraphStore.save() call count: {mock_gs.save.call_count}")

if __name__ == "__main__":
    test_simulated_restart()
