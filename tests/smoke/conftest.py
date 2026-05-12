import os

os.environ["SQLITE_URL"] = "sqlite:///./data/smoke_test.db"
os.environ["CHECKPOINT_PATH"] = "./data/smoke_checkpoints.db"
os.environ["CHROMADB_PATH"] = "./data/smoke_chroma"

from pathlib import Path
import pytest
import shutil
from sqlmodel import SQLModel

from data.sqlite import engine
from data.vector_store import client as chroma_client


@pytest.fixture(autouse=True)
def clean_smoke_state():
    # sql
    SQLModel.metadata.drop_all(engine)
    SQLModel.metadata.create_all(engine)
    
    # chroma
    chroma_client.reset()
    
    # checkpoints
    checkpoint_path = Path(os.environ["CHECKPOINT_PATH"])
    if checkpoint_path.exists():
        checkpoint_path.unlink()
    
    yield