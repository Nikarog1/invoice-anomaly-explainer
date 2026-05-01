import os
os.environ["SQLITE_URL"] = "sqlite:///:memory:"

import pytest

import chromadb
from chromadb.config import Settings

from sqlmodel import Session, SQLModel

from data.sqlite import engine, create_db_and_tables
from tests.data.test_vector_store import FakeEmbeddingFunction



@pytest.fixture
def fake_session():
    create_db_and_tables()
    with Session(engine) as session:    
        yield session
    SQLModel.metadata.drop_all(engine)
    

@pytest.fixture
def fake_chroma_client():
    client = chromadb.Client(Settings(allow_reset=True))
    yield client
    client.reset()


@pytest.fixture
def fake_collection(fake_chroma_client):
    return fake_chroma_client.get_or_create_collection(
        name="test_contracts",
        embedding_function=FakeEmbeddingFunction(),  # type: ignore
        configuration={"hnsw": {"space": "cosine"}},
    )