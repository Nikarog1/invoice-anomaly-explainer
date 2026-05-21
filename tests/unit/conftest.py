import os
os.environ["SQLITE_URL"] = "sqlite:///:memory:"

import pytest

import chromadb
from chromadb.config import Settings

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

import data.sqlite as sqlite_module
from tests.unit.data.test_vector_store import FakeEmbeddingFunction



_test_engine = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
sqlite_module.engine = _test_engine

@pytest.fixture
def fake_session():
    SQLModel.metadata.create_all(_test_engine)
    with Session(_test_engine) as session:
        yield session
    SQLModel.metadata.drop_all(_test_engine)
    

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

@pytest.fixture(autouse=True)
def block_llm(monkeypatch):
    """Prevent unit tests from accidentally hitting Ollama."""
    async def _skip(*args, **kwargs):
        return None
    
    monkeypatch.setattr("data.llm_client.call_local_llm", _skip)
    monkeypatch.setattr("ingestion.normalizer.call_local_llm", _skip)
    monkeypatch.setattr("pipeline.agents.agent_explanation.call_local_llm", _skip)
    monkeypatch.setattr("pipeline.nodes.contract_matching.call_local_llm", _skip)
    monkeypatch.setattr("pipeline.nodes.contract_matching._llm_match", _skip)