import pytest
from fastapi.testclient import TestClient

from api.main import app
from data.sqlite import get_session



@pytest.fixture
def client(fake_session):
    """TestClient with get_session overridden to use the in-memory fake_session."""
    app.dependency_overrides[get_session] = lambda: fake_session
    yield TestClient(app)
    app.dependency_overrides.clear()