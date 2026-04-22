import os
os.environ["SQLITE_URL"] = "sqlite:///:memory:"

import pytest
from sqlmodel import Session
from data.sqlite import engine, create_db_and_tables



@pytest.fixture
def fake_session():
    create_db_and_tables()
    with Session(engine) as session:    
        yield session