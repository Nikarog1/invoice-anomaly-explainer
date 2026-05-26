import csv
from pathlib import Path
from unittest.mock import AsyncMock, patch
from uuid import uuid4, UUID

from sqlmodel import Session

from api.background import run_ingestion
from schemas.jobs import IngestionJob



async def test_run_ingestion_no_job_found_returns_empty(fake_session: Session) -> None:
    job_id = uuid4()
    output = await run_ingestion(job_id, file_paths=[])
    
    assert output is None
    

async def test_run_ingestion_happy_path(tmp_path: Path, fake_session: Session) -> None:
    job = IngestionJob(file_results=[])
    job_id = job.job_id
    fake_session.add(job)
    fake_session.commit()
    
    data = [
        ["invoice_number", "supplier_name", "buyer_name"],
        ["012345", "Company1", "SuperCompany"],
    ]
    path = tmp_path / "data.csv"
    with open(path, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data)
    
    mock_service = AsyncMock()
    mock_service.run.return_value = uuid4()
    
    with patch(
        "api.background.IngestionService",
        return_value=mock_service,
    ):
        await run_ingestion(job_id, file_paths=[path])
        
    assert not path.is_file()
    
    upd_job = fake_session.get(IngestionJob, job_id)
    assert upd_job
    assert upd_job.status == "succeeded"
    
    file = upd_job.file_results[0]
    assert file
    assert file["filename"] == path.name
    assert file["status"] == "succeeded"
    assert isinstance(file["invoice_id"], str)
    assert file["error_message"] is None