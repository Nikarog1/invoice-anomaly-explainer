from datetime import datetime, timezone
from uuid import uuid4

from api.models.ingestion_jobs import FileResult
from schemas.jobs import IngestionJob



def test_get_ingestion_job_returns_dto_when_job_exists(client, fake_session):
    job = IngestionJob(
        job_id=uuid4(),
        status="succeeded",
        created_at=datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc),
        started_at=datetime(2026, 4, 1, 12, 0, 1, tzinfo=timezone.utc),
        finished_at=datetime(2026, 4, 1, 12, 0, 10, tzinfo=timezone.utc),
        file_results=[
            FileResult(
                filename="data.csv",
                status="succeeded",
                invoice_id=uuid4(),
                error_message=None,
            ).model_dump(mode="json")
        ],
        error_message=None,

    )

    fake_session.add(job)
    fake_session.commit()

    response = client.get(f"/ingestion-jobs/{job.job_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "succeeded"
    assert body["created_at"] == "2026-04-01T12:00:00"
    assert len(body["file_results"]) == 1
    assert body["file_results"][0]["filename"] == "data.csv"


def test_get_ingestion_job_returns_404_when_job_missing(client):
    missing_id = uuid4()

    response = client.get(f"/ingestion-jobs/{missing_id}")

    assert response.status_code == 404
    assert response.json()["detail"] == f"Job {missing_id} not found"