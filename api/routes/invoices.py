import asyncio
import json
from pathlib import Path
from uuid import UUID

from fastapi import Depends, File, APIRouter,  UploadFile
from sqlmodel import Session

from api.background import run_ingestion
from api.models.ingestion_jobs import IngestionJobCreated
from api.models.invoices import InvoiceDTO, InvoiceLineItemDTO
from api.models.reports import AnomalyFlagDTO, AnomalyReportDTO, ReportResponse
from config.settings import settings
from core.exceptions import InvalidCSVError, InvoiceNotFoundError
from data.sqlite import (
    get_session, invoice_exists, load_anomaly_flags, load_invoice_from_sql, load_latest_analysis_job,
)
from ingestion.csv_parser import CSVParser
from schemas.anomaly import AnomalyReport
from schemas.jobs import IngestionJob



STATUS_MAP = {
    "queued": "analyzing",
    "running": "analyzing",
    "succeeded": "ready",
    "failed": "failed",
}

router = APIRouter(prefix="/invoices", tags=["invoices"])

@router.get("/{invoice_id}")
async def get_invoice(invoice_id: UUID, session: Session = Depends(get_session)) -> InvoiceDTO:
    """Fetch invoice header and line items by id."""
    invoice, invoice_line_items = load_invoice_from_sql(session, invoice_id)
    return InvoiceDTO(
        invoice_id=invoice_id,
        invoice_number=invoice.invoice_number,
        supplier_name=invoice.supplier_name,
        buyer_name=invoice.buyer_name,
        issue_date=invoice.issue_date,
        due_date=invoice.due_date,
        total_amount=invoice.total_amount,
        currency=invoice.currency,
        payment_details=invoice.payment_details,
        metadata=invoice.invoice_metadata,
        line_items=[
            InvoiceLineItemDTO(
                description=line.description,
                quantity=line.quantity,
                unit_price=line.unit_price,
                amount_net=line.amount_net,
                amount_gross=line.amount_gross,
                vat_rate=line.vat_rate,
                notes=line.notes,
            )
            for line in invoice_line_items
        ]
    )


@router.get("/{invoice_id}/report")
async def get_anomaly_report(invoice_id: UUID, session: Session = Depends(get_session)) -> ReportResponse:
    """Fetch anomaly report by invoice id."""
    if not invoice_exists(session, invoice_id):
        raise InvoiceNotFoundError(invoice_id)

    latest_job = load_latest_analysis_job(session, invoice_id, None)
    if latest_job is None:
        return ReportResponse(status="not_analyzed", report=None, error_message=None)

    status = STATUS_MAP[latest_job.status]
    error_message = latest_job.error_message if latest_job.status == "failed" else None

    report_dto: AnomalyReportDTO | None = None

    latest_success_job = load_latest_analysis_job(session, invoice_id, "succeeded")
    if latest_success_job and latest_success_job.anomaly_report_id:
        report = session.get(AnomalyReport, latest_success_job.anomaly_report_id)
        if report is None:
            raise RuntimeError(
                f"Integrity violation: succeeded job {latest_success_job.job_id} "
                f"references missing report {latest_success_job.anomaly_report_id}"
            )

        flags = load_anomaly_flags(session, report.anomaly_report_id)
        report_dto = AnomalyReportDTO(
            anomaly_report_id=report.anomaly_report_id,
            invoice_id=invoice_id,
            anomalies_count=report.anomalies_count,
            agent_explanation=report.agent_explanation, # type: ignore[arg-type]
            explanation_date=report.explanation_date, # type: ignore[arg-type]
            flags=[
                AnomalyFlagDTO(
                    name=flag.anomaly_name,
                    severity=flag.anomaly_severity,
                    source=flag.anomaly_source,
                    deviation=flag.anomaly_deviation,
                    notes=json.loads(flag.anomaly_notes) if flag.anomaly_notes else None,
                )
                for flag in flags
            ],
        )

    return ReportResponse(status=status, report=report_dto, error_message=error_message) # type: ignore[arg-type]


@router.post("", status_code=202)
async def upload_file(files: list[UploadFile]) -> IngestionJobCreated:
    """Some doc string"""
    
    job = IngestionJob(file_results=[]) 
    file_dir = Path(settings.csv_dir / str(job.job_id))
    file_dir.mkdir(parents=True, exist_ok=True)
    paths = []

    for i, file in enumerate(files):
        if not file or not file.filename:
            raise InvalidCSVError("None")
        
        bytes = await file.read()
        
        if not file.filename.lower().endswith(".csv"):
            raise InvalidCSVError(file.filename)
        
        if not CSVParser.is_csv(bytes):
            raise InvalidCSVError(file.filename)
        
        csv_name = f"{i}_{file.filename}"
        path = file_dir / Path(csv_name)
        with open(path, "wb") as f:
            f.write(bytes)
            paths.append(path)
            
    
    with get_session() as session:
        session.add(job)
        session.commit()
        
    asyncio.create_task(run_ingestion(job.job_id, paths))
    
    return IngestionJobCreated(job.job_id, status="queued")