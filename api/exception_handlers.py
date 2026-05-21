from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from core.exceptions import (
    JobNotFoundError, InvoiceNotFoundError
)


    
def invoice_not_found_error_handler(request: Request, exc: InvoiceNotFoundError):
    return JSONResponse(
        status_code=404,
        content={"detail": f"Invoice {exc.invoice_id} not found"},
    )

def job_not_found_error_handler(request: Request, exc: JobNotFoundError):
    return JSONResponse(
        status_code=404,
        content={"detail": f"Job {exc.job_id} not found"},
    )

def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(InvoiceNotFoundError, invoice_not_found_error_handler) # type: ignore[arg-type]
    app.add_exception_handler(JobNotFoundError, job_not_found_error_handler) # type: ignore[arg-type]