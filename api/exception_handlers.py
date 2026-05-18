from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from core.exceptions import (
    InvoiceNotFoundError
)


    
def invoice_not_found_error_handler(request: Request, exc: InvoiceNotFoundError):
    return JSONResponse(
        status_code=404,
        content={"detail": f"Invoice {exc.invoice_id} not found"},
    )

def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(InvoiceNotFoundError, invoice_not_found_error_handler) # type: ignore[arg-type]