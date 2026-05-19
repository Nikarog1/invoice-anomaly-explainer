from uuid import UUID

from fastapi import APIRouter, Depends
from sqlmodel import Session

from api.models.invoices import InvoiceDTO, InvoiceLineItemDTO
from data.sqlite import get_session, load_invoice_from_sql


router = APIRouter(prefix="/invoices", tags=["invoices"])

@router.get("/{invoice_id}")
async def get_invoice(invoice_id: UUID, session: Session = Depends(get_session)) -> InvoiceDTO:
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