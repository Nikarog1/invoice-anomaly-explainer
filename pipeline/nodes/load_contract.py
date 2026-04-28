from collections import defaultdict

from sqlmodel import or_, select

from core.exceptions import PipelineStateError
from core.logging import get_logger

from data.sqlite import get_session

from pipeline.state import PipelineState

from schemas.contract import Contract, ContractLineItem, ContractSummary, ContractWithLineItems, DegradationReason

logger = get_logger(__name__)



def load_contract(state: PipelineState) -> dict:
    """
    Load contracts valid for invoice's supplier and issue date, paired with their line items.

    Contract is "valid" when:
    - supplier_name matches invoice
    - signed_on is on or before invoice.issue_date
    - expires_on is None (open-ended) or on or after invoice.issue_date

    Returned ContractSummary states:
        - is_degraded=False: valid contracts found.
        - is_degraded=True, issue_date_missing: invoice has no issue_date, validity unevaluable.
        - is_degraded=True, no_contract: no contract matches supplier and date window.

    Raises:
        PipelineStateError: invoice missing from state.
    """
    logger.info("Running load_contract")
    
    invoice = state["invoice"]
    if invoice is None:
        raise PipelineStateError("invoice")
    
    invoice_id = invoice.invoice_id
    supplier_name = invoice.supplier_name
    issue_date = invoice.issue_date
    
    if issue_date is None:
        logger.warning(f"Could not find issue date for invoice id: {invoice_id}")
        return {
                "contract_summary": ContractSummary(
                    contracts=[], 
                    is_degraded=True, 
                    degradation_reason=DegradationReason.issue_date_missing
                )
            }
        
    
    with get_session() as session:
        contracts = session.exec(
            select(Contract)
            .where(Contract.supplier_name == supplier_name)
            .where(Contract.signed_on <= issue_date)
            .where(or_(
                Contract.expires_on.is_(None), # type: ignore
                Contract.expires_on >= issue_date # type: ignore
                )
            )
        ).all()
        
        if not contracts:
            logger.warning(f"No valid contract for supplier={supplier_name}, issue_date={issue_date}")
            return {
                    "contract_summary": ContractSummary(
                        contracts=[], 
                        is_degraded=True, 
                        degradation_reason=DegradationReason.no_contract
                    )
                }

        contracts_line_items = session.exec(
            select(ContractLineItem)
            .where(ContractLineItem.contract_id.in_([contract.contract_id for contract in contracts])) # type: ignore
        ).all()

    d_line = defaultdict(list)
    for line_item in contracts_line_items:
        d_line[line_item.contract_id].append(line_item)
        
    results = []
    for contract in contracts:
        results.append(
            ContractWithLineItems(
                contract=contract,
                line_items=d_line[contract.contract_id]
            )
        )
        
    contract_summary = ContractSummary(
        contracts=results,
        is_degraded=False,
        degradation_reason=None,
    )
    
    logger.info(f"Loaded {len(results)} contracts with {len(contracts_line_items)} line items "
                f"for supplier '{supplier_name}', for invoice with issue_date {issue_date}"
    ) 
       
    return {
        "contract_summary": contract_summary
    }