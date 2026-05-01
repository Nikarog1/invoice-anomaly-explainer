import chromadb
from chromadb.utils.embedding_functions.ollama_embedding_function import OllamaEmbeddingFunction

from config.settings import settings
from schemas.contract import ContractWithLineItems



client = chromadb.PersistentClient(settings.chromadb_path)

embedding_function = OllamaEmbeddingFunction(
    url=settings.ollama_base_url,
    model_name=settings.embedding_model_name
)

def get_collection():
    return client.get_or_create_collection(
    name="data",
    metadata={"description": "Contract line items embedded for matching"},
    embedding_function=embedding_function, # type: ignore
    configuration={
        "hnsw": {
            "space": "cosine",
            "ef_construction": 200
        }
    }
)

def add_contract_line_items(
        collection: chromadb.Collection,
        contract_with_items: ContractWithLineItems,
    ) -> None:
    """
    Insert contract line items into vector collection.
    
    Each line item stored with contract_line_item_id as primary id and 
    product_service_name as embedded document. Metadata holds contract_id 
    and supplier_name for query-time filtering.
    """
    
    items = contract_with_items.line_items
    contract = contract_with_items.contract
    
    ids = [str(item.contract_line_item_id) for item in items]
    docs = [item.product_service_name for item in items]
    
    collection.add(
        ids=ids,
        documents=docs,
        metadatas=[
            {
                "contract_id": str(contract.contract_id),
                "supplier_name": contract.supplier_name,
            }
            for item in items
        ]
    )
        
def query_similar(
        collection: chromadb.Collection,
        invoice_line_descriptions: list[str], 
        supplier_name: str, 
        contract_line_item_ids: list[str], 
        n_results: int = 1
    ) -> chromadb.QueryResult:
    """
    Find top-n contract line items semantically closest to invoice lines descriptions.
    Process list of invoice lines descriptions at once.
    
    Filters by supplier_name and restricts results to provided contract_line_item_ids,
    so search stays within contracts in scope for the current invoice.
    """
    return collection.query(
        query_texts=invoice_line_descriptions,
        include=["metadatas", "documents", "distances"],
        n_results=n_results,
        ids=contract_line_item_ids,
        where={
            "supplier_name": supplier_name
        } # type: ignore
    )