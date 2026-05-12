from datetime import date

import hashlib

from data.vector_store import add_contract_line_items, query_similar
from schemas.contract import Contract, ContractLineItem, ContractWithLineItems



class FakeEmbeddingFunction:
    """Deterministic, offline embedding function for tests.
    
    Hashes input strings into fixed-dimension vectors. Same string → same vector.
    Vectors normalized so cosine similarity is meaningful.
    """
    
    DIM = 16
    
    def __call__(self, input):
        vectors = []
        for text in input:
            # deterministic vector from string hash
            digest = hashlib.sha256(text.encode("utf-8")).digest()
            vec = [b / 255.0 for b in digest[:self.DIM]]
            # normalize to unit length so cosine distance behaves
            norm = sum(v * v for v in vec) ** 0.5
            vec = [v / norm for v in vec] if norm > 0 else vec
            vectors.append(vec)
        return vectors
    
    def name(self):
        return "fake"
    
    def embed_query(self, input):
        return self(input)

    def embed_documents(self, input):
        return self(input)



def _build_contract_with_items(supplier_name: str = "suppl1", n_items: int = 2) -> ContractWithLineItems:
    contract = Contract(
        supplier_name=supplier_name,
        buyer_name="our_company",
        signed_on=date(2026, 4, 1)
    )
    items = [
        ContractLineItem(
            contract_id=contract.contract_id,
            product_service_name=f"service_{i}",
            unit_price=100.0 * (i + 1),
        )
        for i in range(n_items)
    ]
    return ContractWithLineItems(contract=contract, line_items=items)



def test_add_contract_line_items_persists_to_collection(fake_collection):
    contract_with_items = _build_contract_with_items(supplier_name="suppl1", n_items=2)
    
    add_contract_line_items(fake_collection, contract_with_items)
    
    assert fake_collection.count() == 2


def test_add_contract_line_items_metadata_set_correctly(fake_collection):
    contract_with_items = _build_contract_with_items(supplier_name="suppl1", n_items=1)
    
    add_contract_line_items(fake_collection, contract_with_items)
    
    contract_id = str(contract_with_items.contract.contract_id)
    item_id = str(contract_with_items.line_items[0].contract_line_item_id)
    
    result = fake_collection.get(ids=[item_id])
    
    assert result["metadatas"][0]["supplier_name"] == "suppl1"
    assert result["metadatas"][0]["contract_id"] == contract_id


def test_query_similar_returns_added_item(fake_collection):
    contract_with_items = _build_contract_with_items(supplier_name="suppl1", n_items=2)
    add_contract_line_items(fake_collection, contract_with_items)
    
    contract_ids = [str(item.contract_line_item_id) for item in contract_with_items.line_items]
    target_name = contract_with_items.line_items[0].product_service_name
    
    result = query_similar(
        fake_collection,
        invoice_line_descriptions=[target_name],
        supplier_name="suppl1",
        contract_line_item_ids=contract_ids,
        n_results=1,
    )
    
    assert result["ids"][0][0] == str(contract_with_items.line_items[0].contract_line_item_id)


def test_query_similar_filters_by_supplier(fake_collection):
    contract_a = _build_contract_with_items(supplier_name="suppl1", n_items=1)
    contract_b = _build_contract_with_items(supplier_name="suppl2", n_items=1)
    
    add_contract_line_items(fake_collection, contract_a)
    add_contract_line_items(fake_collection, contract_b)
    
    all_ids = [
        str(contract_a.line_items[0].contract_line_item_id),
        str(contract_b.line_items[0].contract_line_item_id),
    ]
    
    result = query_similar(
        fake_collection,
        invoice_line_descriptions=["anything"],
        supplier_name="suppl1",
        contract_line_item_ids=all_ids,
        n_results=2,
    )
    
    returned_ids = result["ids"][0]
    assert str(contract_a.line_items[0].contract_line_item_id) in returned_ids
    assert str(contract_b.line_items[0].contract_line_item_id) not in returned_ids


def test_query_similar_no_match_returns_empty(fake_collection):
    contract_with_items = _build_contract_with_items(supplier_name="suppl1", n_items=1)
    add_contract_line_items(fake_collection, contract_with_items)
    
    result = query_similar(
        fake_collection,
        invoice_line_descriptions=["anything"],
        supplier_name="nonexistent_supplier",
        contract_line_item_ids=[str(contract_with_items.line_items[0].contract_line_item_id)],
        n_results=1,
    )
    
    assert result["ids"][0] == []