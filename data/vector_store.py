import chromadb
from chromadb.utils.embedding_functions.ollama_embedding_function import OllamaEmbeddingFunction

from config.settings import settings



client = chromadb.PersistentClient(settings.chromadb_path)

embedding_fuction = OllamaEmbeddingFunction(
    url=settings.ollama_base_url,
    model_name=settings.embedding_model_name
)

collection = client.get_or_create_collection(
    name="data",
    metadata={"description": "Contract line items embedded for matching"},
    embedding_function=embedding_fuction, # type: ignore
    configuration={
        "hnsw": {
            "space": "cosine",
            "ef_construction": 200
        }
    }
)