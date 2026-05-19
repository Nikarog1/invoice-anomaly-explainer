import json
import httpx
from httpx import AsyncClient

from config.settings import settings
from core.logging import get_logger

logger = get_logger(__name__)



async def call_local_llm(
    prompt: str,
    expect_json: bool = True,
    model_name: str = settings.model_name,
    ollama_url: str = settings.ollama_base_url,
) -> dict[str, str | None] | str | None:
    """Send prompt to local Ollama, return parsed JSON dict or raw string."""
    
    ollama_url_enhanced = f"{ollama_url.rstrip('/')}/api/chat"
    
    payload = {
    "model": model_name,
    "stream": False,
    "messages": [{"role": "user", "content": prompt}],
    "options": {"temperature": 0.3}
    }
    if expect_json:
        payload["format"] = "json"
        
    async with AsyncClient(timeout=settings.ollama_timeout) as client:
        request = client.build_request(
            "POST", 
            ollama_url_enhanced, 
            json=payload
        )
        response = await client.send(request)
        content = response.json()["message"]["content"]
        if expect_json:
            return json.loads(content)
        else:
            return content
        

async def verify_ollama_reachable() -> httpx.Response | None:
    """
    GET {ollama_base_url}/api/tags.
    Returns Response if reachable, None otherwise.
    Logs warning on failure.
    """
    ollama_url_enhanced = f"{settings.ollama_base_url.rstrip('/')}/api/tags"
    async with AsyncClient(timeout=5) as client:
        request = client.build_request(
            "GET", 
            ollama_url_enhanced, 
        )
        try:
            response = await client.send(request)
        except (httpx.ConnectError, httpx.TimeoutException):
            logger.warning("Ollama is not reachable")
            return None
        
    if response.status_code == 200:
        return response
    else:
        logger.warning(f"Ollama returned status {response.status_code}")
        return None


async def verify_ollama_models() -> None:
    """
    Calls verify_ollama_reachable first.
    If reachable, fetches /api/tags response, checks required models present.
    Logs warning per missing model.
    """
    response = await verify_ollama_reachable()
    
    if response:
        content = response.json()["models"]
        
        models_required = [settings.model_name, settings.embedding_model_name]
        installed_names = [m["name"] for m in content]
        missing = [
            required for required in models_required
            if not any(name.startswith(required) for name in installed_names)
        ]
                
        if not missing:
            logger.info("All models are available")
        
        else:
            logger.warning(f"Model{"" if len(missing) == 1 else "s"} "
                            f"not pulled: {missing}"
            )