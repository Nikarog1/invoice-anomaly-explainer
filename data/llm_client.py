import json
from httpx import AsyncClient

from config.settings import settings



async def call_local_llm(
    prompt: str,
    expect_json: bool = True,
    model_name: str = settings.model_name,
    ollama_url: str = settings.ollama_base_url,
) -> dict[str, str | None] | str | None:
    """Send prompt to local Ollama, return parsed JSON dict or raw string."""
    ollama_url_enhanced = f"{ollama_url.rstrip('/')}/api/chat"
    async with AsyncClient(timeout=settings.ollama_timeout) as client:
        request = client.build_request(
            "POST", 
            ollama_url_enhanced, 
            json={ 
                "model": model_name, 
                "format": "json", 
                "stream": False, 
                "messages": [
                    {
                        "role": "user", 
                        "content": prompt
                    }
                ] 
            }
        )
        response = await client.send(request)
        content = response.json()["message"]["content"]
        if expect_json:
            return json.loads(content)
        else:
            return content
    