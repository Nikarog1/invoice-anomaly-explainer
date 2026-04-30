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
    async with AsyncClient() as client:
        request = client.build_request(
            "POST", 
            ollama_url, 
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
    