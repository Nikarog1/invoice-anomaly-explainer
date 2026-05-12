import httpx
from json.decoder import JSONDecodeError

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from data.llm_client import call_local_llm



def _build_mock_async_client(response: dict | str):
    mock_response = MagicMock()
    mock_response.json.return_value = response
    
    mock_client = AsyncMock()
    mock_client.send.return_value = mock_response
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__ = AsyncMock(return_value=None)
    
    return mock_client
    

async def test_call_local_llm_expect_json_True_return_expected():
    response = {
        "message": {
            "content": 
                '{"num invoice": "invoice_number", '
                    +'"suppl":"supplier_name", '
                    +'"total invoice":"total_amount", '
                    +'"metadata_field": null, '
                    +'"hallucination": "happened"}'
        }
    }

    mock_client = _build_mock_async_client(response)
    
    with patch("data.llm_client.AsyncClient", return_value=mock_client):
        content = await call_local_llm(
            prompt="some instructions",
            expect_json=True
        )
         
    assert isinstance(content, dict)
    assert len(content) == 5
    assert content["num invoice"] == "invoice_number"
    

async def test_call_local_llm_expect_json_False_return_expected():
    response = {
        "message": {
            "content": "Some reply"
        }
    }

    mock_client = _build_mock_async_client(response)
    
    with patch("data.llm_client.AsyncClient", return_value=mock_client):
        content = await call_local_llm(
            prompt="some instructions",
            expect_json=False
        )
         
    assert isinstance(content, str)
    assert content == "Some reply"


async def test_call_local_llm_expect_json_wrong_output_raises_exception():
    response = {
        "message": {
            "content": "Some reply"
        }
    }

    mock_client = _build_mock_async_client(response)
    
    with patch("data.llm_client.AsyncClient", return_value=mock_client):
        with pytest.raises(JSONDecodeError):
            await call_local_llm(
                prompt="some instructions",
                expect_json=True
            )
            

async def test_call_local_llm_http_raises_exception():
    mock_client = AsyncMock()
    mock_client.send.side_effect = httpx.ConnectError("simulated")
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__ = AsyncMock(return_value=None)

    with patch("data.llm_client.AsyncClient", return_value=mock_client):
        with pytest.raises(httpx.HTTPError):
            await call_local_llm(
                prompt="some instructions",
                expect_json=True
            )