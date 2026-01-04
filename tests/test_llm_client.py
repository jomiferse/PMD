import json

from app.llm.client import _parse_openai_response


def test_llm_response_parsing_invalid_json_returns_wait():
    payload = {
        "choices": [
            {"message": {"content": "not-json"}},
        ]
    }
    result = _parse_openai_response(payload)
    assert result["recommendation"] == "WAIT"
    assert result["confidence"] == "LOW"


def test_llm_response_parsing_valid_json():
    content = json.dumps(
        {
            "recommendation": "BUY",
            "confidence": "HIGH",
            "rationale": "Strong dislocation.",
            "risks": "Liquidity may be thin.",
        }
    )
    payload = {"choices": [{"message": {"content": content}}]}
    result = _parse_openai_response(payload)
    assert result["recommendation"] == "BUY"
    assert result["confidence"] == "HIGH"
