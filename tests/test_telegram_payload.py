from app.core.telegram import send_telegram_message
from app.settings import settings


def test_send_telegram_message_payload(monkeypatch):
    sent = {}

    class FakeResponse:
        def __init__(self):
            self.is_success = True
            self.status_code = 200
            self.text = "ok"

        def json(self):
            return {"ok": True}

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, url, json):
            sent["url"] = url
            sent["json"] = json
            return FakeResponse()

    monkeypatch.setattr("app.core.telegram.httpx.Client", FakeClient)
    monkeypatch.setattr(settings, "TELEGRAM_BOT_TOKEN", "token-123")

    result = send_telegram_message(
        chat_id=123,
        text="Hello",
        reply_markup={"inline_keyboard": [[{"text": "Ok", "callback_data": "ok"}]]},
    )

    assert result == {"ok": True}
    assert sent["url"].endswith("/bottoken-123/sendMessage")
    assert sent["json"]["chat_id"] == 123
    assert sent["json"]["text"] == "Hello"
    assert sent["json"]["parse_mode"] == "HTML"
    assert sent["json"]["disable_web_page_preview"] is True
    assert sent["json"]["reply_markup"]["inline_keyboard"][0][0]["text"] == "Ok"
