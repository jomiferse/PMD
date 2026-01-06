import importlib.util

from app.settings import settings


def test_execution_disabled_by_default():
    assert settings.EXECUTION_ENABLED is False


def test_execution_module_removed():
    assert importlib.util.find_spec("app.trading.polymarket_execution") is None
