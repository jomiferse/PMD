import asyncio
import logging
import threading
import time
from contextlib import asynccontextmanager, contextmanager

from .settings import settings

logger = logging.getLogger(__name__)


class CircuitBreaker:
    def __init__(self, name: str, max_failures: int, reset_seconds: int) -> None:
        self.name = name
        self.max_failures = max(int(max_failures), 1)
        self.reset_seconds = max(int(reset_seconds), 1)
        self._failures = 0
        self._opened_at: float | None = None
        self._lock = threading.Lock()

    def allow(self) -> bool:
        with self._lock:
            if self._opened_at is None:
                return True
            if time.monotonic() - self._opened_at >= self.reset_seconds:
                self._failures = 0
                self._opened_at = None
                return True
            return False

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._opened_at = None

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._failures >= self.max_failures:
                if self._opened_at is None:
                    self._opened_at = time.monotonic()
                    logger.warning("circuit_opened name=%s failures=%s", self.name, self._failures)


def _bounded_semaphore(limit: int | None) -> threading.BoundedSemaphore | None:
    if limit is None or limit <= 0:
        return None
    return threading.BoundedSemaphore(limit)


def _async_semaphore(limit: int | None) -> asyncio.Semaphore | None:
    if limit is None or limit <= 0:
        return None
    return asyncio.Semaphore(limit)


@contextmanager
def limited(semaphore: threading.BoundedSemaphore | None):
    if semaphore is None:
        yield
        return
    semaphore.acquire()
    try:
        yield
    finally:
        semaphore.release()


@asynccontextmanager
async def async_limited(semaphore: asyncio.Semaphore | None):
    if semaphore is None:
        yield
        return
    async with semaphore:
        yield


POLYMARKET_SEMAPHORE = _async_semaphore(settings.EXTERNAL_MAX_CONCURRENT_POLY_CALLS)
LLM_SEMAPHORE = _bounded_semaphore(settings.EXTERNAL_MAX_CONCURRENT_LLM_CALLS)
TELEGRAM_SEMAPHORE = _bounded_semaphore(settings.EXTERNAL_MAX_CONCURRENT_TELEGRAM_CALLS)

POLYMARKET_BREAKER = CircuitBreaker(
    "polymarket",
    settings.POLY_CIRCUIT_MAX_FAILURES,
    settings.POLY_CIRCUIT_RESET_SECONDS,
)
LLM_BREAKER = CircuitBreaker(
    "llm",
    settings.LLM_CIRCUIT_MAX_FAILURES,
    settings.LLM_CIRCUIT_RESET_SECONDS,
)
TELEGRAM_BREAKER = CircuitBreaker(
    "telegram",
    settings.TELEGRAM_CIRCUIT_MAX_FAILURES,
    settings.TELEGRAM_CIRCUIT_RESET_SECONDS,
)
