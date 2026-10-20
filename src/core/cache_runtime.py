from src.core.cache_policy import (
    CacheCircuitBreaker,
    DatabaseAdmission,
    QueryRateLimiter,
)
from src.core.config import settings

# 每个 API 进程共享一份预算；部署多个 worker 时总预算按进程数相乘。
cache_circuit = CacheCircuitBreaker(settings.CACHE_CIRCUIT_COOLDOWN_SECONDS)
cache_admission = DatabaseAdmission(
    concurrency=settings.CACHE_DB_CONCURRENCY,
    queue_size=settings.CACHE_DB_QUEUE_SIZE,
    queue_timeout=settings.CACHE_DB_QUEUE_TIMEOUT_SECONDS,
    query_timeout=settings.CACHE_DB_QUERY_TIMEOUT_SECONDS,
)
event_query_limiter = QueryRateLimiter(
    rate=settings.EVENT_QUERY_RATE,
    burst=settings.EVENT_QUERY_BURST,
    global_rate=settings.EVENT_QUERY_GLOBAL_RATE,
    global_burst=settings.EVENT_QUERY_GLOBAL_BURST,
    max_clients=settings.EVENT_QUERY_MAX_CLIENTS,
)
