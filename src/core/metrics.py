"""查询缓存监控指标；每个 API/Arq 进程使用独立的注册表。

标签只使用路由模板与有限枚举，不接收活动 ID、客户端地址、原始 URL 或异常文本。
当前部署要求每个容器仅一个工作进程，由 Prometheus 分别抓取后聚合。
"""

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

registry = CollectorRegistry()

HTTP_REQUESTS = Counter(
    "ticketcore_http_requests_total",
    "HTTP 请求数，包含成功、拒绝与失败响应。",
    ("method", "route", "status"),
    registry=registry,
)
HTTP_DURATION = Histogram(
    "ticketcore_http_request_seconds",
    "HTTP 请求耗时，按响应状态区分成功与拒绝。",
    ("method", "route", "status"),
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
    registry=registry,
)
CACHE_EVENTS = Counter(
    "ticketcore_cache_events_total",
    "活动查询缓存事件；source 区分前台请求与后台预热。",
    ("kind", "source", "event"),
    registry=registry,
)
CACHE_REDIS_OPERATIONS = Counter(
    "ticketcore_cache_redis_operations_total",
    "查询缓存 Redis 操作结果。",
    ("result",),
    registry=registry,
)
CACHE_REDIS_DURATION = Histogram(
    "ticketcore_cache_redis_operation_seconds",
    "查询缓存 Redis 操作耗时。",
    registry=registry,
)
DB_ADMISSION = Counter(
    "ticketcore_db_admission_total",
    "查询回源预算准入结果。",
    ("budget", "result"),
    registry=registry,
)
DB_ACTIVE = Gauge(
    "ticketcore_db_active",
    "当前占用的数据库回源名额。",
    ("budget",),
    registry=registry,
)
DB_QUEUED = Gauge(
    "ticketcore_db_queued",
    "当前等待数据库回源名额的查询数。",
    ("budget",),
    registry=registry,
)
DB_WAIT_SECONDS = Histogram(
    "ticketcore_db_wait_seconds",
    "数据库回源准入等待耗时，包含立即获准与被拒绝的请求。",
    ("budget",),
    registry=registry,
)
DB_QUERY_SECONDS = Histogram(
    "ticketcore_db_query_seconds",
    "数据库回源执行耗时。",
    ("budget", "result"),
    registry=registry,
)
HOTSPOT_RECORDS = Counter(
    "ticketcore_hotspot_records_total",
    "热点访问记录结果。",
    ("result",),
    registry=registry,
)
HOTSPOT_CANDIDATES = Gauge(
    "ticketcore_hotspot_candidates",
    "当前有界热点候选数量。",
    registry=registry,
)
WARMUP_RUNS = Counter(
    "ticketcore_cache_warmup_runs_total",
    "规则预热轮次及结果。",
    ("result",),
    registry=registry,
)
WARMUP_ITEMS = Counter(
    "ticketcore_cache_warmup_items_total",
    "规则预热项目及结果。",
    ("result",),
    registry=registry,
)
WARMUP_DURATION = Histogram(
    "ticketcore_cache_warmup_seconds",
    "规则预热每轮耗时。",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15, 20, 25),
    registry=registry,
)
