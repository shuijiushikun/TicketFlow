"""查询缓存监控指标；每个 API/Arq 进程使用独立的注册表。

标签只使用路由模板与有限枚举，不接收活动 ID、客户端地址、原始 URL 或异常文本。
当前部署要求每个容器仅一个工作进程，由 Prometheus 分别抓取后聚合。
"""

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

registry = CollectorRegistry()

SECKILL_ENTRY_RESULTS = Counter(
    "ticketflow_seckill_entry_total",
    "秒杀申请入口准入结果；accepted 仅表示允许开始接收处理。",
    ("result",),
    registry=registry,
)
SECKILL_ENTRY_ACTIVE = Gauge(
    "ticketflow_seckill_entry_active",
    "当前正在接收处理的秒杀申请数量，按 API 进程限制。",
    registry=registry,
)
SECKILL_ENTRY_WAITING = Gauge(
    "ticketflow_seckill_entry_waiting",
    "尚未开始查询用户与幂等记录的秒杀申请等待数量。",
    registry=registry,
)
SECKILL_ENTRY_WAIT_SECONDS = Histogram(
    "ticketflow_seckill_entry_wait_seconds",
    "秒杀申请在本地入口缓冲中等待的时间。",
    registry=registry,
)
SECKILL_SUBMIT_SECONDS = Histogram(
    "ticketflow_seckill_submit_stage_seconds",
    "秒杀接收分段耗时；acceptance 包含鉴权、幂等及申请保存，分段有包含关系。",
    ("stage", "result"),
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
    registry=registry,
)

HTTP_REQUESTS = Counter(
    "ticketflow_http_requests_total",
    "HTTP 请求数，包含成功、拒绝与失败响应。",
    ("method", "route", "status"),
    registry=registry,
)
HTTP_DURATION = Histogram(
    "ticketflow_http_request_seconds",
    "HTTP 请求耗时，按响应状态区分成功与拒绝。",
    ("method", "route", "status"),
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
    registry=registry,
)
CACHE_EVENTS = Counter(
    "ticketflow_cache_events_total",
    "活动查询缓存事件；source 区分前台请求与后台预热。",
    ("kind", "source", "event"),
    registry=registry,
)
CACHE_REDIS_OPERATIONS = Counter(
    "ticketflow_cache_redis_operations_total",
    "查询缓存 Redis 操作结果。",
    ("result",),
    registry=registry,
)
CACHE_REDIS_DURATION = Histogram(
    "ticketflow_cache_redis_operation_seconds",
    "查询缓存 Redis 操作耗时。",
    registry=registry,
)
INVENTORY_READS = Counter(
    "ticketflow_inventory_reads_total",
    "余票展示读取次数，区分 Redis 命中与数据库回源。",
    ("source",),
    registry=registry,
)
DB_ADMISSION = Counter(
    "ticketflow_db_admission_total",
    "查询回源预算准入结果。",
    ("budget", "result"),
    registry=registry,
)
DB_ACTIVE = Gauge(
    "ticketflow_db_active",
    "当前占用的数据库回源名额。",
    ("budget",),
    registry=registry,
)
DB_QUEUED = Gauge(
    "ticketflow_db_queued",
    "当前等待数据库回源名额的查询数。",
    ("budget",),
    registry=registry,
)
DB_WAIT_SECONDS = Histogram(
    "ticketflow_db_wait_seconds",
    "数据库回源准入等待耗时，包含立即获准与被拒绝的请求。",
    ("budget",),
    registry=registry,
)
DB_QUERY_SECONDS = Histogram(
    "ticketflow_db_query_seconds",
    "数据库回源执行耗时。",
    ("budget", "result"),
    registry=registry,
)
HOTSPOT_RECORDS = Counter(
    "ticketflow_hotspot_records_total",
    "热点访问记录结果。",
    ("result",),
    registry=registry,
)
HOTSPOT_CANDIDATES = Gauge(
    "ticketflow_hotspot_candidates",
    "当前有界热点候选数量。",
    registry=registry,
)
WARMUP_RUNS = Counter(
    "ticketflow_cache_warmup_runs_total",
    "规则预热轮次及结果。",
    ("result",),
    registry=registry,
)
WARMUP_ITEMS = Counter(
    "ticketflow_cache_warmup_items_total",
    "规则预热项目及结果。",
    ("result",),
    registry=registry,
)
WARMUP_DURATION = Histogram(
    "ticketflow_cache_warmup_seconds",
    "规则预热每轮耗时。",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15, 20, 25),
    registry=registry,
)
