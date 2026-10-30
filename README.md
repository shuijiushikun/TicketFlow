# TicketFlow · 高并发活动票务后端

个人独立开发的 Python 后端项目，覆盖活动管理、普通预约、异步秒杀、模拟支付、取消与超时回收。围绕票务场景中的查询性能、突发流量、并发库存、重复请求与故障恢复，构建可运行、可观测、可验证的业务闭环。

项目提供 REST API、Swagger 接口文档和 Python 秒杀客户端，重点实现 PostgreSQL 原子扣票、Redis 查询缓存治理、有界准入与 Arq 异步削峰、持久化幂等、事务 Outbox 和异步补偿。

## 功能概览

| 模块 | 功能 |
|---|---|
| 用户与权限 | 注册、登录、JWT 鉴权、修改密码、管理员权限校验 |
| 活动与票种 | 活动及票种维护、分页查询、价格与总票量管理，按票种区分普通预约与秒杀 |
| 普通预约 | 同步预订一张普通票、查看个人票据、取消未支付预订 |
| 异步秒杀 | 秒杀票种专用入口、有界准入、持久化申请、独立队列消费、按编号查询结果 |
| 秒杀客户端 | 同键有限重试、退避错峰、遵守 Retry-After，受理后自动查询结果 |
| 模拟支付 | 校验归属、状态和支付期限，将有效预订改为已售出 |
| 超时回收 | Arq 延迟任务释放到期预订，数据库定时扫描补偿漏调度 |
| 查询缓存 | 活动详情与列表缓存、空值缓存、互斥重建、版本校验、有界回源 |
| 余票展示 | Redis 读取剩余票量，缺失或故障时有界回源并修复副本 |
| 可靠性 | 请求幂等、事务 Outbox、失败退避重试、库存版本校验 |
| 监控与预热 | Prometheus 指标、Grafana 看板、请求追踪、规则热点识别与缓存预热 |

票据状态为 `RESERVED`（预留）、`SOLD`（已售出）、`CANCELED`（已取消）。取消保留记录，通过状态校验保证库存只释放一次。

## 技术栈

- **接口与校验：** Python 3.12+、FastAPI、Pydantic V2、JWT、Argon2、HTTPX。
- **数据库：** PostgreSQL 16、SQLAlchemy 2 异步会话、Asyncpg、Alembic。
- **缓存与任务：** Redis 7、Lua、Arq。
- **部署与观测：** Docker Compose、Nginx、Prometheus、Grafana、JSON 日志。
- **质量与验证：** Pytest、Locust、Ruff、Mypy、GitHub Actions。

## 系统结构

```mermaid
flowchart LR
    Client[客户端 / Swagger] --> Nginx[Nginx]
    Nginx --> API[FastAPI]
    API --> PG[(PostgreSQL)]
    API --> Redis[(Redis)]
    Worker[维护 Worker] --> PG
    Worker --> Redis
    SeckillWorker[独立秒杀 Worker] --> PG
    SeckillWorker --> Redis
    Prometheus[Prometheus] --> API
    Prometheus --> Worker
    Grafana[Grafana] --> Prometheus
```

PostgreSQL 保存业务事实、库存占用、幂等响应、秒杀申请和待办；Redis 保存查询缓存、余票副本、短期互斥、热点统计与秒杀准入名额，并为 Arq 提供任务存储。维护 Worker 执行回收、补偿与预热，秒杀 Worker 在独立队列中处理抢票申请。两类 Worker 仍共享数据库与 Redis 服务。

## 核心设计

### 1. 原子扣票与状态协调

预订使用条件更新，在数据库中完成库存判断与扣减：

```sql
UPDATE ticket_types
SET tickets_sold = tickets_sold + 1,
    inventory_version = inventory_version + 1
WHERE id = :ticket_type_id
  AND tickets_sold < tickets_quantity
RETURNING id, tickets_quantity, tickets_sold, inventory_version;
```

库存占用、票据创建、库存同步待办及可选幂等响应在同一事务中提交，提交前异常由数据库事务统一回滚。`tickets_sold` 表示预留和已售出共同占用的数量。

支付、取消和超时回收锁定同一张票据，在锁内复核状态与期限；取消和回收只释放一次占用。管理员调整总量时锁定票种，以最新占用量校验总票量下限。数据库约束限制占用数量处于零和总量之间。

### 2. 活动查询缓存治理

- 对数据库确认不存在的活动写入 30～60 秒的空值缓存，减少重复回源。
- 详情缓存默认 24～25 小时，列表缓存默认 60～70 分钟，通过随机 TTL 分散自然过期。
- 仅缓存未命中时竞争重建锁；持有者二次检查，竞争者最多等待 0.3 秒。
- 锁使用随机 token 和有限租期；Lua 释放时核对持有者。
- 回填 Lua 同时核对缓存版本和锁 token，拒绝失效版本及过期持有者写入。
- Redis 故障时进入进程内熔断与有界回源；默认每进程最多 4 个回源查询、8 个等待者，限制排队和执行时间。
- 查询区分业务不存在 `404`、请求限流 `429` 和暂时不可用 `503`，后两者附带 `Retry-After`。

活动及票种变更通过事务 Outbox 驱动缓存版本切换，结合回填校验和重试补偿实现缓存最终一致性。

### 3. 持久化幂等与可靠补偿

携带 `Idempotency-Key` 的接口，以用户、操作和键组成数据库唯一作用域，并保存请求摘要。业务变更与原始响应快照同事务提交；同键同内容重试复用已提交结果，内容冲突或仍在处理中返回 `409`。

Redis 短租期锁用于快速拒绝竞争，持久去重由数据库唯一约束保证。客户端为同一次操作的重试复用原键，新操作生成新键；响应快照用于重试复用，票据最新状态通过查询接口获取。

缓存失效、库存同步、预订标记清理和秒杀申请投递先写入事务 Outbox，提交后快速尝试执行。失败保留记录，由维护 Worker 每 10 秒有限扫描重试，按 1～300 秒退避；每轮最多领取 1000 条，共用 5 秒预算。库存同步按票种合并，只确认本批实际锁定的待办，其他记录留待后续处理。

处理器通过版本校验、状态复核和可重复删除，支持任务重复执行与故障后恢复。

### 4. 余票展示与库存副本

`GET /api/v1/ticket-types/{ticket_type_id}/inventory` 返回供展示的 `remaining`。Redis 命中时不查询数据库；副本缺失、损坏或缓存故障时，复用有界回源预算查询并尝试修复。

库存变化随业务事务推进 `inventory_version`，Lua 拒绝低版本覆盖；版本使用定宽十进制字符串比较，避免 BIGINT 转为浮点数后的精度问题。删除票种保留版本墓碑，阻止迟到快照恢复已删除的余票。

Redis 余票副本用于展示，扣票、支付和结算判断以 PostgreSQL 为准。库存同步采用版本控制，活动查询缓存采用独立的 TTL 策略。

### 5. 规则热点、预热与观测

成功的活动详情访问写入有界分钟桶，根据访问量和增长率识别热点。Arq 每分钟第 10 秒预热默认第一页及最多 20 个热点详情，复用在线缓存的重建锁和版本校验。预热只在剩余 TTL 不足时回源，限制批次时间与回源并发，缓存故障时暂停。

API 与 worker 分别导出 Prometheus 指标，Grafana 展示成功率、分状态延迟、缓存命中、锁竞争、回源压力与预热结果。HTTP 指标使用路由模板，避免活动 ID 等无界标签；响应中的 `X-Request-ID` 与 JSON 日志关联。

### 6. 异步秒杀与入口保护

管理员通过票种的 `is_seckill` 字段区分预约模式：普通票种继续使用 `POST /tickets`；秒杀票种使用 `POST /seckill/requests`，普通预约入口会拒绝该票种。

秒杀流程为：

1. 接收前先做请求限流与进程内有界准入，默认每个 API 最多 8 个接收请求同时执行、56 个等待者，等待预算 1 秒；这部分保护位于鉴权查库之前。
2. Redis Lua 维护带期限的全局排队名额，默认 200 个、期限 60 秒；名额控制待处理申请，票量仍由数据库判断。
3. 将申请、幂等响应和入队 Outbox 同事务保存，提交后尝试投递 Arq，返回 `202` 和申请编号；受理凭证通过 `Location` 指向结果接口。
4. 独立秒杀 Worker 默认最多并发处理 4 个任务，在数据库事务中完成扣票、创建票据、更新申请结果及写入库存同步待办。重复消费先检查申请状态，避免再次扣票。
5. 用户查询 `GET /seckill/requests/{request_id}` 获取最终结果；成功后复用原有支付、取消、库存同步与超时回收流程。支付期限从实际创建票据时开始计算。

申请状态包含 `QUEUED`（排队）、`SUCCEEDED`（预订成功）、`SOLD_OUT`（售罄）、`EXPIRED`（排队超时）和 `REJECTED`（票种不存在或预约模式不符）。`SUCCEEDED` 返回票据编号，之后仍需完成支付。相同用户使用不同幂等键表示不同申请，当前未设置一人一票限制。

投递失败由事务 Outbox 补偿；秒杀 Worker 每 5 秒检查待处理申请，补投任务或处理已到期申请。Redis 中的准入名额带有租期，持久化申请和最终结果保存在 PostgreSQL。入口预算按 API 进程分别计算，排队准入通过 Redis 协调；开启秒杀时，API 启动会预建默认 8 条数据库连接，连接池上限保持 20。

### 7. 同键重试与结果查询

提供 `SeckillClient` 和 `python -m src.clients` 命令行工具：

- 同一次申请保留相同票种和 `Idempotency-Key`；初次提交加最多 3 次重试。
- 对网络暂时故障、HTTP `429/502/503/504` 及明确的“相同请求处理中”响应进行有限重试；基础等待为 1、2、4 秒，再增加随机偏移，并遵守 `Retry-After`。
- 单次请求最多 5 秒，每次请求及其重试共用 30 秒预算；身份错误、幂等内容冲突等交由调用方处理。
- 收到申请编号后只查询原申请，排队时查询间隔至少 1 秒并随机错开，默认最多等待 70 秒；停止等待后可使用原编号继续查询。

上述自动重试和查询策略在使用该客户端时生效。直接调用 API 的程序需遵守同样的幂等约定；客户端停止等待不会撤销后台已保存的申请。

## 并发验证

使用 Pytest 验证库存竞争、幂等、事务回滚、支付状态流转及秒杀恢复，通过 Locust 和专用突发脚本测量查询与秒杀入口。以下均为已有测试记录，本地原始结果保存在 `load_tests/artifacts/`。

- **库存正确性：** 核对并发预订后的有效票据、数据库库存占用与 Redis 余票。
- **查询性能：** 统计每秒成功查询数、成功响应 P95/P99 与 HTTP 成功率。
- **秒杀承压：** 分开统计受理、容量拒绝、依赖异常与客户端超时，再核对已受理申请的最终结果。
- **后台恢复：** 观察同步待办积压，并核对压测结束后的库存与待办处理结果。

本文的**查询 QPS**指每秒成功完成的 HTTP 查询请求，与 Locust 的成功请求 RPS 对齐；它统计接口查询，数据库 SQL 执行次数另计。RPS 使用工具实际统计窗口计算，P95/P99 对应成功响应；秒杀返回 `202` 表示申请已受理，最终是否抢到票通过结果状态统计。

### 活动详情查询

本机 Windows、i9-12900H、约 16GB 内存，压测端与服务端同机；4 个独立 API 进程，专用 PostgreSQL 16 / Redis 7，每进程数据库连接池上限 20，Redis 查询操作预算 150ms。缓存预热后访问 50 个活动，80% 流量集中于 5 个热点，零思考时间；查询压测临时调高限流阈值至每进程 10000/s，业务默认配置保持原值。

| 日期与负载 | 成功查询 QPS | 成功响应 P95 / P99 | 请求结果 |
|---|---|---|---|
| 2026-10-22：100 并发用户，配置运行 5 分钟 | **1044.38** | **130ms / 160ms** | 312038 次请求，312034 次 200、4 次 503，成功率 **99.9987%** |
| 2026-10-25：100 并发用户，配置运行 60 秒 | **1168.19** | **100ms / 120ms** | 68631 次请求全部 200 |
| 2026-10-27：100 并发用户，配置运行 30 分钟 | **1232.74** | **95ms / 120ms** | 2211609 次请求，2209751 次 200、1858 次 503，成功率 **99.916%** |

以上结果来自不同轮次，分别保留各自时长、延迟与错误率；表示对应负载下的本机测量，容量上限尚未验证。2026-10-25 数据采用 Locust 退出时的最终日志，包含最后一次 CSV 快照之后完成的在途请求。

2026-10-27 的 Locust 配置时长为 1800 秒，扣除启动与统计重置后，实际统计窗口约 1792.55 秒（29 分 53 秒）。期间出现两段短时缓存操作超时，随后进入有界回源；1298 次准入拒绝及 560 次排队超时对应上述 1858 次 503，结束时 4 个 API 就绪检查均为 200。压测生成器出现 CPU 高于 90% 的告警，缓存超时的根因仍待定位；本轮用于观察持续运行表现。

预热链路另在正式压测前核验：真实访问识别 5 个热点后，仅将其专用缓存 TTL 调至 240 秒，等待定时 worker 自动刷新；5 个热点 TTL 恢复到 24 小时以上，后台实际回填计数增加 6（含默认列表），验证热点识别与自动预热流程。

#### 数据库、缓存与主动预热对照

2026-10-30，在本机专用 PostgreSQL / Redis 上，使用 1 个 API 和 1 个维护 Worker，对相同 5 个活动回放同一请求轨迹。各组运行 60 秒、发送 1640 次请求：前 40 秒每秒 30 次，暂停 6 秒，第 46 秒同时发起 50 次查询，第 47 秒恢复每秒 30 次至结束。使用 Python 固定节奏回放，三组轨迹哈希一致。

A 组仅在测试进程中让活动详情直接查库；B、C 组先建立详情缓存，再统一将测试键设为开测后 45 秒过期，C 使用原有 Arq 定时预热。三组均保留接口校验、热点记录、每 IP 100/s 限流及数据库每进程 4 并发、8 等待者、50ms 排队预算；业务源码、默认 TTL 和 150ms Redis 操作预算保持原值。

| 组别 | 成功 / 总请求 | 全程成功 P95 | 突发成功 / 50 请求 | 突发成功 P95 | 前台详情回源 | 后台详情 / 列表回源 |
|---|---|---|---|---|---|---|
| A：直接查库 | 1586 / 1640 | 41.28ms | 4 / 50 | 521.77ms | 1586 | 0 / 0 |
| B：缓存，关闭预热 | 1626 / 1640 | 22.93ms | 36 / 50 | 620.59ms | 5 | 0 / 0 |
| C：缓存，开启预热 | 1640 / 1640 | 25.85ms | 50 / 50 | 411.06ms | 0 | 5 / 1 |

缓存尚未到期的前 40 秒，成功响应 P95 分别为 **38.58 / 21.64 / 21.19ms**。A 组共 54 次 503，其中 38 次回源容量拒绝、16 次排队超时；B 组 14 次 503 均为缓存锁等待超时；C 组本轮无失败。回源按应用层逻辑读取计数，一次详情读取包含活动及关联票种加载，SQL 条数另计。

A、B 对照观察到缓存大幅减少前台查库；B、C 对照观察到预热将 5 次详情重建提前交给后台完成，突发前 5 个缓存均已刷新、前台锁竞争为零。两组详情回源总数仍各为 5，C 还额外预热默认列表 1 次，因此总数据库读取没有减少。C 的整段成功 P95 较 B 略高，本轮收益集中于临期突发时的成功率和等待表现。

这是一轮固定 A/B/C 顺序、同机运行、人工制造临期的短测。C 组前两次准备因缓存回填或锁释放超时、短期 503 未达到开测条件，均未发正式负载；保存记录后，在初始详情缓存确认存在时补测。P95 仅统计成功响应，尤其 A 组突发仅有 4 个成功样本，应与失败数一起解读；该结果不用于推断最大 QPS、日常预热收益或统一性能提升比例。[三组核对数据与测试条件](docs/benchmarks/cache-comparison-20261030.json)随仓库保存，逐请求 CSV、指标及准备失败日志留在本地原始产物中。

#### 30 分钟压测可视化

以下截图来自 2026-10-27 同一轮压测的原始 Locust HTML 报告，展示请求汇总以及吞吐、失败速率和响应延迟的变化。

![100 并发用户、30 分钟档的活动查询请求统计](docs/images/activity-query-100u-30m-summary.jpg)

概览中的 `activity [200]` 对应成功查询 QPS；`Aggregated` 和趋势图中的 RPS 包含 HTTP 503 请求。因此，成功查询为 **1232.74 QPS**，全部请求为 **1233.78 RPS**。

![活动查询压测的吞吐量、失败速率与响应时间趋势](docs/images/activity-query-100u-30m-charts.jpg)

趋势图显示各采样时刻的统计，延迟曲线展示第 50 和第 95 百分位；整体成功响应的 P95/P99 见上表。红色曲线保留了本轮短时失败的记录。

### 余票查询优化对照

2026-10-22，单 API、10 并发用户、每轮配置运行 30 秒，零思考时间，在相同专用资源上比较数据库读取与 Redis 副本读取，业务库存保持不变：

| 读取方式 | 成功查询 QPS | P95 | HTTP 失败数 | 测量窗口数据库回源次数 |
|---|---|---|---|---|
| 数据库读取 | 149.89 | 81ms | 33 / 4260 | 4227 |
| Redis 预热后读取 | 387.91 | 31ms | 0 / 10989 | 0 |

该短测中成功查询吞吐达到原来的 **2.59 倍**，P95 降低约 **62%**。对照针对余票展示读取，扣票仍由 PostgreSQL 事务执行。

### 千请求秒杀突发

2026-10-25，本机 4 个 API 进程、1 个并发为 4 的独立秒杀 Worker；1000 个独立账号各绑定不同回环源 IP、各提交一次申请，争抢同一秒杀票种的 100 张票。客户端完成 TCP 预热后统一放行，发起跨度约 0.777 秒、在途请求峰值 1000；本轮未启用客户端重试或结果轮询。

| 检查项 | 实测结果 |
|---|---|
| HTTP 受理与容量拒绝 | **210 次 202**；**790 次 503**，其中 746 次来自入口并发/等待保护、44 次来自全局排队名额不足 |
| 已受理申请的最终结果 | **100 次预订成功、110 次售罄**，待处理申请归零 |
| 客户端超时、非预期依赖异常与 HTTP 500 | 均为 **0**；容量拒绝仍按 503 单列 |
| 受理响应与最终处理时间 | 202 响应 P95 约 **2.50 秒**；全部 210 条申请从首个提交起约 **27.87 秒**处理完 |
| 票据与库存核对 | 100 张唯一票据、100 个不同归属用户；数据库与 Redis 余票均为 0、版本均为 100；Outbox 归零 |
| 压测后服务 | API 与 Worker 存活，4 个 API 就绪检查均为 200 |

这组测试验证了有界过载保护、已受理申请完成处理与库存正确性。客户端重试功能在之后完成并通过针对性检查，其性能收益尚未进行真实压测。

## 快速开始

### 本地运行

准备 Python 3.12+、PostgreSQL 16 和 Redis 7。在项目根目录首次配置：

```powershell
python -m pip install -e .
Copy-Item .env.example .env
```

修改 `.env` 中的 `SECRET_KEY`、数据库凭证与服务地址，并先创建 `POSTGRES_DB` 指定的数据库。示例 PostgreSQL 端口为 `5433`，以实际环境为准。已有环境沿用实际数据库名称，将 `PROJECT_NAME` 设置为 `TicketFlow` 即可。

执行迁移并启动 API：

```powershell
python -m alembic upgrade head
python -m uvicorn src.main:app --host 127.0.0.1 --port 8000
```

另开终端，在相同环境启动 worker：

```powershell
python -m arq src.worker.WorkerSettings
```

运行 `python create_superuser.py`，按提示创建管理员账号。

- Swagger：[http://localhost:8000/docs](http://localhost:8000/docs)
- 存活检查：[http://localhost:8000/healthz](http://localhost:8000/healthz)
- 依赖就绪检查：[http://localhost:8000/readyz](http://localhost:8000/readyz)

### Docker Compose

配置 `.env` 后执行：

```powershell
docker compose up --build -d
```

启动 PostgreSQL、Redis、API、worker 和 Nginx，API 自动执行数据库迁移，Swagger 位于 [http://localhost/docs](http://localhost/docs)。

启用监控时，先在本地 `.env` 设置 `GRAFANA_ADMIN_PASSWORD`，再执行：

```powershell
docker compose -f docker-compose.yml -f docker-compose.monitoring.yml --profile monitoring up -d --build
```

Prometheus 位于 [http://localhost:9090](http://localhost:9090)，Grafana 位于 [http://localhost:3000](http://localhost:3000)，管理员用户名为 `admin`。Nginx 不向外暴露 `/metrics`，监控通过容器内网抓取 API 与 worker。

### 启用异步秒杀

秒杀接收入口默认关闭。在本地 `.env` 中设置 `SECKILL_ENABLED=true`，执行最新迁移，并让 API 与两类 Worker 使用相同的数据库、Redis 和队列配置。管理员通过票种创建或修改接口将目标票种设为 `is_seckill=true`。

本地运行时，按前述方式启动或重启 API 与维护 Worker，再另开终端启动独立秒杀 Worker：

```powershell
python -m arq src.seckill_worker.WorkerSettings
```

Docker Compose 使用 `seckill` profile 启动该 Worker；同样需要先在 `.env` 开启秒杀接收：

```powershell
docker compose --profile seckill up --build -d
```

维护 Worker 仍需运行，负责库存同步补偿、预订超时回收和查询缓存预热。默认秒杀队列名为 `ticketflow:seckill:queue`，消费并发为 4；入口的 8 个执行名额、56 个等待名额按 API 进程独立计算。

### 使用秒杀客户端

下面示例连接本地启动的 `8000` 端口；命令显式传入地址，以覆盖客户端默认的 `18000` 端口。先登录取得令牌，将票种编号替换为实际的秒杀票种：

```powershell
$env:TICKETFLOW_ACCESS_TOKEN = "登录后获得的 access_token"
$attemptKey = [guid]::NewGuid().ToString("N")
python -X utf8 -B -m src.clients --base-url http://127.0.0.1:8000 submit --ticket-type-id 1 --idempotency-key $attemptKey
```

命令会输出幂等键、申请编号和最终结果。同一次申请需要再次提交时，保留原 `$attemptKey`，只重复执行 `submit` 命令；开始新的购票操作时再生成新键。

已有申请编号时，直接查询该编号；将示例中的 `12` 换为实际返回值：

```powershell
python -X utf8 -B -m src.clients --base-url http://127.0.0.1:8000 result --request-id 12
```

查询等待结束或手动停止后，仍可继续查询原编号。`SUCCEEDED` 返回的 `ticket_id` 可用于原有支付或取消接口；返回售罄、超时或拒绝状态时，本次申请已结束。

### 常用接口

默认前缀为 `/api/v1`，完整参数与响应以 Swagger 为准。

| 方法与路径 | 用途 |
|---|---|
| `POST /auth/signup`、`POST /auth/login` | 注册与登录 |
| `GET /events`、`GET /events/{event_id}` | 活动列表与详情 |
| `GET /events/{event_id}/ticket-types` | 活动下的票种 |
| `GET /ticket-types/{ticket_type_id}/inventory` | 余票展示 |
| `POST /tickets` | 同步预订一张普通票 |
| `POST /seckill/requests` | 提交秒杀申请，必须携带 Idempotency-Key，返回 202 和编号 |
| `GET /seckill/requests/{request_id}` | 查询本人秒杀结果，成功时返回票据编号 |
| `GET /tickets`、`GET /tickets/{ticket_id}` | 本人的票据 |
| `DELETE /tickets/{ticket_id}` | 取消预订 |
| `POST /tickets/{ticket_id}/payments` | 模拟支付 |
| `GET /analytics/hot-events` | 管理员查看规则热点 |

### 测试与负载工具

开发依赖可通过 `uv sync --locked --all-groups` 安装。运行无需外部服务的单元测试或本地回归：

```powershell
python -m pytest --noconftest -p no:cacheprovider tests/unit -q
python -m pytest --confcutdir tests/regression tests/regression tests/unit
```

本地回归使用临时 SQLite 与内存 Redis。真实数据库并发验证使用独立 PostgreSQL 与 Redis；集成与并发测试的根级 fixture 会重建专用测试数据库并清空专用 Redis，必须显式配置隔离资源：

```powershell
docker compose -f docker-compose.test.yml up -d --wait
$env:TICKETFLOW_TEST_ALLOW_RESET = "1"
$env:TICKETFLOW_TEST_POSTGRES_DSN = "postgresql://tester:test_only_password@127.0.0.1:55433/ticketflow_integration_test"
$env:TICKETFLOW_TEST_REDIS_URL = "redis://127.0.0.1:56379/0"
$env:TICKETFLOW_TEST_RESOURCE_TOKEN = [guid]::NewGuid().ToString("N")
python -m tests.prepare_resources --initialize-isolated-resources
python -m pytest tests/stress
```

初始化命令仅用于已确认的专用实例。缺少配置、数据库名不满足 `_test` 后缀或所有权标记不匹配时，测试拒绝重置。

`load_tests/` 提供混合、查询、预订与余票读取场景。压测数据初始化只接受 `_loadtest` 后缀的专用 PostgreSQL 数据库，不重置已有票据或库存；真实用户与票种 ID 从 manifest 读取。Windows 下 Locust 使用 `python -X utf8 -m locust`，避免中文配置的默认编码问题。含认证的场景需要显式设置与专用被测服务一致的 `LOADTEST_SECRET_KEY`，不在报告中保存密钥。

客户端重试另有 16 项针对性隔离检查，覆盖重试上限、Retry-After、随机退避、有界查询，以及“申请已保存但回复丢失”后的同键恢复；这些检查验证功能行为，真实负载收益另行测量。

## 目录结构

```text
.
├── src/
│   ├── api/              # 路由、权限与依赖注入
│   ├── clients/          # 秒杀同键重试、结果查询与命令行入口
│   ├── core/             # 配置、缓存策略、锁、指标与日志
│   ├── db/               # 数据库会话与 Redis 连接
│   ├── models/           # 业务表、幂等记录与 Outbox
│   ├── repositories/     # 活动数据库访问与缓存包装
│   ├── schemas/          # 请求与响应模型
│   ├── services/         # 业务事务、库存、补偿与预热
│   ├── worker.py         # 回收、补偿、预热等维护任务
│   └── seckill_worker.py # 独立秒杀队列消费与申请恢复
├── migrations/           # Alembic 迁移
├── tests/                # 单元、回归、集成与并发验收
├── load_tests/           # Locust 场景与数据准备
├── monitoring/           # Prometheus 与 Grafana 配置
├── nginx/                # 反向代理配置
└── .github/workflows/    # CI
```
