# TicketFlow · 活动票务预约后端

个人独立开发的 Python 后端项目，覆盖活动管理、票务预订、模拟支付、取消与超时回收。围绕票务场景中的并发库存、重复请求、缓存竞争与故障恢复，构建可运行、可观测、可验证的业务闭环。

项目提供 REST API 与 Swagger 接口文档，重点实现 PostgreSQL 原子扣票、Redis 查询缓存治理、持久化幂等、事务 Outbox 和异步补偿。

## 功能概览

| 模块 | 功能 |
|---|---|
| 用户与权限 | 注册、登录、JWT 鉴权、修改密码、管理员权限校验 |
| 活动与票种 | 活动及票种维护、分页查询、价格与总票量管理 |
| 票务预订 | 单次预订一张票、查看个人票据、取消未支付预订 |
| 模拟支付 | 校验归属、状态和支付期限，将有效预订改为已售出 |
| 超时回收 | Arq 延迟任务释放到期预订，数据库定时扫描补偿漏调度 |
| 查询缓存 | 活动详情与列表缓存、空值缓存、互斥重建、版本校验、有界回源 |
| 余票展示 | Redis 读取剩余票量，缺失或故障时有界回源并修复副本 |
| 可靠性 | 请求幂等、事务 Outbox、失败退避重试、库存版本校验 |
| 监控与预热 | Prometheus 指标、Grafana 看板、请求追踪、规则热点识别与缓存预热 |

票据状态为 `RESERVED`（预留）、`SOLD`（已售出）、`CANCELED`（已取消）。取消保留记录，通过状态校验保证库存只释放一次。

## 技术栈

- **接口与校验：** Python 3.12+、FastAPI、Pydantic V2、JWT、Argon2。
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
    Worker[Arq Worker] --> PG
    Worker --> Redis
    Prometheus[Prometheus] --> API
    Prometheus --> Worker
    Grafana[Grafana] --> Prometheus
```

PostgreSQL 保存业务事实、库存占用、幂等响应和待办；Redis 保存查询缓存、余票副本、短期互斥与热点统计，并为 Arq 提供任务存储。API 处理在线请求，worker 执行回收、补偿与预热。

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

缓存失效、库存同步和预订标记清理先写入事务 Outbox，提交后快速尝试执行。失败保留记录，由 worker 每 10 秒有限扫描重试，按 1～300 秒退避；每轮最多领取 1000 条，共用 5 秒预算。库存同步按票种合并，只确认本批实际锁定的待办，其他记录留待后续处理。

处理器通过版本校验、状态复核和可重复删除，支持任务重复执行与故障后恢复。

### 4. 余票展示与库存副本

`GET /api/v1/ticket-types/{ticket_type_id}/inventory` 返回供展示的 `remaining`。Redis 命中时不查询数据库；副本缺失、损坏或缓存故障时，复用有界回源预算查询并尝试修复。

库存变化随业务事务推进 `inventory_version`，Lua 拒绝低版本覆盖；版本使用定宽十进制字符串比较，避免 BIGINT 转为浮点数后的精度问题。删除票种保留版本墓碑，阻止迟到快照恢复已删除的余票。

Redis 余票副本用于展示，扣票、支付和结算判断以 PostgreSQL 为准。库存同步采用版本控制，活动查询缓存采用独立的 TTL 策略。

### 5. 规则热点、预热与观测

成功的活动详情访问写入有界分钟桶，根据访问量和增长率识别热点。Arq 每分钟第 10 秒预热默认第一页及最多 20 个热点详情，复用在线缓存的重建锁和版本校验。预热只在剩余 TTL 不足时回源，限制批次时间与回源并发，缓存故障时暂停。

API 与 worker 分别导出 Prometheus 指标，Grafana 展示成功率、分状态延迟、缓存命中、锁竞争、回源压力与预热结果。HTTP 指标使用路由模板，避免活动 ID 等无界标签；响应中的 `X-Request-ID` 与 JSON 日志关联。

## 并发验证

使用 Pytest 验证库存竞争、幂等、事务回滚和支付状态流转，通过 Locust 测量活动详情查询的吞吐与延迟。

- **库存正确性：** 核对并发预订后的有效票据、数据库库存占用与 Redis 余票。
- **查询性能：** 统计活动详情查询的成功 RPS、P95/P99 延迟及成功率。
- **后台恢复：** 观察同步待办积压，并核对压测结束后的库存与待办处理结果。

压测结果按同一部署、数据和负载口径汇总，原始记录保存在本地 `load_tests/artifacts/`。

2026-10-22 本机实测：Windows、i9-12900H、约 16GB 内存；4 个独立 API 进程、1 个 Arq worker，专用 PostgreSQL 16 / Redis 7。每个 API 数据库连接池上限 20，Redis 查询操作预算 150ms。为压测共享 IP，临时查询限流为每进程 10000/s、突发 20000，业务默认配置不变。

| 场景 | 负载 | 实测结果 |
|---|---|---|
| 活动详情查询 | 100 并发用户，配置运行 5 分钟；50 个活动，80% 流量集中于 5 个热点，零思考时间 | 成功吞吐 **1044.4 RPS**，成功响应 **P95 130ms / P99 160ms**；312038 次请求中 312034 次有效 200、4 次 503，成功率 **99.9987%** |

RPS 使用 Locust 实际统计窗口计算，P95/P99 对应成功响应。

预热链路另在正式压测前核验：真实访问识别 5 个热点后，仅将其专用缓存 TTL 调至 240 秒，等待定时 worker 自动刷新；5 个热点 TTL 恢复到 24 小时以上，后台实际回填计数增加 6（含默认列表），验证热点识别与自动预热流程。

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

### 常用接口

默认前缀为 `/api/v1`，完整参数与响应以 Swagger 为准。

| 方法与路径 | 用途 |
|---|---|
| `POST /auth/signup`、`POST /auth/login` | 注册与登录 |
| `GET /events`、`GET /events/{event_id}` | 活动列表与详情 |
| `GET /events/{event_id}/ticket-types` | 活动下的票种 |
| `GET /ticket-types/{ticket_type_id}/inventory` | 余票展示 |
| `POST /tickets` | 预订一张票 |
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

## 目录结构

```text
.
├── src/
│   ├── api/              # 路由、权限与依赖注入
│   ├── core/             # 配置、缓存策略、锁、指标与日志
│   ├── db/               # 数据库会话与 Redis 连接
│   ├── models/           # 业务表、幂等记录与 Outbox
│   ├── repositories/     # 活动数据库访问与缓存包装
│   ├── schemas/          # 请求与响应模型
│   ├── services/         # 业务事务、库存、补偿与预热
│   └── worker.py         # 延迟任务与定时任务
├── migrations/           # Alembic 迁移
├── tests/                # 单元、回归、集成与并发验收
├── load_tests/           # Locust 场景与数据准备
├── monitoring/           # Prometheus 与 Grafana 配置
├── nginx/                # 反向代理配置
└── .github/workflows/    # CI
```
