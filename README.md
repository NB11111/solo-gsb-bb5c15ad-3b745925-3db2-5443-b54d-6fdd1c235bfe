# Differential-Privacy Budget Ledger

基于 **Python · FastAPI · SQLite · asyncio** 的差分隐私预算账本服务，支持
Docker Compose 一键启动。

客户端为数据集登记 epsilon / delta 总预算（cap），然后提交带
`request_id`、`query_type`、`epsilon`、`delta` 的分析申请；服务在一组
Rényi DP（RDP）orders 上累加各次申请的组合消耗，在并发申请下**原子地**
决定预留成功或拒绝超预算，并对重复 `request_id` 做**幂等**处理。提供
预算明细与剩余额度查询。

此外支持**预算预留（reservation）**：客户端以 `reservation_id`、`epsilon`、
`delta`、TTL 创建预留，系统在同一数据集上原子检查「已授权消耗 + 未过期
预留」后决定 granted/rejected；预留可在有效期内**确认（confirm）**为一
次正式分析申请（沿用 RDP 核算），也可**释放（release）**取消；过期预留
自动让出额度。预留的创建、确认、释放同样幂等且在并发下不超预算。

---

## 1. 快速启动（Docker Compose）

```bash
# 构建并后台启动
docker compose up -d --build

# 查看日志
docker compose logs -f

# 停止
docker compose down

# 停止并删除数据库卷（清空所有账本数据）
docker compose down -v
```

启动后：

| 项目 | 地址 |
| --- | --- |
| API Base URL | `http://localhost:8000` |
| 健康检查 | `GET http://localhost:8000/health` |
| Swagger 交互文档 | `http://localhost:8000/docs` |
| ReDoc 文档 | `http://localhost:8000/redoc` |
| OpenAPI Schema | `http://localhost:8000/openapi.json` |

宿主机端口可用环境变量覆盖：

```bash
LEDGER_PORT=18000 docker compose up -d --build
# 然后访问 http://localhost:18000
```

数据库文件持久化在命名卷 `dp-ledger-data` 内（容器内 `/data/ledger.db`，
SQLite WAL 模式）。

---

## 2. 本地（无 Docker）运行

需要 Python 3.11+：

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 数据库文件路径（默认 /data/ledger.db，本地运行建议改成当前目录）
export DATABASE_PATH=./ledger.db
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

---

## 3. 配置

所有配置通过环境变量注入（见 `docker-compose.yml`）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_PATH` | `/data/ledger.db` | SQLite 数据库文件路径，所在目录需可写 |
| `HOST` | `0.0.0.0` | uvicorn 监听地址 |
| `PORT` | `8000` | 容器内监听端口 |
| `LEDGER_PORT` | `8000` | Compose 映射到宿主机的端口 |
| `SQLITE_BUSY_TIMEOUT_MS` | `10000` | SQLite 写锁等待超时（毫秒） |
| `EPSILON_TOLERANCE` | `1e-12` | 预算比较的浮点容差，避免累计舍入误拒 |

---

## 4. API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/datasets/{dataset_id}` | 登记数据集及 epsilon/delta 上限 |
| `GET` | `/datasets/{dataset_id}` | 数据集信息 + 预算明细 |
| `GET` | `/datasets/{dataset_id}/budget` | 仅预算明细（消耗 / 预留 / 可用 / RDP 曲线） |
| `POST` | `/datasets/{dataset_id}/requests` | 提交分析申请（幂等） |
| `GET` | `/datasets/{dataset_id}/requests` | 全部申请记录（含 granted / rejected） |
| `POST` | `/datasets/{dataset_id}/reservations` | 创建预算预留（幂等，带 TTL） |
| `GET` | `/datasets/{dataset_id}/reservations` | 全部预留记录（含各状态与时间戳） |
| `GET` | `/datasets/{dataset_id}/reservations/{reservation_id}` | 单个预留详情 |
| `POST` | `/datasets/{dataset_id}/reservations/{reservation_id}/confirm` | 将预留确认为一次分析申请 |
| `POST` | `/datasets/{dataset_id}/reservations/{reservation_id}/release` | 释放预留（幂等取消） |
| `GET` | `/health` | 健康检查 |

`dataset_id` 允许字母、数字、`.`、`_`、`-`，长度 ≤ 200；`reservation_id`
同样限制为该字符集（需能作为 URL 路径段寻址）。

### 4.1 登记数据集

```bash
curl -X POST http://localhost:8000/datasets/sales-2026 \
  -H 'Content-Type: application/json' \
  -d '{
    "epsilon_cap": 1.0,
    "delta_cap": 1e-6,
    "description": "2026 销售数据集"
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `epsilon_cap` | float | `> 0`，总 epsilon 预算 |
| `delta_cap` | float | `>= 1e-20, < 1`，总 delta；同时作为 RDP 转回 `(ε,δ)` 时的目标 δ |
| `description` | string | 可选，≤ 2000 字符 |

重复登记同一 `dataset_id` 返回 `409 dataset_already_exists`。

### 4.2 提交分析申请

```bash
curl -X POST http://localhost:8000/datasets/sales-2026/requests \
  -H 'Content-Type: application/json' \
  -d '{
    "request_id": "req-20260923-0001",
    "query_type": "gaussian_mean",
    "epsilon": 0.2,
    "delta": 1e-8
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `request_id` | string | 必填，客户端生成的幂等键，**同一数据集内唯一** |
| `query_type` | string | 必填，查询类型标签（如 `gaussian_mean`、`histogram`） |
| `epsilon` | float | `> 0`，本次申请的 epsilon |
| `delta` | float | `>= 0, < 1`；`0` 表示纯 ε-DP |

返回 `201` 与裁定结果：

```json
{
  "dataset_id": "sales-2026",
  "request_id": "req-20260923-0001",
  "query_type": "gaussian_mean",
  "epsilon": 0.2,
  "delta": 1e-8,
  "allowed": true,
  "status": "granted",
  "reason": null,
  "epsilon_before": 0.0,
  "epsilon_after": 0.203936,
  "epsilon_remaining": 0.796064,
  "rdp_best_order": 8192.0,
  "idempotent_replay": false,
  "created_at": "2026-09-23T01:10:16.563790+00:00"
}
```

* 预算内 → `allowed=true, status="granted"`，预算被原子预留；
* 超预算 → 仍返回 `201`（请求被正常受理并留痕），但
  `allowed=false, status="rejected"`，`reason` 说明投影消耗，
  **不占用任何预算**；
* 数据集不存在 → `404`。

### 4.3 幂等语义

* 相同 `(dataset_id, request_id)` + **相同负载**重复提交：返回 `200`、
  与第一次完全一致的裁定，`idempotent_replay=true`，预算**只扣一次**；
* 相同 `request_id` 但 `query_type` / `epsilon` / `delta` 不同：
  返回 `409 request_id_conflict`；
* 被拒绝的申请同样幂等：重复提交返回最初的拒绝结果。

### 4.4 查询预算明细与剩余额度

```bash
curl http://localhost:8000/datasets/sales-2026/budget
```

```json
{
  "epsilon_cap": 1.0,
  "delta_cap": 1e-6,
  "epsilon_consumed": 0.203936,
  "epsilon_reserved": 0.0,
  "epsilon_committed": 0.203936,
  "epsilon_remaining": 0.796064,
  "epsilon_available": 0.796064,
  "rdp_best_order": 8192.0,
  "granted_requests": 1,
  "active_reservations": 0,
  "rdp_curve": {
    "2.0": 18.62, "4.0": 6.34, "8.0": 2.83, "16.0": 1.43,
    "32.0": 0.79, "64.0": 0.50, "128.0": 0.35, "512.0": 0.21,
    "4096.0": 0.204
  }
}
```

字段口径：

| 字段 | 含义 |
| --- | --- |
| `epsilon_consumed` | 仅**已授权申请**的组合消耗 |
| `epsilon_reserved` | 仅**未过期活跃预留**的组合占用 |
| `epsilon_committed` | 已授权 + 活跃预留的**联合组合**值，准入判定以此对照 `epsilon_cap` |
| `epsilon_remaining` | `epsilon_cap − epsilon_consumed` |
| `epsilon_available` | `epsilon_cap − epsilon_committed`，新申请/新预留实际可动用额度 |

> 注意 `committed ≠ consumed + reserved`：RDP 组合是在同一条累计曲线上
> 取最优 order，分开各自组合再相加会重复计入转换开销，因此准入判定
> 一律使用联合组合值 `epsilon_committed`。

### 4.5 查询申请明细

```bash
curl http://localhost:8000/datasets/sales-2026/requests
```

按受理顺序返回所有申请（granted 与 rejected），每条含申请参数、裁定、
理由、申请前后累计 epsilon 与时间戳。

### 4.6 预算预留（reservation）

预留用于「先锁住一部分预算、稍后再决定要不要真正执行分析」。预留与
已授权申请**共享同一预算池**：任何准入判定（新申请或新预留）都把
「已授权消耗 + 未过期活跃预留」一起投影核算。

#### 创建预留

```bash
curl -X POST http://localhost:8000/datasets/sales-2026/reservations \
  -H 'Content-Type: application/json' \
  -d '{
    "reservation_id": "rsv-20260923-0001",
    "epsilon": 0.3,
    "delta": 1e-8,
    "ttl_seconds": 600
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `reservation_id` | string | 必填，幂等键，**同一数据集内唯一**，字符集同 `dataset_id` |
| `epsilon` | float | `> 0`，预留的 epsilon |
| `delta` | float | `>= 0, < 1`；`0` 表示纯 ε-DP |
| `ttl_seconds` | float | `(0, 31536000]`，预留存活秒数，到期自动让出额度 |

返回 `201` 与裁定结果（结构与申请裁定一致，另含 `ttl_seconds` 与
`expires_at`）：

```json
{
  "dataset_id": "sales-2026",
  "reservation_id": "rsv-20260923-0001",
  "epsilon": 0.3,
  "delta": 1e-8,
  "ttl_seconds": 600.0,
  "allowed": true,
  "status": "active",
  "reason": null,
  "epsilon_before": 0.203936,
  "epsilon_after": 0.506184,
  "epsilon_remaining": 0.493816,
  "rdp_best_order": 8192.0,
  "idempotent_replay": false,
  "created_at": "2026-09-23T01:20:00.000000+00:00",
  "expires_at": "2026-09-23T01:30:00.000000+00:00"
}
```

* 预算内 → `allowed=true, status="active"`，额度被原子锁定；
* 超预算 → 仍返回 `201`，`allowed=false, status="rejected"`，**不占用
  任何预算**；被拒绝的预留是终态，不能确认或释放；
* `epsilon_before/after` 记录的是「已授权 + 活跃预留」联合组合值在
  本次判定前后的变化。

#### 预留的状态机

```
            create
              │
        ┌─────┴─────┐
        ▼           ▼
     active      rejected（终态，未占用预算）
        │
        ├─ confirm ──► confirmed（终态，转为一条 granted 分析申请）
        ├─ release ──► released（终态，额度释放）
        └─ TTL 到期 ─► expired（终态，额度自动释放）
```

#### 确认预留（转为分析申请）

```bash
curl -X POST http://localhost:8000/datasets/sales-2026/reservations/rsv-20260923-0001/confirm \
  -H 'Content-Type: application/json' \
  -d '{"request_id": "req-20260923-0007", "query_type": "gaussian_mean"}'
```

* 请求体可省略：`request_id` 默认取 `reservation_id`，`query_type`
  默认 `reserved_analysis`；
* 确认把预留的 `(epsilon, delta)` 原样转为一条 `granted` 分析申请并
  沿用 RDP 核算：因为预留本身已占着这部分额度，转换是**预算中性**的，
  不需要重新准入，也绝不会超预算；
* 成功返回 `201` 与申请裁定（含 `confirmed_at`）；该申请会出现在
  `GET /requests` 中，与普通申请一致；
* `request_id` 与已有申请冲突 → `409 request_id_conflict`（预留保持
  active，可换个 `request_id` 重试）；
* 对已 released / expired / rejected 的预留确认 → `409`
  （`reservation_already_released` / `reservation_expired` /
  `reservation_not_active`）。

#### 释放预留

```bash
curl -X POST http://localhost:8000/datasets/sales-2026/reservations/rsv-20260923-0001/release
```

* 对 active 预留：释放额度，返回 `200` 与 `status="released"`；
* 重复释放：幂等返回 `200`、`idempotent_replay=true`，`released_at`
  与首次一致；
* 对已 confirmed 的预留：`409 reservation_already_confirmed`。

#### 预留的幂等与过期语义

* 相同 `(dataset_id, reservation_id)` + **相同负载**（`epsilon`、`delta`、
  `ttl_seconds` 均一致）重复创建：返回 `200`、首次的裁定，
  `idempotent_replay=true`，额度只锁一次；负载任一字段不同 →
  `409 reservation_id_conflict`；
* 重复确认：用同样的（显式或默认）`request_id` / `query_type` 重放返回
  `200` 与首次结果；参数不一致 → `409 reservation_id_conflict`；
* **过期不占用额度**：`expires_at` 一过，预算查询、新建预留、新申请
  的准入判定都不再计入该预留；状态在读取时即时反映为 `expired`，并在
  下一次写操作时持久化落库；
* 所有状态与时间（`created_at` / `expires_at` / `confirmed_at` /
  `released_at`）都可通过
  `GET /datasets/{id}/reservations[/{reservation_id}]` 查询。

---

## 5. RDP 组合会计（核算口径）

1. 每个申请被视为一个 `(ε, δ)`-DP 机制，转换为所有 Rényi order
   `α ∈ {1.1, 1.2, …, 10.9, 11, …, 63, 128, 256, 512, 1024, 2048, 4096, 8192}`
   上的 RDP 曲线：

   * 近似 DP（`δ > 0`）：`ε_rdp(α) = ε + log(1/δ) / (α − 1)`；
   * 纯 ε-DP（`δ = 0`）：`ε_rdp(α) = ε`（常数曲线）。

2. 组合（串行/并行同理）在每个 order 上逐点相加：
   `ε_rdp_total(α) = Σ_i ε_rdp_i(α)`。

3. 在数据集的目标 δ（`delta_cap`）处转换回 `(ε', δ)`-DP：

   ```
   ε'(δ) = min_α [ ε_rdp_total(α)
                   + (log((α−1)/α) + log((1−δ)/δ)) / (α−1) ]
   ```

4. 准入判定：把**当前已授权申请 + 未过期活跃预留 + 本次申请/预留**投影
   后得到 `ε_projected`，当且仅当
   `ε_projected ≤ epsilon_cap + EPSILON_TOLERANCE` 时预留成功。

特殊情形：

* 全部已授权申请都是纯 ε-DP 时，直接采用精确的基本组合
  `ε_total = Σ ε_i`，不收取 RDP 转换的保守余量；
* 纯 DP 与近似 DP 混合时，纯 ε 之和作为常数偏移叠加到 RDP 曲线上再统一
  转换。

> 该口径使用的是教科书式、与机制无关的保守上界（Mironov 2017 的
> (ε,δ)-DP→RDP→(ε,δ) 界），不利用高斯机制等的更紧会计；优点是对任意
> `query_type` 都成立、实现简单可审计。`rdp_curve` 字段暴露各 order 的
> 累计 RDP 值，便于核对。

---

## 6. 并发与原子性

* 每个写操作（登记、提交申请、创建/确认/释放预留）在进程内
  `asyncio.Lock` 保护下打开一个 SQLite **`BEGIN IMMEDIATE`** 事务：
  「查重 → 重算组合消耗 → 判定 → 插入/更新记录」在同一个事务临界区内
  完成，并发操作被串行化，**不可能出现两个操作同时看到同一旧余额而
  双双超支**；
* SQLite 启用 **WAL** 与 `busy_timeout`：读不阻塞写，写冲突排队等待
  而非立即报错；`(dataset_id, request_id)` 与
  `(dataset_id, reservation_id)` 上均有 `UNIQUE` 约束作为最终防线；
* 拒绝不写入任何消耗，只留一条 `rejected` 记录，因此并发申请人中
  “先到先得、额满即拒”；
* 预留的确认在同一事务内「插入 granted 申请 + 更新预留状态」，与并发
  的释放/确认互斥：同一预留并发 confirm 与 release 时恰有一个成功，
  另一个收到 `409`，预算不变式 `committed ≤ cap` 始终保持；
* 过期清扫（active → expired）只在写事务内发生；只读接口按当前时间
  即时计算过期状态，不产生写操作。

## 6.1 数据库迁移

Schema 采用版本化迁移：当前版本存于 SQLite `PRAGMA user_version`，
启动时（`init_db`）按序应用 `app/db.py` 中 `MIGRATIONS` 里尚未执行的
迁移（v1 = 基础账本表，v2 = 预留表）。旧版本创建的数据库文件会在启动
时自动升级，无需手工干预；所有迁移语句幂等，可安全重复执行。

---

## 7. 运行测试

```bash
pip install -r requirements-dev.txt
PYTHONPATH=. pytest -q
```

测试覆盖：登记/重复登记、授予/拒绝、拒绝不留痕、纯 ε-DP 精确可加、
RDP 数学性质、幂等回放（含拒绝回放）、`request_id` 冲突 409、缺失
数据集 404，以及 60 路并发申请不超预算、20 路重复 `request_id` 并发仅
一条被创建。预留相关：创建/超预算拒绝、幂等回放与负载冲突 409、预留
占用额度并拦截新申请与新预留、确认转换为申请且预算中性、确认/释放
幂等、非法状态迁移 409、TTL 过期自动释放额度、并发预留下不超预算、
并发重复 `reservation_id` 仅一条生效、并发确认仅生成一条申请、
confirm/release 竞态安全、申请与预留混合并发不超预算，以及
`user_version` 版本化迁移（含从 v1 旧库升级）。

---

## 8. 项目结构

```
.
├── app/
│   ├── __init__.py
│   ├── config.py      # 环境变量配置
│   ├── rdp.py         # Rényi DP orders、组合与 (ε,δ) 转换
│   ├── schemas.py     # Pydantic 请求/响应模型
│   ├── db.py          # SQLite 连接、版本化迁移（user_version）、数据访问
│   ├── ledger.py      # 原子准入控制 + 预留生命周期 + 幂等业务逻辑
│   └── main.py        # FastAPI 路由与异常处理
├── tests/             # pytest 端到端（含预留）+ RDP 单元测试
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
└── README.md
```
