# Differential-Privacy Budget Ledger

基于 **Python · FastAPI · SQLite · asyncio** 的差分隐私预算账本服务，支持
Docker Compose 一键启动。

客户端为数据集登记 epsilon / delta 总预算（cap），然后提交带
`request_id`、`query_type`、`epsilon`、`delta` 的分析申请；服务在一组
Rényi DP（RDP）orders 上累加各次申请的组合消耗，在并发申请下**原子地**
决定预留成功或拒绝超预算，并对重复 `request_id` 做**幂等**处理。提供
预算明细与剩余额度查询。

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
| `GET` | `/datasets/{dataset_id}/budget` | 仅预算明细（消耗 / 剩余 / RDP 曲线） |
| `POST` | `/datasets/{dataset_id}/requests` | 提交分析申请（幂等） |
| `GET` | `/datasets/{dataset_id}/requests` | 全部申请记录（含 granted / rejected） |
| `GET` | `/health` | 健康检查 |

`dataset_id` 允许字母、数字、`.`、`_`、`-`，长度 ≤ 200。

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
  "epsilon_remaining": 0.796064,
  "rdp_best_order": 8192.0,
  "granted_requests": 1,
  "rdp_curve": {
    "2.0": 18.62, "4.0": 6.34, "8.0": 2.83, "16.0": 1.43,
    "32.0": 0.79, "64.0": 0.50, "128.0": 0.35, "512.0": 0.21,
    "4096.0": 0.204
  }
}
```

### 4.5 查询申请明细

```bash
curl http://localhost:8000/datasets/sales-2026/requests
```

按受理顺序返回所有申请（granted 与 rejected），每条含申请参数、裁定、
理由、申请前后累计 epsilon 与时间戳。

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

4. 准入判定：把**当前累计曲线 + 本次申请**投影后得到 `ε_projected`，
   当且仅当 `ε_projected ≤ epsilon_cap + EPSILON_TOLERANCE` 时预留成功。

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

* 每个写操作（登记、提交申请）在进程内 `asyncio.Lock` 保护下打开一个
  SQLite **`BEGIN IMMEDIATE`** 事务：「查重 → 重算组合消耗 → 判定 →
  插入记录」在同一个事务临界区内完成，并发申请被串行化，
  **不可能出现两个申请同时看到同一旧余额而双双超支**；
* SQLite 启用 **WAL** 与 `busy_timeout`：读不阻塞写，写冲突排队等待
  而非立即报错；`(dataset_id, request_id)` 上有 `UNIQUE` 约束作为最终
  防线；
* 拒绝不写入任何消耗，只留一条 `rejected` 记录，因此并发申请人中
  “先到先得、额满即拒”。

---

## 7. 运行测试

```bash
pip install -r requirements-dev.txt
PYTHONPATH=. pytest -q
```

测试覆盖：登记/重复登记、授予/拒绝、拒绝不留痕、纯 ε-DP 精确可加、
RDP 数学性质、幂等回放（含拒绝回放）、`request_id` 冲突 409、缺失
数据集 404，以及 60 路并发申请不超预算、20 路重复 `request_id` 并发仅
一条被创建。

---

## 8. 项目结构

```
.
├── app/
│   ├── __init__.py
│   ├── config.py      # 环境变量配置
│   ├── rdp.py         # Rényi DP orders、组合与 (ε,δ) 转换
│   ├── schemas.py     # Pydantic 请求/响应模型
│   ├── db.py          # SQLite 连接、schema、数据访问
│   ├── ledger.py      # 原子准入控制 + 幂等业务逻辑
│   └── main.py        # FastAPI 路由与异常处理
├── tests/             # pytest 端到端 + RDP 单元测试
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
└── README.md
```
