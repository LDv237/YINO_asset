# TickForge

一个接入 Binance 现货公开行情的实时高频数据流水线 demo：WebSocket 原始帧无损落盘 → L2 订单簿重建 → 因子计算 → 做市策略信号 → 模拟成交，并支持从 WAL 逐字节确定性回放与校验。

> 笔试题目：YINO ASSET 量化开发工程师（实时高频行情交易系统 demo）。
> GitHub: https://github.com/LDv237/YIKO_asset
> 设计与取舍见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)，模块接口与 WAL 格式见 [docs/INTERFACES.md](docs/INTERFACES.md)，实测与复现见 [docs/PERFORMANCE.md](docs/PERFORMANCE.md)，完整报告见 [REPORT.md](REPORT.md)。

## 核心特性

- **不丢数据**：每条 WebSocket 帧在解析前进入有界队列，由独立线程顺序写入带 CRC32 的分段 WAL；队列满时反压 socket，不静默丢包。
- **持久化契约**：正常关闭会排空队列并 flush+fsync（实测 0 丢失）；`fsync_policy=interval` 每 250ms **尝试**组提交，崩溃时未同步的尾部可能丢失，250ms 是目标间隔而非硬上界（受队列/磁盘影响）；`always` 模式每条记录 fsync 后才进入处理，延迟更高，异常终止后已 fsync 前缀仍可校验读取；`never` 只在关闭/轮转时同步。
- **状态可恢复**：按 Binance `U/u` 规则维护全深度订单簿；检测到丢包/交叉时自动进入缓冲并用 REST 快照 + 增量重放恢复。
- **可复现**：REST 快照作为 `kind=2` 事件写入 WAL；回放使用同一套流水线，因子/信号/成交 JSONL 的 SHA-256 必须与实时运行一致。
- **可观测**：`/healthz`、`/readyz`、`/status`、`/metrics`；WAL 队列、处理滞后、逐阶段延迟、订单簿状态、反压次数均可查。
- **可验证**：WAL CRC/seq/分段哈希校验、计数对账、单元/集成/混沌测试、回放摘要对比。
- **可运行**：本地 `python -m tickforge`，或 Docker / docker compose。

## 数据源

默认使用 Binance 行情专用只读端点（无需 API Key，在无法访问主站网络时仍可用）：

- WebSocket：`wss://data-stream.binance.vision/stream`
- REST 快照：`https://data-api.binance.vision`

默认订阅 6 个交易对（`BTCUSDT`、`ETHUSDT`、`SOLUSDT`、`BNBUSDT`、`XRPUSDT`、`DOGEUSDT`）的 `depth@100ms`、`aggTrade`、`bookTicker` 三条流。可在 [config/default.toml](config/default.toml) 或命令行覆盖。

题目允许自行选择公开、免费的实时高频数据源，`BTCUSDT` 是题目给出的示例而非唯一选项。本项目在**同一个币安现货公开接口**上加入了 5 个 USDT 交易对以提高 15 分钟窗口内的消息负载，BTCUSDT 始终包含在主列表中；这不是更换数据源，而是同一接口的多 symbol 订阅。

## 快速开始

要求 Python 3.11+（本机验证使用 3.14.7）。`requirements-lock.txt` 固定了运行时的全部直接/传递依赖（aiohttp 3.14.5、orjson 3.13.0、websockets 17.3、psutil 7.2.2 等）；重建环境请使用锁文件。

```bash
python -m pip install -r requirements-lock.txt

# 15 分钟实测（默认输出到 artifacts/runs/<run_id>）
python -m tickforge live --config config/default.toml --duration 900 --out artifacts

# 运行中查看健康与指标
curl http://127.0.0.1:9100/healthz
curl http://127.0.0.1:9100/metrics
```

运行结束后：

```bash
RUN_DIR=$(ls -td artifacts/runs/* | head -1)
python -m tickforge show --run "$RUN_DIR"
python -m tickforge verify --run "$RUN_DIR"
python -m tickforge replay --run "$RUN_DIR" --verify
python tools/analyze_run.py "$RUN_DIR"

# 把二进制 WAL 导出为人类可读的原始 JSONL（可选）
python tools/wal_export.py --run "$RUN_DIR" --out "$RUN_DIR/raw_events.jsonl.gz"

# 按事件类型导出 CSV（Excel/表格查看）
python tools/wal_export_csv.py --run "$RUN_DIR" --out-dir "$RUN_DIR/csv"
```

`replay --verify` 会比较实时运行与回放的：

- WAL 记录数（按 kind）；
- 订单簿同步、因子快照、信号、成交计数；
- `factors.jsonl` / `signals.jsonl` / `fills.jsonl` 的 SHA-256。

任何一项不一致都会返回非零退出码。

## 两种执行语义

- **默认：抽象做市**（`config/default.toml`）。允许负仓位/负现金，用于从零库存跑双边做市；它不是 Binance 现货账户；它是本文的性能/链路基准，不代表现货执行语义，现货执行版本见只做多配置。
- **现货只做多**（`config/spot_long_only.toml`）。禁止裸卖，卖出不超过持仓，买入受每 symbol 10,000 USDT 初始现金限制；`position<=0` 时策略不发 ask。设计与实测见 [docs/SPOT_LONG_ONLY.md](docs/SPOT_LONG_ONLY.md)。

```bash
# 现货只做多 15 分钟
python -m tickforge live --config config/spot_long_only.toml   --duration 900 --out artifacts
```

严格落盘版（每条记录 fsync 后才处理，延迟更高）：

```bash
python -m tickforge live --config config/spot_long_only_durable.toml   --duration 900 --out artifacts
```

严格版实测：272,784 帧、272,802 条 WAL、1,576 fills、0 负仓位/负现金；p50 3.444 ms / p99 5.793 ms；2 次网络重连导致 data gap，但收到的原始帧 0 丢失，订单簿通过快照恢复。异常终止恢复实验见 `docs/VERIFICATION.md`。

## Docker

> **当前状态：Docker Desktop 已安装，但本机 WSL2 虚拟化组件不完整，Docker 引擎无法启动；镜像尚未实际构建/运行。** 提交前必须在有 Docker 的环境执行 [docs/DOCKER_VERIFICATION.md](docs/DOCKER_VERIFICATION.md) 中的命令，并保存构建日志、容器 summary、verify/replay 证据；一键脚本为 [scripts/docker_verify.sh](scripts/docker_verify.sh)。

```bash
docker compose build
docker compose up --abort-on-container-exit

# 回放（容器内）
docker compose run --rm tickforge replay --run /data/runs/<run_id> --verify
```

镜像内默认 `TICKFORGE_HTTP_HOST=0.0.0.0`、输出目录 `/data`；`docker-compose.yml` 将 `./artifacts` 挂载到 `/data`。如构建网络受限，可设置 `PIP_INDEX_URL` 指向镜像站。

## 目录

```text
tickforge/           核心包（feed/store/book/factor/strategy/exec/runtime/replay/metrics/server）
tests/               单元、集成、混沌与端到端回放测试
tools/               运行分析、样本裁剪
scripts/             15 分钟运行、回放校验、Docker 运行
config/default.toml  默认配置
docs/                架构、接口、性能、验证、AI 协作、运行手册
REPORT.md            笔试交付报告
artifacts/           运行产物（WAL、可读 JSONL/CSV、派生 JSONL、日志、摘要、清单）
```

## 测试

```bash
python -m pip install -r requirements-dev-lock.txt
python -m pytest -q
```

测试包含：订单簿序列/快照恢复/随机对拍、WAL 轮转/CRC/截断/fsync、因子手算、策略与模拟执行、WebSocket 断线重连与 REST 重试、指标并发与 HTTP、以及“本地模拟交易所 → 实时引擎 → WAL → 回放摘要一致”的端到端测试。

## 15 分钟实测摘要

> 完整数据、延迟分位数、资源占用和复现命令见 [docs/PERFORMANCE.md](docs/PERFORMANCE.md)。

| 指标 | 结果 |
|---|---|
| 测量窗口 | 900.116 s |
| WebSocket 原始帧 | 388,388 |
| WAL 原始记录 / 字节 | 388,394 / 92.61 MiB |
| 处理事件 / 信号 / 成交 | 388,394 / 43,309 / 10,066 |
| 端到端延迟 p50 / p99 / p999 | 0.512 ms / 2.435 ms / 27.554 ms |
| 原始帧丢失 / 未处理（正常关闭） | 0 / 0 |
| 回放摘要一致性 | 因子/信号/成交 SHA-256 全部一致 |
| 交易活跃度 | 抽象/interval 与只做多/interval 在 07:00–08:00 UTC 桶排名 3/47；严格版在 08:00–09:00 UTC 桶排名 4/47（47 个完整小时） |

## 已知局限

- 公开行情 WebSocket 不提供断线补发；断线期间真实成交无法补齐，缺口会记录在 `summary.json` 的 `data_gaps`；
- Python 单进程不是生产级 HFT，延迟数据用于工程验证而非与 C++/FPGA 系统比较；
- 模拟撮合未建模排队位置、撤单延迟与市场冲击；
- `Simulator` 是抽象记账器，允许负仓位/负现金，**不代表 Binance 现货账户的可执行性**，也不建模借币、保证金或强平；
- 未接入私有下单 API 与真实风控前置。
