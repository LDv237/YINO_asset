# TickForge

TickForge 是我这次笔试交的实时高频数据流水线 demo，行情接 Binance 现货公开接口。WebSocket 原始帧先进入 WAL 队列，再进入处理队列；独立写线程负责异步写入分段 WAL。默认 `interval` 按目标间隔组提交，处理不等待写入或 fsync 完成，正常关闭时 flush+fsync；`always` 则等待 fsync 完成后才进入处理队列。处理队列再做 L2 订单簿重建、因子计算、做市信号和模拟成交；一次跑完可以从 WAL 做确定性回放，并把回放结果和运行目录里的派生文件逐项对上。

本次提交的**主结果**是最终代码的 15 分钟**现货 live 单源 run** `20261021T162501Z-9821`：650,532 条原始帧、47,127 个信号、6,437 笔模拟成交，原始帧 0 丢失、0 data gap、0 订单簿 resync、0 负仓位/0 负现金，回放三项 SHA-256 一致。3049 抽象做市、53b5 历史基准、6db8 严格落盘只作为报告里的对照，完整数据在 GitHub Release。

> 笔试题目：YINO ASSET 量化开发工程师（实时高频行情交易系统 demo）。
> GitHub：https://github.com/LDv237/YINO_asset
> 先看 [`交付说明.md`](交付说明.md) 开头的“评审导览（60 秒）”；完整历史数据见 [Release delivery-20261020](https://github.com/LDv237/YINO_asset/releases/tag/delivery-20261020)。

## 这个项目重点解决什么

- **原始数据不丢**：WebSocket 帧在解析前进入有界队列，由独立线程顺序写入带 CRC32 的分段 WAL；REST 快照以规范化 JSON 作为 `kind=2` 事件写入。队列满了反压 socket，正常关闭排空队列再 flush+fsync。主结果 0 丢失。
- **状态可恢复**：订单簿按 Binance `U/u` 规则维护；检测到丢包或交叉时进入缓冲，用 REST 快照加增量重放恢复。
- **结果可复现**：回放走同一套 Pipeline，`factors` / `signals` / `fills` 三个 SHA-256 必须和运行目录里的 derived 一致。主结果 live 同源。
- **可观测**：`/healthz`、`/readyz`、`/status`、`/metrics` 暴露 WAL 队列、处理滞后、逐阶段延迟、订单簿状态和反压次数。

更完整的设计在 [`docs/系统架构.md`](docs/系统架构.md)，事件/WAL/模块 API 在 [`docs/接口设计.md`](docs/接口设计.md)。

## 行情数据从哪来

Binance 公开只读端点，不需要 API Key：

- WebSocket：`wss://data-stream.binance.vision/stream`
- REST 快照：`https://data-api.binance.vision`

订阅 6 个 USDT 交易对（BTC、ETH、SOL、BNB、XRP、DOGE）的 `depth@100ms`、`aggTrade`、`bookTicker`；BTCUSDT 始终在列，多交易对是为了把 15 分钟窗口的消息量撑起来。

## 怎么跑

环境 Python 3.11+，本机验证用 3.14.7；`requirements-lock.txt` 固定运行时依赖。

```bash
python -m pip install -r requirements-lock.txt

# 新跑一次 15 分钟实时行情（需要交易所网络）
python -m tickforge live --config config/default.toml --duration 900 --out artifacts

# 运行中查看健康与指标
curl http://127.0.0.1:9100/healthz
curl http://127.0.0.1:9100/metrics
```

> 路径约定：**本邮件包不含完整 `artifacts/`**（约 1,011 MiB，7 个历史 run 在 Release 的 `YINO_artifacts_complete_20261020.zip`）。文档里的 `artifacts/runs/<run_id>` 指新跑 live 或 Release/仓库；包内主结果在 `运行材料/01_主结果_现货_9821/`，完整数据在 `run_data.tar.xz`。详见 [`artifacts/README.md`](artifacts/README.md)。

包内主结果先解压再验证：

```bash
python scripts/extract_main_run.py 运行材料/01_主结果_现货_9821
python -m tickforge verify --run 运行材料/01_主结果_现货_9821
python -m tickforge replay --run 运行材料/01_主结果_现货_9821 \
  --out 运行材料/01_主结果_现货_9821-replay --verify
```

`replay --verify` 比对 WAL 记录数、订单簿同步/因子/信号/成交计数和三个 JSONL 的 SHA-256；不一致退出码非零。

离线样本（约 6 秒）：

```bash
python -m tickforge replay --run samples/demo_run --out samples/demo_run-replay --verify
```

## 两套执行语义

**主结果用 `config/spot_long_only.toml`**：禁止裸卖、卖出不超过持仓、买入受每 symbol 10,000 USDT 现金限制；本次 650,532 帧里所有 symbol position/cash ≥ 0。完整设计和对照见 [`docs/现货语义设计.md`](docs/现货语义设计.md)。

默认抽象做市（`config/default.toml`）允许负仓位/负现金，是执行语义对照；最终代码的 3049 抽象 run 摘要放在 `运行材料/02_对照_抽象做市_3049/`，完整 run 在 Release。

`config/spot_long_only_durable.toml` 增加 `fsync=always`；严格落盘的完整数字和边界见 [`docs/性能实测.md`](docs/性能实测.md)。

## Docker

本机 Docker Desktop 的 WSL2 组件不完整，引擎起不来；已在 GitHub Actions 干净环境用最终代码完成 build、60 秒 live、verify、replay，CI 通过。镜像包 `tickforge-0.1.0.tar.gz` 在 Release，`docker load` 后可直接运行。复跑方法见 [`docs/Docker构建验证.md`](docs/Docker构建验证.md)。

## 目录与测试

```text
tickforge/         核心包（feed/store/book/factor/strategy/exec/runtime/replay/metrics/server）
tests/             单元、集成、混沌与端到端回放测试（276 passed）
tools/ scripts/    运行分析、导出、追溯、样本裁剪、15 分钟运行和 Docker 校验
config/            默认、现货、严格落盘三套配置
docs/              架构、接口、性能、验证、开发过程、AI 协作、依赖、运行手册
samples/demo_run/  20,000 条 WAL 的离线样本（来自 53b5，仅验证 replay 机制）
运行材料/          主结果 20261021T162501Z-9821/、对照_抽象做市_3049/、验证证据/
交付说明.md        提交说明（开头是评审导览）
笔试报告.md        完整报告
```

```bash
python -m pip install -r requirements-dev-lock.txt
python -m pytest -q
```

测试覆盖订单簿序列/随机对拍、WAL 轮转/CRC/截断/fsync、因子手算、策略与模拟执行、断线重连和 REST 重试、指标/HTTP，以及“本地模拟交易所 → 实时引擎 → WAL → 回放摘要一致”的端到端链路。完整结果 **276 passed**。

## 15 分钟主结果

`20261021T162501Z-9821`，900.037 s，日志/信号/成交/性能/回放全部来自同一次 live：

| 指标 | 结果 |
|---|---|
| 原始帧 / WAL 记录 | 650,532 / 650,542 |
| 信号 / fills | 47,127 / 6,437 |
| depth / aggTrade / bookTicker | 47,830 / 43,593 / 559,109 |
| depth e2e p50 / p99 | 0.609 / 2.897 ms |
| 聚合 e2e p50 / p99 | 0.724 / 2.435 ms（bookTicker 约占 86.0%） |
| 原始帧丢失 / data gap / resync | 0 / 0 / 0 |
| 负仓位 / 负现金 | 0 / 0 |
| 回放 | `verified=true`、`deterministic=true`，三项 SHA 一致 |

延迟从 `ws.recv()` 返回后开始计时，不含网络/内核缓冲，只证明接收后的处理延迟；完整分位数和资源数据见 [`docs/性能实测.md`](docs/性能实测.md)。

## 已知的局限

公开 WS 不自动补发，本 demo 尚未实现 REST 补采；Python 单进程不是生产级 HFT；`Simulator` 是抽象记账器，主结果用现货配置、所有 position/cash ≥ 0，但仍不是真实交易所账户；3049 抽象对照允许负仓位/负现金。成交用 `@aggTrade` 而非逐笔 `@trade`。完整边界见 [`交付说明.md`](交付说明.md) §6 和 [`docs/正确性与可靠性验证.md`](docs/正确性与可靠性验证.md)。

MIT，见 [LICENSE](LICENSE)。
