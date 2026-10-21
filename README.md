# TickForge

TickForge 是我这次笔试交的实时高频数据流水线 demo，行情接 Binance 现货公开接口。WebSocket 原始帧先进入 WAL 队列，再进入处理队列；独立写线程负责异步写入分段 WAL。默认 `interval` 按目标间隔组提交，处理不等待写入或 fsync 完成，正常关闭时 flush+fsync；`always` 则等待 fsync 完成后才进入处理队列。处理队列再做 L2 订单簿重建、因子计算、做市信号和模拟成交；一次跑完可以从 WAL 做确定性回放，并把回放结果和运行目录里的派生文件逐项对上。

本次提交的**主结果**是 15 分钟 live 单源 run `20261021T075925Z-3049`：303,784 条原始帧、41,470 个信号、8,828 笔模拟成交，原始帧 0 丢失、0 data gap、0 订单簿 resync，回放三项 SHA-256 一致。53b5、6db8、同一 WAL 只做多只在报告里作为对照表，完整数据在 GitHub Release。

> 笔试题目：YINO ASSET 量化开发工程师（实时高频行情交易系统 demo）。
> GitHub：https://github.com/LDv237/YINO_asset
> 先看 [`交付说明.md`](交付说明.md) 开头的“评审导览（60 秒）”；完整历史数据见 [Release delivery-20261020](https://github.com/LDv237/YINO_asset/releases/tag/delivery-20261020)。

## 这个项目重点解决什么

- **原始数据不丢**：WebSocket 帧在解析前进入有界队列，由独立线程顺序写入带 CRC32 的分段 WAL；队列满了反压 socket，正常关闭排空队列再 flush+fsync。主结果 0 丢失。
- **状态可恢复**：订单簿按 Binance `U/u` 规则维护；检测到丢包或交叉时进入缓冲，用 REST 快照加增量重放恢复。
- **结果可复现**：回放走同一套 Pipeline，`factors` / `signals` / `fills` 三个 SHA-256 必须和运行目录里的 derived 一致。主结果 live 同源；历史基准 53b5 是同一份 WAL 重算。
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

> 路径约定：**本邮件包不含完整 `artifacts/`**（约 1,011 MiB，7 个 run 在 Release 的 `YINO_artifacts_complete_20261020.zip`）。文档里的 `artifacts/runs/<run_id>` 指新跑 live 或 Release/仓库；包内主结果在 `运行材料/20261021T075925Z-3049/`，不需要找 `artifacts/`。详见 [`artifacts/README.md`](artifacts/README.md)。

包内主结果已经就绪，直接验证：

```bash
python -m tickforge verify --run 运行材料/20261021T075925Z-3049
python -m tickforge replay --run 运行材料/20261021T075925Z-3049 \
  --out 运行材料/20261021T075925Z-3049-replay --verify
```

`replay --verify` 比对 WAL 记录数、订单簿同步/因子/信号/成交计数和三个 JSONL 的 SHA-256；不一致退出码非零。

离线样本（约 6 秒）：

```bash
python -m tickforge replay --run samples/demo_run --out samples/demo_run-replay --verify
```

## 两套执行语义

默认抽象做市（`config/default.toml`）允许负仓位/负现金，用于从零库存跑双边做市；它是链路/性能基准，不代表 Binance 现货账户。主结果 3049 用的就是这套配置。

`config/spot_long_only.toml` 禁止裸卖、卖出不超过持仓、买入受 10,000 USDT/symbol 初始现金限制。同一份 53b5 WAL 的只做多对照是 3,077 fills、0 负仓位/负现金，见 [`docs/现货只做多设计.md`](docs/现货只做多设计.md)。

严格落盘用 `config/spot_long_only_durable.toml`（`fsync=always`）；15 分钟样本 6db8 的完整数字和网络缺口边界见 `交付说明.md` §6 / Release。

## Docker

本机 Docker Desktop 的 WSL2 组件不完整，引擎起不来；已在 GitHub Actions 干净环境完成 build、60 秒 live、verify、replay，CI 通过。镜像包 `tickforge-0.1.0.tar.gz` 在 Release，`docker load` 后可直接运行。复跑方法见 [`docs/Docker构建验证.md`](docs/Docker构建验证.md)。

## 目录与测试

```text
tickforge/         核心包（feed/store/book/factor/strategy/exec/runtime/replay/metrics/server）
tests/             单元、集成、混沌与端到端回放测试（275 passed）
tools/ scripts/    运行分析、导出、追溯、样本裁剪、15 分钟运行和 Docker 校验
config/            默认、只做多、严格落盘三套配置
docs/              架构、接口、性能、验证、开发过程、AI 协作、依赖、运行手册
samples/demo_run/  20,000 条 WAL 的离线样本（来自 53b5，仅验证 replay 机制）
运行材料/          主结果 20261021T075925Z-3049/ 和 验证证据/
交付说明.md        提交说明（开头是评审导览）
笔试报告.md        完整报告
```

```bash
python -m pip install -r requirements-dev-lock.txt
python -m pytest -q
```

测试覆盖订单簿序列/随机对拍、WAL 轮转/CRC/截断/fsync、因子手算、策略与模拟执行、断线重连和 REST 重试、指标/HTTP，以及“本地模拟交易所 → 实时引擎 → WAL → 回放摘要一致”的端到端链路。完整结果 **275 passed**。

## 15 分钟主结果

`20261021T075925Z-3049`，900.129 s，日志/信号/成交/性能/回放全部来自同一次 live：

| 指标 | 结果 |
|---|---|
| 原始帧 / WAL 记录 | 303,784 / 303,793 |
| 信号 / fills | 41,470 / 8,828 |
| depth / aggTrade / bookTicker | 42,620 / 17,381 / 243,783 |
| depth e2e p50 / p99 | 0.512 / 2.435 ms |
| 聚合 e2e p50 / p99 | 0.512 / 2.435 ms（bookTicker 占 80.2%） |
| 原始帧丢失 / data gap / resync | 0 / 0 / 0 |
| 回放 | `verified=true`、`deterministic=true`，三项 SHA 一致 |

延迟从 `ws.recv()` 返回后开始计时，不含网络/内核缓冲，只证明接收后的处理延迟；完整分位数和资源数据见 [`docs/性能实测.md`](docs/性能实测.md)。

## 已知的局限

- 公开 WS 不自动补发，本 demo 尚未实现 REST 补采；缺口记在 `summary.data_gaps`。
- Python 单进程不是生产级 HFT，延迟不能和 C++/FPGA/colocation 比；策略收益不具解释力。
- `Simulator` 是抽象记账器，主结果允许负仓位/负现金；成交用 `@aggTrade` 而非逐笔 `@trade`。完整边界见 `交付说明.md` §6。

MIT，见 [LICENSE](LICENSE)。
