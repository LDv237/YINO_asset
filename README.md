# TickForge

TickForge 是我这次笔试交的实时高频数据流水线 demo，行情接的是 Binance 现货公开接口。WebSocket 原始帧先原样落盘，然后做 L2 订单簿重建、因子计算、做市信号和模拟成交；一次跑完可以从 WAL 按字节做确定性回放，把回放结果和运行目录里的派生文件逐项对上。

本次提交的**主结果**是最终代码的 15 分钟 live 单源 run `20261021T075925Z-3049`：303,784 条原始帧、41,470 个信号、8,828 笔模拟成交，原始帧 0 丢失、0 data gap、0 订单簿 resync，回放三项 SHA-256 一致。历史基准 53b5、严格落盘 6db8 和同一 WAL 只做多对照只在报告里以表格出现，完整数据在 GitHub Release，不随邮件提交。

> 笔试题目：YINO ASSET 量化开发工程师（实时高频行情交易系统 demo）。
> GitHub: https://github.com/LDv237/YINO_asset
> 先看 [`交付说明.md`](交付说明.md) 开头的“评审导览（60 秒）”；材料清单与复现入口也在里面。
> 完整历史数据见 [GitHub Release delivery-20261020](https://github.com/LDv237/YINO_asset/releases/tag/delivery-20261020)。

## 我重点处理的几件事

实时行情最怕的是看起来在跑、其实已经丢了数据，所以我先把落盘做扎实：每条 WebSocket 帧在解析前先进有界队列，由一个独立线程按顺序写进带 CRC32 的分段 WAL；队列满了就反压 socket，不静默丢包。正常关闭会把队列排空，再 flush+fsync，实测 0 丢失。

持久化策略在配置里留了口子。`fsync_policy=interval` 每 250ms 尝试组提交，崩溃时没同步的尾部可能丢；250ms 是目标间隔，不是硬上界。`always` 要每条记录 fsync 之后才进处理，延迟更高，异常终止后已 fsync 的前缀仍然能校验读取（崩溃实验只证明强杀后可读前缀，不逐条证明 fsync）。`never` 只在关闭或轮转时同步。这些边界报告里都按实际表现写了。

订单簿按 Binance 的 `U/u` 规则维护完整档位。检测到丢包或者交叉时，程序会自动进入缓冲，再用 REST 快照加增量重放把簿恢复回来。

回放能不能对上，是这条流水线的底线。REST 快照会作为 `kind=2` 事件写进 WAL，回放走同一套流水线，所以因子、信号、成交三个 JSONL 的 SHA-256 必须和运行目录里的 derived 一致；对不上就说明中间有环节不可复现。主结果 `3049` 的 derived 是 live 同源，回放直接对比该结果；历史基准 53b5 的 derived 已按同一份 WAL 重算，只作为性能对照。

运行状态可以从 `/healthz`、`/readyz`、`/status`、`/metrics` 查，WAL 队列、处理滞后、逐阶段延迟、订单簿状态、反压次数都在里面。校验手段包括 WAL 的 CRC/seq/分段哈希、计数对账、单元/集成/混沌测试，以及回放摘要对比。

## 行情数据从哪来

默认接 Binance 的行情专用只读端点，不需要 API Key：

- WebSocket：`wss://data-stream.binance.vision/stream`
- REST 快照：`https://data-api.binance.vision`

默认订阅 6 个交易对（`BTCUSDT`、`ETHUSDT`、`SOLUSDT`、`BNBUSDT`、`XRPUSDT`、`DOGEUSDT`）的 `depth@100ms`、`aggTrade`、`bookTicker` 三条流。BTCUSDT 始终在列表里；多交易对是为了把 15 分钟窗口的消息量撑起来。

## 怎么跑

环境是 Python 3.11+，本机验证用 3.14.7。`requirements-lock.txt` 固定了运行时依赖（aiohttp、orjson、websockets、psutil 及其传递依赖）。

```bash
python -m pip install -r requirements-lock.txt

# 新跑一次 15 分钟实时行情（需要交易所网络）
python -m tickforge live --config config/default.toml --duration 900 --out artifacts

# 运行中查看健康与指标
curl http://127.0.0.1:9100/healthz
curl http://127.0.0.1:9100/metrics
```

> 路径约定：**本邮件包不含完整 `artifacts/`**（约 1,011 MiB，7 个 run 在 GitHub Release 的 `YINO_artifacts_complete_20261020.zip`）。文档里的 `artifacts/runs/<run_id>` 指新跑 live 或 Release/仓库；提交包内主结果在 `运行材料/20261021T075925Z-3049/`，不需要找 `artifacts/`。`artifacts/README.md` 有更详细的说明。

包内已经带了主结果的完整 run 目录，不需要重新采集就能验证：

```bash
python -m tickforge verify --run 运行材料/20261021T075925Z-3049
python -m tickforge replay --run 运行材料/20261021T075925Z-3049 \
  --out 运行材料/20261021T075925Z-3049-replay --verify
```

`replay --verify` 会比对 WAL 记录数、订单簿同步/因子/信号/成交计数，以及 `factors.jsonl`、`signals.jsonl`、`fills.jsonl` 的 SHA-256；任何一项对不上，退出码就是非零。

离线样本（不访问交易所网络，约 6 秒）：

```bash
python -m tickforge replay --run samples/demo_run \
  --out samples/demo_run-replay --verify
```

## 两套执行语义

默认是抽象做市，配置在 `config/default.toml`。它允许负仓位和负现金，为的是从零库存开始跑双边做市；它不是 Binance 现货账户，只是这条流水线的性能/链路基准。主结果 `3049` 用的就是这套配置。

`config/spot_long_only.toml` 禁止裸卖，卖出不超过持仓，买入受每个 symbol 10,000 USDT 初始现金限制，`position<=0` 的时候策略不发 ask。同一份 53b5 WAL 的只做多对照是 3,077 fills、0 负仓位/负现金；这是现货语义的对照，不是主结果。设计和完整数据见 [`docs/现货只做多设计.md`](docs/现货只做多设计.md) 与 Release。

严格落盘版（每条记录 fsync 后才处理）用 `config/spot_long_only_durable.toml`。15 分钟样本 `6db8` 是 272,784 帧、272,802 次 fsync、1,578 fills、0 负仓位/负现金；该次运行有网络侧缺口（不是 WAL 丢帧），完整数字和边界见 `交付说明.md` §6 / Release。

## Docker 的现状

Docker 这块本机没跑起来：Docker Desktop 的 WSL2 虚拟化组件不完整，引擎起不来。但镜像已经在 GitHub Actions 的干净环境完成 build、容器版本、离线 sample replay、60 秒 live、verify 和 replay，全部通过；run 是 https://github.com/LDv237/YINO_asset/actions/runs/39552903429，commit `83d4c1a`，image ID 是 `sha256:a960d6c2…`。关键证据在 [`运行材料/验证证据/docker_ci/`](运行材料/验证证据/docker_ci/)，完整 build 日志在 Release。

Release 另附 `tickforge-0.1.0.tar.gz`（`docker load` 后可直接运行，linux/amd64）。本机或其他 Docker 主机可按 [`docs/Docker构建验证.md`](docs/Docker构建验证.md) 复跑。

## 目录结构

```text
tickforge/           核心包（feed/store/book/factor/strategy/exec/runtime/replay/metrics/server）
tests/               单元、集成、混沌与端到端回放测试（274 passed）
tools/               运行分析、导出、追溯和样本裁剪
scripts/             15 分钟运行、回放校验和 Docker 运行
config/              默认、只做多、严格落盘三套配置
docs/                架构、接口、性能、验证、开发过程、AI 协作、依赖说明、运行手册
samples/demo_run/    20,000 条 WAL 的离线样本（来自 53b5，仅用于验证 replay 机制）
运行材料/            主结果 20261021T075925Z-3049/ 和 验证证据/
交付说明.md           提交说明（开头是评审导览）
笔试报告.md           完整报告
```

## 测试覆盖

```bash
python -m pip install -r requirements-dev-lock.txt
python -m pytest -q
```

测试里有订单簿序列、快照恢复和随机对拍，WAL 的轮转/CRC/截断/fsync，因子手算，策略与模拟执行，WebSocket 断线重连与 REST 重试，指标并发与 HTTP，还有一条“本地模拟交易所 → 实时引擎 → WAL → 回放摘要一致”的端到端链路。完整测试结果是 **274 passed**。

## 15 分钟主结果

> 完整数据、延迟分位、资源占用和复现命令见 [`docs/性能实测.md`](docs/性能实测.md)。

`20261021T075925Z-3049`，900.129 s，日志/信号/成交/性能/回放全部来自同一次 live：

| 指标 | 结果 |
|---|---|
| 原始帧 / WAL 记录 | 303,784 / 303,793 |
| 信号 / fills | 41,470 / 8,828 |
| depth / aggTrade / bookTicker | 42,620 / 17,381 / 243,783 |
| depth e2e p50 / p95 / p99 / p999 | 0.512 / 1.448 / 2.435 / 4.871 ms |
| 聚合 e2e p50 / p99 / p999 | 0.512 / 2.435 / 5.793 ms（bookTicker 占 80.2%，只做轻处理） |
| 原始帧丢失 / `clean_shutdown` | 0 / true |
| data gap / 订单簿 resync | 0 / 0 |
| 队列峰值 / durable p50 / p95 | WAL 413 / pipeline 16；262 / 312 ms（`interval` 批次口径） |
| 起始小时桶活跃度 | 07:00–08:00 UTC，67.53M USDT / 97,351 笔，排 6/47（89.4 分位） |
| 回放摘要一致性 | 因子/信号/成交三项 SHA-256 全部一致 |

延迟从 `ws.recv()` 返回后开始计时，不含 WebSocket 库、内核缓冲和网络 RTT；它只证明接收后的处理延迟，不能证明行情从交易所产生到策略完成同样只用几毫秒。

历史基准 53b5（388,388 帧、43,309 信号、10,177 fills，执行结果为 WAL 重算）和 6db8（always 落盘）只在报告和 Release 里作为对照，不作为本次提交的主结果。

## 已知的局限

- 公开行情 WebSocket 不自动补发；本 demo 尚未实现 REST 补采（REST aggTrades 可按 fromId/时间分页补取聚合成交，历史 depth 增量和原 WS 帧不能恢复），缺口会写进 `summary.json` 的 `data_gaps`；
- Python 单进程不是生产级 HFT，这些延迟数据只用于工程验证，不能拿去和 C++/FPGA 系统比；
- 模拟撮合没有建模排队位置、撤单延迟和市场冲击；主结果 3049 的 fills 是 live 同源，历史 run 的执行结果由同一份 WAL 用基于实际成交时间 `T` 的 quote-time guard + tick 取整重算；
- `Simulator` 是抽象记账器，主结果允许负仓位/负现金，不代表 Binance 现货账户的可执行性；现货语义看只做多对照；
- 没有接私有下单 API 和真实风控前置。

## License

MIT，见 [LICENSE](LICENSE)。
