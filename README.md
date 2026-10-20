# TickForge

TickForge 是我这次笔试交的实时高频数据流水线 demo，行情接的是 Binance 现货公开接口。WebSocket 原始帧先原样落盘，然后做 L2 订单簿重建、因子计算、做市信号和模拟成交；一次跑完可以从 WAL 按字节做确定性回放，把回放结果和实时运行逐项对上。

> 笔试题目：YINO ASSET 量化开发工程师（实时高频行情交易系统 demo）。
> GitHub: https://github.com/LDv237/YINO_asset
> 文件名说明：GitHub 首页固定读取 `README.md`；其余设计、报告、验证文档均使用中文文件名。
> 提交材料清单与复现入口见 [交付说明.md](交付说明.md)。
> 完整运行材料（原始 WAL、JSONL、CSV、验证证据、日志）见仓库根目录的 [`交付材料/`](交付材料/)；邮件发送版把全部内容打成一个完整压缩包。
> 完整运行产物的打包下载（`artifacts/` 压缩包与邮件完整包）见 [GitHub Release delivery-20261020](https://github.com/LDv237/YINO_asset/releases/tag/delivery-20261020)。
> 设计与取舍见 [docs/系统架构.md](docs/系统架构.md)，模块接口与 WAL 格式见 [docs/接口设计.md](docs/接口设计.md)，实测与复现见 [docs/性能实测.md](docs/性能实测.md)，完整报告见 [笔试报告.md](笔试报告.md)。

## 我重点处理的几件事

实时行情最怕的是看起来在跑、其实已经丢了数据，所以我先把落盘做扎实：每条 WebSocket 帧在解析前先进有界队列，由一个独立线程按顺序写进带 CRC32 的分段 WAL；队列满了就反压 socket，不静默丢包。正常关闭会把队列排空，再 flush+fsync，实测 0 丢失。

持久化策略我在配置里都留了口子。`fsync_policy=interval` 每 250ms 尝试组提交，崩溃时没同步的尾部可能丢；250ms 是目标间隔，不是硬上界，队列和磁盘都会影响它。`always` 要每条记录 fsync 之后才进处理，延迟更高，但异常终止后已 fsync 的前缀仍然能校验读取。`never` 只在关闭或轮转时同步。这些边界报告里都按实际表现写了。

订单簿按 Binance 的 `U/u` 规则维护完整档位。检测到丢包或者交叉时，程序会自动进入缓冲，再用 REST 快照加增量重放把簿恢复回来。

回放能不能对上，是这条流水线的底线。REST 快照会作为 `kind=2` 事件写进 WAL，回放走同一套流水线，所以因子、信号、成交三个 JSONL 的 SHA-256 必须和实时运行一致；对不上就说明中间有环节不可复现。

运行状态可以从 `/healthz`、`/readyz`、`/status`、`/metrics` 查，WAL 队列、处理滞后、逐阶段延迟、订单簿状态、反压次数都在里面。校验手段包括 WAL 的 CRC/seq/分段哈希、计数对账、单元/集成/混沌测试，以及回放摘要对比。跑法上，本地直接 `python -m tickforge`，也可以用 Docker 或 docker compose。

## 行情数据从哪来

默认接 Binance 的行情专用只读端点，不需要 API Key，主站网络访问不了的时候也能用：

- WebSocket：`wss://data-stream.binance.vision/stream`
- REST 快照：`https://data-api.binance.vision`

默认订阅 6 个交易对（`BTCUSDT`、`ETHUSDT`、`SOLUSDT`、`BNBUSDT`、`XRPUSDT`、`DOGEUSDT`）的 `depth@100ms`、`aggTrade`、`bookTicker` 三条流。要改的话，[config/default.toml](config/default.toml) 或命令行都可以覆盖。

题目允许自己选公开、免费的实时高频数据源，`BTCUSDT` 是题目给的示例，不是唯一选项。我在同一个币安现货公开接口上加了 5 个 USDT 交易对，把 15 分钟窗口里的消息量撑起来，BTCUSDT 始终在主列表里。说白了，这不是换数据源，是同一接口的多 symbol 订阅。

## 怎么跑

环境要 Python 3.11+，我本机验证用的是 3.14.7。`requirements-lock.txt` 把运行时的全部直接和传递依赖都固定住了（aiohttp 3.14.5、orjson 3.13.0、websockets 17.3、psutil 7.2.2 等），重建环境请直接用锁文件。

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
python -m tickforge replay --run "$RUN_DIR" --out "$RUN_DIR-replay" --verify
python tools/analyze_run.py "$RUN_DIR"

# 把二进制 WAL 导出为人类可读的原始 JSONL（可选）
python tools/wal_export.py --run "$RUN_DIR" --out "$RUN_DIR/raw_events.jsonl.gz"

# 按事件类型导出 CSV（Excel/表格查看）
python tools/wal_export_csv.py --run "$RUN_DIR" --out-dir "$RUN_DIR/csv"
```

`replay --verify` 会拿实时运行和回放比这几样东西：

- WAL 记录数（按 kind）；
- 订单簿同步、因子快照、信号、成交计数；
- `factors.jsonl` / `signals.jsonl` / `fills.jsonl` 的 SHA-256。

任何一项对不上，退出码就是非零。

## 两套执行语义

默认是抽象做市，配置在 `config/default.toml`。它允许负仓位和负现金，为的是从零库存开始跑双边做市；它不是 Binance 现货账户，只是这条流水线的性能和链路基准，不代表现货执行语义。现货执行看只做多那套配置。

`config/spot_long_only.toml` 禁止裸卖，卖出不超过持仓，买入受每个 symbol 10,000 USDT 初始现金限制，`position<=0` 的时候策略不发 ask。设计和实测在 [docs/现货只做多设计.md](docs/现货只做多设计.md)。

```bash
# 现货只做多 15 分钟
python -m tickforge live --config config/spot_long_only.toml   --duration 900 --out artifacts
```

严格落盘版（每条记录 fsync 后才处理，延迟更高）：

```bash
python -m tickforge live --config config/spot_long_only_durable.toml   --duration 900 --out artifacts
```

严格落盘版我实跑了一轮：272,784 帧、272,802 条 WAL、1,576 fills、0 负仓位/负现金；p50 3.444 ms / p99 5.793 ms。中间有 2 次网络重连，出现了 data gap，但收到的原始帧 0 丢失，订单簿靠快照恢复了。异常终止恢复的实验在 `docs/正确性与可靠性验证.md`。

## Docker 的现状

Docker 这块本机没跑起来：Docker Desktop 装了，但 WSL2 虚拟化组件不完整，引擎起不来。不过镜像已经在 GitHub Actions 的干净环境里完成 build、容器版本、离线 sample replay、60 秒 live、verify 和 replay，全部通过；run 是 https://github.com/LDv237/YINO_asset/actions/runs/39457220374 ，image ID 是 sha256:e67c76fd6a54d1011248c59acdc3d1522e00659d5495beaeed4ba4356b46e949。CI 证据在 `artifacts/experiments/docker/ci/`，本机或其他 Docker 主机可以按 [docs/Docker构建验证.md](docs/Docker构建验证.md) 和 [scripts/docker_verify.sh](scripts/docker_verify.sh) 复跑。

```bash
docker compose build
docker compose up --abort-on-container-exit

# 回放（容器内）
docker compose run --rm tickforge replay --run /data/runs/<run_id> --out /data/runs/<run_id>-replay --verify
```

镜像内默认 `TICKFORGE_HTTP_HOST=0.0.0.0`、输出目录 `/data`；`docker-compose.yml` 将 `./artifacts` 挂载到 `/data`。如构建网络受限，可设置 `PIP_INDEX_URL` 指向镜像站。

## 目录结构

```text
tickforge/           核心包（feed/store/book/factor/strategy/exec/runtime/replay/metrics/server）
tests/               单元、集成、混沌与端到端回放测试
tools/               运行分析、样本裁剪
scripts/             15 分钟运行、回放校验、Docker 运行
config/default.toml  默认配置
docs/                架构、接口、性能、验证、开发过程、人工智能协作、运行手册
笔试报告.md            笔试交付报告
artifacts/           运行产物（WAL、可读 JSONL/CSV、派生 JSONL、日志、摘要、清单）
```

## 测试覆盖

```bash
python -m pip install -r requirements-dev-lock.txt
python -m pytest -q
```

测试里有订单簿序列、快照恢复和随机对拍，WAL 的轮转/CRC/截断/fsync，因子手算，策略与模拟执行，WebSocket 断线重连与 REST 重试，指标并发与 HTTP，还有一条“本地模拟交易所 → 实时引擎 → WAL → 回放摘要一致”的端到端链路。

## 15 分钟实测

> 完整数据、延迟分位数、资源占用和复现命令见 [docs/性能实测.md](docs/性能实测.md)。

跑下来的结果：

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

## 已知的局限

- 公开行情 WebSocket 不提供断线补发，断线期间真实成交补不回来，缺口会写进 `summary.json` 的 `data_gaps`；
- Python 单进程不是生产级 HFT，这些延迟数据只用于工程验证，不能拿去和 C++/FPGA 系统比；
- 模拟撮合没有建模排队位置、撤单延迟和市场冲击；
- `Simulator` 是抽象记账器，允许负仓位/负现金，不代表 Binance 现货账户的可执行性，也没有建模借币、保证金或强平；
- 没有接私有下单 API 和真实风控前置。
