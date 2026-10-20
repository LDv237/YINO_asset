# 正确性与可靠性验证

本项目把“能跑”拆成五类可重复的验证：单元正确性、状态机恢复、端到端链路、故障注入、确定性回放。所有命令均在项目根目录执行。

## 1. 验证矩阵

| 层次 | 关注点 | 证据 |
|---|---|---|
| 单元测试 | 订单簿序列规则、WAL 格式/CRC、因子手算、策略风控、模拟撮合、指标并发 | `pytest` |
| 集成测试 | 本地模拟交易所（WebSocket + REST）→ 实时引擎 → WAL → 派生输出 → 回放 | `tests/test_integration_live.py` |
| 故障注入 | 断线重连、消息超时、REST 500/重试、WAL CRC 翻转、尾部截断、快照过旧/丢包、交叉簿 | 各模块测试中的 mock/fixture |
| 运行时对账 | 接收帧数 = WAL 记录数 = 已处理事件数；队列排空；无静默丢弃 | `summary.json.reconciliation` |
| 确定性回放 | 同一 WAL 回放产生的因子/信号/成交 SHA-256 与实时一致 | `replay_summary.json.comparison` |
| 完整性校验 | WAL magic/长度/CRC/seq、manifest 分段 SHA-256、产物校验和 | `python -m tickforge verify` |

## 2. 运行测试

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

测试数量与结果：`259 passed, 0 failed`（Python 3.14.7，Windows 11）。 另在全新虚拟环境中仅安装 `requirements-dev-lock.txt`，`pip check` 无冲突，259 个测试全部通过。

重点用例：

- `tests/test_book.py`
  - Binance 文档中的“快照 `lastUpdateId` + 首笔 `U/u`”边界；
  - 连续更新、重复、重叠、gap → 缓冲 → 新快照恢复；
  - 快照过旧（缓冲首事件 `U > last+1`）时不得误判同步；
  - 交叉簿、零量删除、`top_levels` 排序、缓冲溢出、`begin_resync`；
  - 随机差分测试：同一事件流分别喂给实现与朴素全量簿，逐步比对。
- `tests/test_wal.py`
  - 10k 记录往返、分段轮转、CRC 篡改、尾部截断、fsync 策略、关闭/Digest；
  - 读取端严格校验 seq 单调与长度上限。
- `tests/test_factors.py`
  - microprice、spread 手算；OFI 四种最优档变化；成交不平衡；动量窗口；
  - 波动非负、节流/乱序时间、reset、无簿/单边簿。
- `tests/test_strategy.py` / `tests/test_simulator.py`
  - 公允价、库存偏移方向、冷却、禁用以外的单边限制；
  - 买卖/部分成交、费率、平均成本、名义上限、取消、对账；
  - 相同输入两次执行得到完全相同的 `Fill` 序列。
- `tests/test_feed.py` / `tests/test_rest.py`
  - 本地 WebSocket：消息接收/时间戳、断线重连、stale 超时；
  - 本地 REST：成功、500 重试、重试耗尽、字段校验失败。
- `tests/test_metrics.py` / `tests/test_http.py`
  - 多线程并发计数、直方图分位、Prometheus 文本、`/healthz` 200/503、`/metrics`。
- `tests/test_integration_live.py`
  - 本地模拟 Binance：1 个交易对、2 秒窗口；
  - 验证 `clean_shutdown`、无丢失、订单簿同步、因子/信号/成交均非零；
  - `verify_run` 通过；`ReplayRunner` 摘要与实时运行一致。

## 3. 15 分钟实测的对账口径

`summary.json` 中：

```json
{
  "counts": {"ws_frames_received": ..., "processed_envelopes": ...,
             "pipeline": {"ws_frames": ..., "snapshots": ..., "factor_snapshots": ...,
                          "signals": ..., "fills": ...}},
  "wal": {"records_ws": ..., "records_snapshot": ..., "records_total": ...},
  "reconciliation": {"lost_raw_frames": 0, "unprocessed_at_shutdown": 0,
                     "not_enqueued_at_shutdown": 0, "drained": true},
  "clean_shutdown": true
}
```

判定：

1. `ws_frames_received == wal.records_ws`：每个收到的原始帧都进入 WAL（正常关闭时 flush+fsync；`interval` 的崩溃窗口见 `docs/PERFORMANCE.md` 第 4 节的持久化契约）；
2. `wal.records_ws + wal.records_snapshot == processed_envelopes`：每个持久化事件都被处理；
3. `lost_raw_frames == 0`、`unprocessed_at_shutdown == 0`、`not_enqueued_at_shutdown == 0`；
4. `clean_shutdown == true`；
5. `verify` 通过；
6. `replay --verify` 的 `digests` 三项全部匹配。

统计窗口从第一条行情到达开始，避免把 DNS/握手/重连时间算进 15 分钟样本。

## 4. 故障注入

| 注入 | 测试方式 | 期望 |
|---|---|---|
| WebSocket 断开 | 本地 server 主动 `close()` | feed 退避重连；`on_disconnect/on_connect` 计数；数据缺口记录 |
| 消息停滞 | 本地 server 停止发送 > `stale_timeout_ms` | 连接被判定 stale 并重建 |
| depth 丢包 | 删除中间 `u` 序列 | `BookResult=gap`，缓冲，请求快照；快照后恢复同步 |
| 快照过旧 | 快照 `lastUpdateId` 落后于缓冲首事件 | 保持未同步并保留缓冲，等待新快照；不得输出因子 |
| 交叉簿 | 构造 bid ≥ ask 的增量 | 立即隐藏订单簿、计数并请求快照 |
| REST 500 | mock 返回 500 若干次后成功 | 指数退避重试；最终成功或健康降级 |
| WAL 损坏 | 翻转 payload 一个字节 | `WalReader` 抛 `WalCorruptionError`，`verify` 失败 |
| WAL 截断 | 截掉最后一条记录尾部 | 默认严格模式报错；允许模式记录 `truncated_tail_bytes` |
| 慢消费者 | 小队列 + 人工延迟 | 反压计数增加，原始帧不丢；处理滞后可见 |
| 磁盘写失败 | 单测 monkeypatch writer I/O | WAL 标记 error、后续 append 抛 `WalClosed`、进程可退出 |

## 5. 对抗评审发现并修复的问题

实现完成后，每个子系统由独立 reviewer 按接口与真实 WAL 做反例验证。以下问题被复现并修复（不是“看起来可能”）：

| 级别 | 模块 | 问题 | 修复 |
|---|---|---|---|
| major | 因子 | `_ofi_delta` 的 ask 侧价格变化符号与经典 Cont–Kukanov–Stoikov 定义相反；实现者测试沿用了同一错误期望 | ask 下移 `-cur_qty`、上移 `+prev_qty`；修正接口文档与测试 |
| major | 订单簿 | 早先抓取的旧 REST 快照可能在较新的增量之后入队，`apply_snapshot` 会把簿回退到旧 `lastUpdateId`，下一笔连续增量被误判为 gap | `apply_snapshot` 拒绝 `lastUpdateId < last_update_id`，不重建、不回退；新增回归测试 |
| major | 接入 | `BinanceFeed.run()` 在握手刚完成时被取消，可能遗留一个未关闭的 WebSocket | 取消路径检查已完成握手的结果并 `shield` 关闭；补强取消测试 |
| minor | 执行 | 极大但有限的 price×1e8 溢出导致 `OverflowError`；名义上限的测试不变量在有价格变动时不可实现 | 缩放后检查有限性并丢弃；测试改为“不增加超限暴露 + 允许减仓” |
| minor | 指标/HTTP | `port=0` 重启复用了旧的临时端口；异常标签名可能碰撞 | 区分 configured/bound port 并恢复；指标边界问题记录为已知限制（生产标签为固定合法集合） |

修复后完整测试通过，并用真实行情 smoke + 回放重新验证摘要。

## 6. 成交编号连续性（P2）

`@aggTrade` 的 `a` 编号现在按 symbol 独立检查：

- 重复/回退事件：计数并跳过，避免重复计入因子和模拟成交；
- 缺口：累计 `missing_ids`，仍处理缺口事件本身；
- 统计写入 `summary.pipeline.trade_quality` 与 Prometheus 指标。

抽象做市性能基准运行的 388,394 条 WAL 回放结果为 6 个 symbol **0 duplicates / 0 backwards / 0 gaps / 0 missing_ids**（`artifacts/trade_integrity.json`）。注意区分：

- `raw_frame_loss=0`：程序没有漏收已送达的 WebSocket 帧；
- `trade_gap=0`：在本段流内未检测到 aggTrade ID 缺口，但不能证明断线期间交易所端没有发生成交；断线数据缺口仍由 `summary.data_gaps` 单独记录。

## 7. 实测结果

- 测试：**259 passed, 0 failed**（完整测试套件，Windows 11 / Python 3.14.7）。
- 实测窗口：900.116 s；6 个交易对；`run_id=20261020T072820Z-53b5`。
- 交易活跃度：窗口所在 07:00–08:00 UTC 桶成交额 73.96M USDT、125,770 笔，47 个完整小时中排名 3/47（95.7 分位），见 `artifacts/market_activity.json`。
- 原始帧 / WAL / 处理：388,388 / 388,394（WS 388,388 + snapshot 6）/ 388,394。
- 丢失 / 未处理 / 未入队：0 / 0 / 0；`drained=true`，`clean_shutdown=true`。
- `verify`：`ok=true`；388,394 条记录 CRC 全部通过；两个分段 SHA-256 与 manifest 一致。
- 回放摘要三项：`factors` / `signals` / `fills` 的 SHA-256 与实时运行全部一致；计数一致；回放 13.732 s，28,283.4 events/s。
- 首轮 15 分钟（443,326 帧）与评审期磁盘压力重叠，仍实现 0 丢失、0 gap、0 重连；说明反压/落盘路径在压力下保持完整。
- 现货只做多变体：同一份 WAL 重放 3,082 fills、负仓位/负现金均为 0；独立 15 分钟运行 325,280 帧、2,131 fills、0 负仓位/0 负现金，回放摘要一致。
- 健康语义：正式 summary 的 `health_before_shutdown.ok=true`、`health_final.ok=true`、`health_final_context=after_intentional_shutdown`；主动关停的 feed 断开不再被误报为异常。

### 7.1 严格落盘与异常终止

- 严格模式（`config/spot_long_only_durable.toml`，`fsync_policy=always`）15 分钟：`run_id=20261020T082850Z-6db8`，272,784 帧、272,802 条 WAL、每条 fsync；0 负仓位/负现金，`verify` 与 `replay --verify` 通过。期间 2 次网络重连造成 data gap，但收到的原始帧 0 丢失，订单簿 resync 恢复；其 08:00–09:00 UTC 小时桶成交额排名 4/47（93.6 分位）。
- 异常终止实验：`tools/crash_recovery_check.py` 启动短时 live 后强制终止进程（非优雅关闭），WAL 中 229 条已 fsync 记录全部可读，CRC 与 seq 连续，`truncated_tail_bytes=0`；由于进程被终止，没有 `summary.json`/`clean_shutdown`，这符合预期。证据：[crash_recovery_report.json](artifacts/crash_recovery_report.json)。

必须区分：
- 异常终止测试验证的是“已持久化前缀可恢复”，不是物理掉电测试；
- `always` 是应用层每条 fsync，不等于对磁盘固件/物理损坏的绝对保证；
- `interval` 的目标 250ms 组提交不是硬上界，队列/磁盘积压会扩大未同步窗口。

## 8. 测试的局限

- 本地 mock 只能覆盖协议与状态机行为，不能模拟真实交易所的撮合、限频细节与网络抖动；
- 回放确定性证明的是“对这份 WAL 的输入，同一实现产生同一输出”，不是对任意未来输入的证明；
- 单机测试未覆盖多进程/多机、时钟跳变、内核 OOM、磁盘满等生产故障；
- Python 的 GC 停顿、操作系统调度会进入延迟长尾，报告如实给出分位数而不是只给均值。
