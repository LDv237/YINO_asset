# 交付清单与邮件模板

## 1. 本次交付包含

| 内容 | 位置 |
|---|---|
| 全部代码 | `tickforge/`、`tests/`、`tools/`、`scripts/` |
| 架构与接口文档 | `docs/ARCHITECTURE.md`、`docs/INTERFACES.md` |
| 性能实测文档 | `docs/PERFORMANCE.md` |
| 验证文档 | `docs/VERIFICATION.md` |
| AI 协作记录 | `docs/AI_COLLABORATION.md` |
| 运行手册 | `docs/RUNBOOK.md` |
| 完整报告 | `REPORT.md` |
| Docker | `Dockerfile`、`docker-compose.yml`、`.dockerignore` |
| 锁定依赖 | `requirements-lock.txt`、`requirements-dev-lock.txt`（Python 3.14.7 下验证） |
| 15 分钟实测数据（抽象做市） | `artifacts/runs/20261020T072820Z-53b5/`（WAL、派生数据、日志、summary、manifest） |
| 现货只做多变体 | `config/spot_long_only.toml`、`artifacts/runs/20261020T074416Z-91ac/`、`docs/SPOT_LONG_ONLY.md`、`artifacts/spot_long_only_summary.json` |
| 严格落盘变体 | `config/spot_long_only_durable.toml`、`artifacts/runs/20261020T082850Z-6db8/`、`artifacts/spot_long_only_durable_summary.json` |
| 异常终止恢复证据 | `artifacts/crash_recovery_report.json`、`tools/crash_recovery_check.py` |
| 离线样本 | `samples/demo_run/`（20,000 条记录，约 64.9 s；含离线参考 summary，可 `--verify`） |
| 压缩原始 WAL | `artifacts/tickforge_raw_15m.tar.gz`（16 MB） |
| 人类可读原始行情 | `artifacts/tickforge_raw_15m.jsonl.gz`（15 MB，388,394 行） |
| 表格视图 CSV | `artifacts/tickforge_csv_15m.zip`（5.3 MB，按 aggTrade/bookTicker/depth/快照拆分） |
| 成交编号完整性 | `artifacts/trade_integrity.json`（0 重复/0 回退/0 缺口） |
| 交易活跃度证据 | `artifacts/market_activity.json`（07:00 桶 3/47）与 `market_activity_strict.json`（08:00 桶 4/47） |
| cash 最低值审计 | `artifacts/cash_minima.json`（区分期末最小值与逐笔过程最低值） |
| 持久化口径 | 正常关闭 flush+fsync（0 丢失）；`interval` 每 250ms 尝试组提交，未同步尾部可能丢失（目标间隔非硬上界）；`always` 每条 fsync 后才处理，延迟更高（见 docs/PERFORMANCE.md） |
| 两种执行语义成交 CSV | `artifacts/abstract_fills.csv`、`artifacts/spot_long_only_fills.csv`、`artifacts/spot_long_only_same_wal_fills.csv`、`artifacts/spot_long_only_durable_fills.csv` |

## 2. GitHub 公开仓库（已发布）

```text
https://github.com/LDv237/YINO_asset
```

本地仓库已有 `main` 分支和提交历史，`origin` 指向上述仓库；`.gitignore` 排除了 `artifacts/`、完整 WAL、日志和缓存，公开仓库只包含代码、测试、文档、配置和 `samples/demo_run` 离线样本。

后续修改后的推送方式：

```bash
git add <files>
git commit -m "<message>"
git push origin main
```

## 3. 构建与验证 Docker 镜像（必做；当前尚未在本机验证）

```bash
docker build -t <dockerhub用户名>/tickforge:0.1.0 .
docker run --rm -p 9100:9100 -v "$PWD/artifacts:/data" \
  <dockerhub用户名>/tickforge:0.1.0 \
  live --duration 60 --out /data

docker push <dockerhub用户名>/tickforge:0.1.0
```

**注意：只看 Dockerfile 不算完成 P0。** 题目要求“打包 Docker image 保证代码正常运行”，提交前必须在有 Docker 的机器上执行并保存证据。完整命令与验收清单见 [docs/DOCKER_VERIFICATION.md](docs/DOCKER_VERIFICATION.md) 和 [scripts/docker_verify.sh](scripts/docker_verify.sh)。构建成功后可推送公共镜像，但不是必须。

## 4. 打包材料

```bash
# 1) 代码 + 文档 + 样本（不含完整 artifacts）
zip -r tickforge_delivery.zip \
  tickforge tests tools scripts config docs samples README.md REPORT.md DELIVERY.md \
  Dockerfile docker-compose.yml .dockerignore requirements.txt requirements-dev.txt requirements-lock.txt requirements-dev-lock.txt pyproject.toml

# 2) 15 分钟原始数据（压缩）
cd artifacts/runs/<run_id>
tar -czf ../../../artifacts/tickforge_raw_15m.tar.gz raw
cd ../../..
ls -lh tickforge_delivery.zip artifacts/tickforge_raw_15m.tar.gz

# 3) 汇总/清单单独附上，便于 HR 快速查看
cp artifacts/runs/<run_id>/summary.json tickforge_summary.json
cp artifacts/runs/<run_id>/manifest.json tickforge_manifest.json
```

若邮件附件限制 20–25 MB：

- 代码与文档约 1 MB；
- 15 分钟 WAL 压缩后通常 15–40 MB（取决于行情活跃度）；可拆分为 `split -b 15m` 的多卷，或用网盘链接；
- 至少附上 `samples/demo_run` 离线样本，确保评审无需交易所网络即可回放。

## 5. 邮件模板（中文）

> 主题：YINO ASSET 量化开发工程师笔试提交 - 姓名
>
> 您好，
>
> 以下是我的笔试项目 TickForge（实时高频行情交易系统 demo）提交材料：
>
> 1. 项目代码与文档：https://github.com/LDv237/YINO_asset
> 2. Docker：仓库根目录 Dockerfile / docker-compose.yml；构建与运行命令见 README
> 3. 15 分钟实测数据：附件 `tickforge_raw_15m.tar.gz`（原始 WAL，含全部 WS 帧与 REST 快照）、`tickforge_raw_15m.jsonl.gz`（可读视图）、`tickforge_csv_15m.zip`（CSV 表格视图）、`tickforge_summary.json`
> 4. 现货只做多变体：`config/spot_long_only.toml`、`spot_long_only_summary.json`、`spot_long_only_same_wal_summary.json`；严格落盘版：`config/spot_long_only_durable.toml`、`spot_long_only_durable_summary.json`、`crash_recovery_report.json`。设计与对照见 `docs/SPOT_LONG_ONLY.md`。
> 5. 完整报告：附件 `REPORT.md` / `docs/` 下的架构、接口、性能、验证与 AI 协作记录
> 6. 离线回放样本：`samples/demo_run`（无需交易所网络即可复现）
>
> 复现入口：
>
> ```bash
> python -m pip install -r requirements.txt
> python -m tickforge live --config config/default.toml --duration 900
> python -m tickforge verify --run artifacts/runs/<run_id>
> python -m tickforge replay --run artifacts/runs/<run_id> --verify
> ```
>
> 实测关键结果：抽象做市性能基准运行 15 分钟 388,388 帧（6 个交易对），原始帧丢失 0、队列全部排空；端到端延迟 p50 0.512 ms / p99 2.435 ms / p999 27.554 ms；WAL CRC/分段哈希校验通过，回放因子/信号/成交 SHA-256 与实时运行完全一致。另附现货只做多变体（同一 WAL 对照：负仓位/负现金均为 0），共 259 个测试通过。
>
> 感谢审阅，期待交流。
>
> 姓名 / 电话 / 邮箱

## 6. 提交前检查

- [x] `python -m pytest -q` 259 passed；
- [x] 15 分钟 `summary.clean_shutdown == true`；
- [x] `lost_raw_frames == 0`、`drained == true`；
- [x] `verify` 与 `replay --verify` 通过；
- [x] 报告中的数字与 `summary.json` 一致；
- [x] GitHub 仓库公开可访问，README 首屏有运行命令；
- [ ] Dockerfile 在干净环境构建成功（本机无 Docker；已用干净虚拟环境验证依赖与入口，见 RUNBOOK）；
- [ ] 邮件附件不含密钥、Cookie、个人路径。
