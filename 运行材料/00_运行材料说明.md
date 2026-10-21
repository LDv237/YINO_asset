# 运行材料说明

本次提交的主结果：

- [`01_主结果_现货_9821/`](01_主结果_现货_9821/)：最终代码的 15 分钟**现货 live** run。`run_data.tar.xz` 里是 raw/ 和 derived/，其余是 summary/manifest/config/replay/logs/console；解压后可直接 `verify` / `replay --verify`。

对照：

- [`02_对照_抽象做市_3049/`](02_对照_抽象做市_3049/)：3049 抽象做市 run 的摘要、成交编号和活跃度；完整 run 在 Release 的 `YINO_artifacts_complete_20261020.zip`。

[`03_验证证据/`](03_验证证据/) 收录主结果的成交编号、活跃度、现金/仓位审计、`pytest`（276 passed）、crash 和 Docker 证据。

53b5、6db8、同一 WAL 现货对照等历史对照的完整目录、WAL、JSONL、CSV 见 Release；报告中的对照表汇总关键数字。

完整 `artifacts/` 目录约 1,011 MiB，含 7 个历史 run，见 GitHub Release；主结果完整数据在 `run_data.tar.xz`。说明见 `artifacts/README.md`。

主结果复现：

```bash
python scripts/extract_main_run.py 运行材料/01_主结果_现货_9821
python -m tickforge verify --run 运行材料/01_主结果_现货_9821
python -m tickforge replay --run 运行材料/01_主结果_现货_9821 \
  --out 运行材料/01_主结果_现货_9821-replay --verify
```
