# 运行材料

本次提交的主结果：

- [`20261021T162501Z-9821/`](20261021T162501Z-9821/)：最终代码的 15 分钟**现货只做多 live** run。`run_data.tar.xz` 里是 raw/ 和 derived/，其余是 summary/manifest/config/replay/logs/console；解压后可直接 `verify` / `replay --verify`。

对照：

- [`对照_抽象做市_3049/`](对照_抽象做市_3049/)：3049 抽象做市 run 的摘要、成交编号和活跃度；完整 run 在 Release 的 `YINO_artifacts_complete_20261020.zip`。

[`验证证据/`](验证证据/) 放主结果的成交编号、活跃度、现金/仓位审计、`pytest`（276 passed）、crash 和 Docker 证据。

53b5、6db8、同一 WAL 只做多等历史对照的完整目录、WAL、JSONL、CSV 在 Release；报告里只保留对照表。

**本邮件包不含完整 `artifacts/`**（约 1,011 MiB，7 个历史 run）；主结果完整数据在 `run_data.tar.xz`，完整历史 artifacts 在 Release。详见包根目录的 `artifacts/README.md`。

主结果复现：

```bash
python scripts/extract_main_run.py 运行材料/20261021T162501Z-9821
python -m tickforge verify --run 运行材料/20261021T162501Z-9821
python -m tickforge replay --run 运行材料/20261021T162501Z-9821 \
  --out 运行材料/20261021T162501Z-9821-replay --verify
```
