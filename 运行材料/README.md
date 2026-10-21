# 运行材料

本次提交只放一个主结果：

- [`20261021T075925Z-3049/`](20261021T075925Z-3049/)：最终代码的 15 分钟 live 单源 run，`raw/`、`derived/`、`summary.json`、`manifest.json`、`config.json`、`replay_summary.json` 和日志齐全，可以直接 `verify` / `replay --verify`。

[`验证证据/`](验证证据/) 放主结果的成交编号、活跃度、`pytest` 和 crash/Docker 证据。

53b5、6db8 等历史 run 的完整目录、WAL、JSONL、CSV 在 GitHub Release 的 `YINO_artifacts_complete_20261020.zip`；报告里只保留对照表，不随邮件提交。

主结果复现：

```bash
python -m tickforge verify --run 运行材料/20261021T075925Z-3049
python -m tickforge replay --run 运行材料/20261021T075925Z-3049 \
  --out 运行材料/20261021T075925Z-3049-replay --verify
```
