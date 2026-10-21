# 抽象做市对照：20261021T075925Z-3049

3049 是最终代码的抽象做市 live 单源 run，用作执行语义对照；本次主结果是最终代码的现货 live run。这里只保留摘要、成交编号和活跃度，完整 raw/derived/logs/replay 在 GitHub Release 的 `YINO_artifacts_complete_20261020.zip` 内 `artifacts/runs/20261021T075925Z-3049/`。

- 3049 允许负仓位/负现金：1 个 symbol 负仓位、6 个 symbol 负现金（期末 cash 最小值 -9,340.86）。
- 主结果（现货）所有 symbol position/cash ≥ 0，见 `运行材料/01_主结果_现货_9821/`。
