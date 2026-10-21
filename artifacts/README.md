# 关于 artifacts/

完整 `artifacts/` 目录约 1,011 MiB（7 个历史 run 及 exports、experiments、summaries、evidence、logs），压缩后约 193 MB，超过 25 MiB 的附件限制，改由 GitHub Release 提供。

主结果 `20261021T162501Z-9821` 的完整 run 位于 `运行材料/01_主结果_现货_9821/`：

```text
运行材料/01_主结果_现货_9821/
  run_data.tar.xz    raw/ 3 个 WAL 分段（650,542 条记录）+ derived/
  summary.json       计数、延迟、资源、对账、digests
  manifest.json      WAL 分段和 derived 的 SHA-256
  config.json        本次运行配置（spot_long_only）
  replay_summary.json replay --verify 结果
  logs/ console.log  结构化日志 / 控制台日志
```

解压后可执行以下命令：

```bash
python scripts/extract_main_run.py 运行材料/01_主结果_现货_9821
python -m tickforge verify --run 运行材料/01_主结果_现货_9821
python -m tickforge replay --run 运行材料/01_主结果_现货_9821 \
  --out 运行材料/01_主结果_现货_9821-replay --verify
```

完整的 `artifacts/`（7 个历史 run 目录、exports、experiments、summaries、evidence、logs）在 GitHub Release：

https://github.com/LDv237/YINO_asset/releases/tag/delivery-20261020

文件名 `YINO_artifacts_complete_20261020.zip`，大小和 SHA-256 以 Release 的 `SHA256SUMS.txt` 为准。Release 的 artifacts 压缩包内 `artifacts/目录说明.md` 对应 2026-10-20 的产物状态；本次主结果位于 `运行材料/01_主结果_现货_9821/`。

说明：文档中的 `artifacts/runs/<run_id>` 路径指 Release 与仓库中的完整 artifacts；本次提交中对应路径为 `运行材料/01_主结果_现货_9821/`。
