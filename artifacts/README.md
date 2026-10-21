# 关于 artifacts/

本邮件提交包**不包含完整的 `artifacts/` 目录**（约 1,011 MiB：7 个历史 run + exports + experiments + summaries + evidence + logs）。这是为了把邮件附件控制在 25 MiB 内，避免评审下载一个 193MB 的压缩包。

主结果 `20261021T162501Z-9821` 的完整 run 已经放在邮件包里：

```text
运行材料/01_主结果_现货_9821/
  run_data.tar.xz    raw/ 3 个 WAL 分段（650,542 条记录）+ derived/
  summary.json       计数、延迟、资源、对账、digests
  manifest.json      WAL 分段和 derived 的 SHA-256
  config.json        本次运行配置（spot_long_only）
  replay_summary.json replay --verify 结果
  logs/ console.log  结构化日志 / 控制台日志
```

先解压再验证：

```bash
python scripts/extract_main_run.py 运行材料/01_主结果_现货_9821
python -m tickforge verify --run 运行材料/01_主结果_现货_9821
python -m tickforge replay --run 运行材料/01_主结果_现货_9821 \
  --out 运行材料/01_主结果_现货_9821-replay --verify
```

完整的 `artifacts/`（7 个历史 run 目录、exports、experiments、summaries、evidence、logs）在 GitHub Release：

https://github.com/LDv237/YINO_asset/releases/tag/delivery-20261020

文件名 `YINO_artifacts_complete_20261020.zip`；大小和 SHA-256 以 Release 里的 `SHA256SUMS.txt` 为准，下载后先校验。本 zip 内的 `artifacts/目录说明.md` 是历史产物快照，当前主结果在邮件包的 `运行材料/01_主结果_现货_9821/`。

说明：文档里的 `artifacts/runs/<run_id>` 路径指 Release/仓库的完整 artifacts，不是本邮件包内的路径；本包内请用 `运行材料/01_主结果_现货_9821/`。
