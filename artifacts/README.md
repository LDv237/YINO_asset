# 关于 artifacts/

本邮件提交包**不包含完整的 `artifacts/` 目录**（约 1,011 MiB：7 个 15 分钟 run + exports + experiments + summaries + evidence + logs）。这是为了把邮件附件控制在 16–17 MiB，避免评审下载一个 193MB 的压缩包。

主结果 `20261021T075925Z-3049` 的完整 run 已经放在邮件包里：

```text
运行材料/20261021T075925Z-3049/
  raw/                2 个 WAL 分段，303,793 条记录
  derived/            factors / signals / fills JSONL
  summary.json        计数、延迟、资源、对账、digests
  manifest.json       WAL 分段和 derived 的 SHA-256
  config.json         本次运行配置
  replay_summary.json replay --verify 结果
  logs/ console.log   结构化日志 / 控制台日志
```

验证主结果不需要 `artifacts/`：

```bash
python -m tickforge verify --run 运行材料/20261021T075925Z-3049
python -m tickforge replay --run 运行材料/20261021T075925Z-3049 \
  --out 运行材料/20261021T075925Z-3049-replay --verify
```

完整的 `artifacts/`（7 个 run 目录、exports、experiments、summaries、evidence、logs）在 GitHub Release：

```text
https://github.com/LDv237/YINO_asset/releases/tag/delivery-20261020
YINO_artifacts_complete_20261020.zip
193,199,867 bytes
SHA-256 c77ccc734151a5a2033b2fa8b62db20c2f3d53377964e0207a87971a77d12c8b
```

说明：文档里的 `artifacts/runs/<run_id>` 路径指 Release/仓库的完整 artifacts，不是本邮件包内的路径；本包内请用 `运行材料/20261021T075925Z-3049/`。
