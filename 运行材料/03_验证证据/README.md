# 验证证据（主结果相关）

本目录收录与主结果 `20261021T162501Z-9821` 直接相关的证据：

| 文件 | 内容 |
|---|---|
| `pytest_276_passed.txt` | 完整测试套件：276 passed |
| `成交编号完整性.json` | 主结果 6 个 symbol 的 `pipeline.trade_quality`：0 duplicates / 0 backwards / 0 gaps / 0 missing_ids |
| `交易活跃度.json` | 主结果起始小时桶（16:00–17:00 UTC）的活跃度排名：9/47，82.98 分位 |
| `现金与仓位审计.json` | 逐笔 running min cash、min position、期末 cash/position：全部 ≥ 0 |
| `异常终止恢复.json` | 应用层强制终止后的 WAL 可读前缀（229 条记录）；完整 crash WAL 在 Release |
| `docker_ci/` | 60 秒容器 live 的关键证据（节选）；完整 build/日志在 run 39654447001 的 GitHub Actions artifact `docker-verify-evidence`（Release 的 artifacts zip 内为 2026-10-20 的 Docker 证据） |

说明：

- 3049 抽象做市的成交编号和活跃度在 `../02_对照_抽象做市_3049/`；历史 run（53b5/6db8）的完整证据在 GitHub Release。
- 异常终止实验只证明应用层强杀后的 WAL 可读前缀，不等同于物理掉电或系统崩溃恢复安全。
