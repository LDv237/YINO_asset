# 验证证据（主结果相关）

这里只放与主结果 `3049` 直接相关的证据：

| 文件 | 内容 |
|---|---|
| `pytest_274_passed.txt` | 完整测试套件：274 passed |
| `成交编号完整性_最终版.json` | 3049 的 `pipeline.trade_quality`：6 个 symbol 均 0 duplicates / 0 backwards / 0 gaps / 0 missing_ids |
| `交易活跃度_最终版.json` | 3049 起始小时桶（07:00–08:00 UTC）的市场活跃度排名 |
| `异常终止恢复.json` | 应用层强制终止后的 WAL 可读前缀（229 条记录）；完整 crash WAL 在 Release |
| `docker_ci/` | 60 秒容器 live 的关键证据（节选）；完整 build/日志在 Release |

说明：

- 历史 run（53b5 / 6db8）的现金、成交编号、活跃度证据不放在邮件包里，避免把多套口径混在一起；完整证据在 GitHub Release 的 `YINO_artifacts_complete_20261020.zip`。
- 异常终止实验只证明应用层强杀后的 WAL 可读前缀，不等同于物理掉电或系统崩溃恢复安全。
