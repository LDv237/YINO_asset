# 15-minute live measurement run. Extra arguments are forwarded.
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
$env:PYTHONUTF8 = "1"
python -m tickforge live --config config/default.toml --duration 900 --out artifacts @args
exit $LASTEXITCODE
