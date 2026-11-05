# One command to check the whole project on Windows:
#   powershell -ExecutionPolicy Bypass -File scripts\verify.ps1
# Starts PostgreSQL in Docker, migrates, runs lint + all tests + the demos, prints a summary.
$ErrorActionPreference = "Continue"
Set-Location (Split-Path $PSScriptRoot -Parent)
$results = [ordered]@{}

function Step($name, [scriptblock]$block) {
    Write-Host "`n=== $name ===" -ForegroundColor Cyan
    & $block
    $results[$name] = ($LASTEXITCODE -eq 0)
}

if (Test-Path ".venv\Scripts\Activate.ps1") { . .venv\Scripts\Activate.ps1 }

Step "Start PostgreSQL" { docker compose up -d postgres; Start-Sleep -Seconds 8 }
Step "Migrate" { alembic upgrade head }
Step "Lint (ruff)" { ruff check . }
Step "All tests (about 4 minutes)" { python -m pytest -q }
foreach ($demo in "demo_crash_recovery", "demo_duplicate_refund", "demo_multi_worker",
                  "demo_zombie_worker", "demo_durable_waiting", "demo_forking") {
    Step "Demo: $demo" { python "scripts\$demo.py" }
}

Write-Host "`n=== SUMMARY ===" -ForegroundColor Cyan
$failed = 0
foreach ($k in $results.Keys) {
    if ($results[$k]) { Write-Host "  PASS  $k" -ForegroundColor Green }
    else { Write-Host "  FAIL  $k" -ForegroundColor Red; $failed++ }
}
if ($failed -eq 0) { Write-Host "`nEverything passed. Now run: git push" -ForegroundColor Green }
else { Write-Host "`n$failed step(s) failed. Copy this window's output back to Claude." -ForegroundColor Red }
