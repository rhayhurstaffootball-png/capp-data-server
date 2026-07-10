# build_converter.ps1
# Usage: .\build_converter.ps1
#
# Builds the CAPP Binder Converter — a single, invisible, per-coach background
# EXE (no window/console/tray, matches pb_worker.py's own behavior exactly).
# One universal EXE for every coach: it self-installs to %LocalAppData%,
# registers to auto-start with Windows, and pairs itself using the
# pairing_token.txt dropped alongside it by the Binder's "Complete Setup"
# screen. See "T:\BINDER LOCAL PLAN.txt".
#
# Python deps required (dev machine building this, NOT each coach's PC):
#   pip install pyinstaller pywin32 PyMuPDF python-pptx
#
# After building, sign it (same Azure Artifact Signing pipeline as the rest of
# CAPP), then upload the signed EXE to Supabase storage bucket "capp-workflow"
# at "shared/CAPP_Binder_Converter.exe" — same bucket/pattern as
# CAPPNodes_Agent.exe — so /converter/download can serve it.

Set-Location $PSScriptRoot

$OUT_NAME = "CAPP_Binder_Converter"

Write-Host ""
Write-Host "=== CAPP Binder Converter Builder ===" -ForegroundColor Cyan
Write-Host "  Output: dist\$OUT_NAME.exe" -ForegroundColor White
Write-Host "  Invisible: no window, no console, no tray — matches pb_worker.py" -ForegroundColor White
Write-Host ""

Write-Host "Stopping any running converter instances..." -ForegroundColor Yellow
Stop-Process -Name $OUT_NAME -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 1

Write-Host ""
Write-Host "Building $OUT_NAME.exe..." -ForegroundColor Yellow
pyinstaller --noconfirm "CAPP_Binder_Converter.spec"

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Build failed." -ForegroundColor Red
    exit 1
}

$outFile = "dist\$OUT_NAME.exe"
if (-not (Test-Path $outFile)) {
    Write-Host "ERROR: expected output not found at $outFile" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Signing $outFile ..." -ForegroundColor Yellow
powershell -NoProfile -ExecutionPolicy Bypass -File "..\..\CAPP_FINAL\sign_capp.ps1" $outFile
if ($LASTEXITCODE -ne 0) {
    Write-Host "WARNING: signing failed — $outFile was built but is UNSIGNED." -ForegroundColor Red
} else {
    Write-Host "Signed OK." -ForegroundColor Green
}

$sizeMB = [math]::Round((Get-Item $outFile).Length / 1MB, 1)
Write-Host ""
Write-Host "=== Done! ===" -ForegroundColor Green
Write-Host "  $((Get-Item $outFile).FullName)" -ForegroundColor Cyan
Write-Host "  $sizeMB MB" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next step (manual, one-time per release):" -ForegroundColor Yellow
Write-Host "  Upload $outFile to Supabase Storage bucket 'capp-workflow' at" -ForegroundColor White
Write-Host "  'shared/CAPP_Binder_Converter.exe' (same bucket CAPPNodes_Agent.exe uses)." -ForegroundColor White
Write-Host "  Then GET /converter/download on the server serves it." -ForegroundColor White
Write-Host ""
