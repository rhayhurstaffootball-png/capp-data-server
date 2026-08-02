# build_converter.ps1
# Usage: .\build_converter.ps1 -Version 1.1.0
#
# -Version stamps CONVERTER_VERSION into capp_binder_converter.py before
# building, the same way the Build Manager stamps AGENT_VERSION into
# capp_agent.py. Installed converters compare their stamped version against
# GET /converter/version and silently self-update when Render reports a higher
# one, so a release only reaches coaches when BOTH happen: the new exe is
# uploaded AND Render's CONVERTER_VERSION env var is bumped to match.
#
# Builds the CAPP Binder Converter - a single, invisible, per-coach background
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
# at "shared/CAPP_Binder_Converter.exe" - same bucket/pattern as
# CAPPNodes_Agent.exe - so /converter/download can serve it.

param(
    [string]$Version
)

Set-Location $PSScriptRoot

$OUT_NAME = "CAPP_Binder_Converter"
$SRC = "capp_binder_converter.py"

Write-Host ""
Write-Host "=== CAPP Binder Converter Builder ===" -ForegroundColor Cyan
Write-Host "  Output: dist\$OUT_NAME.exe" -ForegroundColor White
Write-Host "  Invisible: no window, no console, no tray - matches pb_worker.py" -ForegroundColor White
Write-Host ""

# Stamp CONVERTER_VERSION so the built exe knows what it is (same idea as
# stamp_agent_version() in capp_build_manager.py).
if ($Version) {
    if ($Version -notmatch '^\d+(\.\d+)*$') {
        Write-Host "ERROR: -Version must look like 1.2.3 (got '$Version')." -ForegroundColor Red
        exit 1
    }
    $txt = Get-Content $SRC -Raw
    $new = [regex]::Replace($txt, '(?m)^(CONVERTER_VERSION\s*=\s*)"[^"]+"', "`${1}""$Version""", 1)
    if ($new -eq $txt) {
        Write-Host "ERROR: could not find CONVERTER_VERSION in $SRC to stamp." -ForegroundColor Red
        exit 1
    }
    Set-Content $SRC -Value $new -Encoding UTF8 -NoNewline
    Write-Host "Stamped CONVERTER_VERSION = $Version" -ForegroundColor Green
} else {
    $cur = ([regex]::Match((Get-Content $SRC -Raw), '(?m)^CONVERTER_VERSION\s*=\s*"([^"]+)"')).Groups[1].Value
    Write-Host "No -Version given; building with CONVERTER_VERSION = $cur" -ForegroundColor Yellow
}
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
    Write-Host "WARNING: signing failed - $outFile was built but is UNSIGNED." -ForegroundColor Red
} else {
    Write-Host "Signed OK." -ForegroundColor Green
}

$sizeMB = [math]::Round((Get-Item $outFile).Length / 1MB, 1)
Write-Host ""
Write-Host "=== Done! ===" -ForegroundColor Green
Write-Host "  $((Get-Item $outFile).FullName)" -ForegroundColor Cyan
Write-Host "  $sizeMB MB" -ForegroundColor Cyan
Write-Host ""
$built = ([regex]::Match((Get-Content $SRC -Raw), '(?m)^CONVERTER_VERSION\s*=\s*"([^"]+)"')).Groups[1].Value
Write-Host "Next steps (BOTH required, or nobody gets this build):" -ForegroundColor Yellow
Write-Host "  1. Upload $outFile to the DO relay so" -ForegroundColor White
Write-Host "     https://relay.cappvcs.com/converter/download serves it." -ForegroundColor White
Write-Host "  2. Set CONVERTER_VERSION = $built on Render." -ForegroundColor White
Write-Host ""
Write-Host "  Installed converters compare their own version against" -ForegroundColor Gray
Write-Host "  /converter/version and self-update within 6 hours of idle time." -ForegroundColor Gray
Write-Host "  Skip step 2 and every coach silently stays on the old build." -ForegroundColor Gray
Write-Host ""
