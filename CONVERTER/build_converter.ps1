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
# ---------------------------------------------------------------------------
# Generate the Windows version resource from CONVERTER_VERSION so the built EXE
# identifies itself in Explorer -> Properties -> Details.
#
# Aug 19 2026: three converter EXEs were sitting in one folder with NO version
# on any of them, and two July builds were still running and failing every
# Excel job. Nothing short of running them told you which was which.
# ---------------------------------------------------------------------------
$cv = Select-String -Path "capp_binder_converter.py" -Pattern '^CONVERTER_VERSION *= *"([^"]+)"' | Select-Object -First 1
if ($cv) { $CONV_VER = $cv.Matches[0].Groups[1].Value } else { $CONV_VER = "0.0.0" }
$cp = [System.Collections.ArrayList]@($CONV_VER.Split("."))
while ($cp.Count -lt 4) { [void]$cp.Add("0") }
$ctup  = "($($cp[0]), $($cp[1]), $($cp[2]), $($cp[3]))"
$cvstr = "$CONV_VER.0"
@"
VSVersionInfo(
  ffi=FixedFileInfo(
    filevers=$ctup,
    prodvers=$ctup,
    mask=0x3f,
    flags=0x0,
    OS=0x40004,
    fileType=0x1,
    subtype=0x0,
    date=(0, 0)
  ),
  kids=[
    StringFileInfo(
      [
        StringTable(
          u'040904B0',
          [StringStruct(u'CompanyName', u'CAPP Solutions LLC'),
           StringStruct(u'FileDescription', u'CAPP Binder Converter'),
           StringStruct(u'FileVersion', u'$cvstr'),
           StringStruct(u'InternalName', u'CAPP_Binder_Converter'),
           StringStruct(u'LegalCopyright', u'Copyright (c) 2026 CAPP Solutions LLC'),
           StringStruct(u'OriginalFilename', u'CAPP_Binder_Converter.exe'),
           StringStruct(u'ProductName', u'CAPP Binder Converter'),
           StringStruct(u'ProductVersion', u'$cvstr')]
        )
      ]
    ),
    VarFileInfo([VarStruct(u'Translation', [1033, 1200])])
  ]
)
"@ | Out-File -FilePath "converter_version_info.txt" -Encoding utf8
Write-Host "  Version resource: $CONV_VER" -ForegroundColor White

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

# Publish the version to Render automatically. This used to be a manual step
# printed in yellow, and it was never once done - CONVERTER_VERSION was not
# even set on the service, so /converter/version served the hardcoded default
# and no installed converter ever saw an update. The Build Manager already does
# this for APP_VERSION and AGENT_VERSION; same idea, same credentials file.
$bump = Join-Path $PSScriptRoot "..\..\CAPP_FINAL\_dev_tools\bump_render_env.py"
$bumped = $false
if (Test-Path $bump) {
    Write-Host "Publishing CONVERTER_VERSION = $built to Render ..." -ForegroundColor Yellow
    python $bump CONVERTER_VERSION $built
    if ($LASTEXITCODE -eq 0) {
        $bumped = $true
    } else {
        Write-Host "WARNING: could not set CONVERTER_VERSION on Render." -ForegroundColor Red
    }
} else {
    Write-Host "WARNING: bump_render_env.py not found at $bump" -ForegroundColor Red
}

Write-Host ""
if ($bumped) {
    Write-Host "Remaining step:" -ForegroundColor Yellow
    Write-Host "  Upload $outFile to the DO relay so" -ForegroundColor White
    Write-Host "  https://relay.cappvcs.com/converter/download serves it." -ForegroundColor White
    Write-Host ""
    Write-Host "  CONVERTER_VERSION = $built is already live on Render, so upload the" -ForegroundColor Gray
    Write-Host "  exe promptly - converters will ask for it within 6 hours of idle time." -ForegroundColor Gray
} else {
    Write-Host "Next steps (BOTH required, or nobody gets this build):" -ForegroundColor Yellow
    Write-Host "  1. Upload $outFile to the DO relay so" -ForegroundColor White
    Write-Host "     https://relay.cappvcs.com/converter/download serves it." -ForegroundColor White
    Write-Host "  2. Set CONVERTER_VERSION = $built on Render, or run:" -ForegroundColor White
    Write-Host "     python ..\..\CAPP_FINAL\_dev_tools\bump_render_env.py CONVERTER_VERSION $built" -ForegroundColor White
    Write-Host ""
    Write-Host "  Skip step 2 and every coach silently stays on the old build." -ForegroundColor Gray
}
Write-Host ""
