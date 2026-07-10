# clean_reinstall.ps1
# Fully wipes any trace of the CAPP Binder Converter from this machine so the
# next "Complete Setup" download is a truly clean install — no stray running
# processes, no leftover install folder/pairing, no autostart entry, no old
# downloaded copies lying around to accidentally double-click.
#
# Usage: right-click -> Run with PowerShell (or run from a PowerShell prompt)

Write-Host ""
Write-Host "=== CAPP Binder Converter - Clean Reinstall ===" -ForegroundColor Cyan
Write-Host ""

# 1) Kill every running copy
Write-Host "Stopping any running CAPP_Binder_Converter.exe..." -ForegroundColor Yellow
$procs = Get-CimInstance Win32_Process -Filter "Name = 'CAPP_Binder_Converter.exe'"
if ($procs) {
    foreach ($p in $procs) {
        Write-Host "  Killing PID $($p.ProcessId)"
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 1
} else {
    Write-Host "  (none running)"
}

# 2) Delete the installed app folder (installed exe copy, device_token.json
#    pairing, converter.log)
$appDir = Join-Path $env:LOCALAPPDATA "CAPP Binder Converter"
if (Test-Path $appDir) {
    Write-Host "Deleting $appDir ..." -ForegroundColor Yellow
    Remove-Item -Path $appDir -Recurse -Force -ErrorAction SilentlyContinue
} else {
    Write-Host "No installed app folder found (already clean)."
}

# 3) Remove the Windows auto-start entry
Write-Host "Removing auto-start registry entry..." -ForegroundColor Yellow
try {
    Remove-ItemProperty -Path "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run" `
        -Name "CAPPBinderConverter" -ErrorAction SilentlyContinue
} catch {}

# 4) Clean up any downloaded copies sitting around (Downloads, Desktop, and
#    OneDrive-redirected Desktop, since this machine has one)
Write-Host "Removing old downloaded copies..." -ForegroundColor Yellow
$searchDirs = @(
    (Join-Path $env:USERPROFILE "Downloads"),
    (Join-Path $env:USERPROFILE "Desktop"),
    (Join-Path $env:USERPROFILE "OneDrive - USAFA (AFAcademy.AF.edu)\Desktop")
)
foreach ($dir in $searchDirs) {
    if (Test-Path $dir) {
        Get-ChildItem -Path $dir -Filter "CAPP_Binder_Converter*.exe" -File -ErrorAction SilentlyContinue |
            ForEach-Object {
                Write-Host "  Deleting $($_.FullName)"
                Remove-Item $_.FullName -Force -ErrorAction SilentlyContinue
            }
    }
}

Write-Host ""
Write-Host "=== Done. This machine is now fully unpaired and clean. ===" -ForegroundColor Green
Write-Host "Go back to the Binder, sign in, and 'Complete Setup' will fire again for a fresh install." -ForegroundColor White
Write-Host ""
