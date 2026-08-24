# clean_reinstall.ps1
# Fully wipes any trace of the CAPP Binder Converter from this machine so the
# next "Complete Setup" download is a truly clean install - no stray running
# processes, no leftover install folder/pairing, no autostart entry, no old
# downloaded copies lying around to accidentally double-click.
#
# Usage: right-click -> Run with PowerShell (or run from a PowerShell prompt)
#
# WHY THIS EXISTS (and why it must match loosely, not exactly):
# Installs before Aug 19 2026 used whatever filename the browser gave the
# download. Download twice and you get "CAPP_Binder_Converter (1).exe", a third
# time "(4).exe" - and EACH one registered its own autostart entry and kept
# RUNNING. Those old copies keep claiming Binder jobs and failing them, while
# the freshly installed build sits there and never gets any work. The symptom
# is "I installed the converter but uploads take forever and it keeps asking me
# to install it again."
#
# So every step below matches CAPP_Binder_Converter*.exe, never just the exact
# name. Killing only the canonical name reports "none running" on precisely the
# machines that have the problem.

Write-Host ""
Write-Host "=== CAPP Binder Converter - Clean Reinstall ===" -ForegroundColor Cyan
Write-Host ""

# 1) Kill every running copy - ANY name variant, not just the canonical one.
Write-Host "Stopping any running converter (all name variants)..." -ForegroundColor Yellow
$procs = Get-CimInstance Win32_Process |
    Where-Object { $_.Name -like "CAPP_Binder_Converter*.exe" }
if ($procs) {
    foreach ($p in $procs) {
        Write-Host "  Killing $($p.Name)  PID $($p.ProcessId)  ->  $($p.ExecutablePath)"
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 2
} else {
    Write-Host "  (none running)"
}

# 2) Delete the installed app folder (installed exe copy, device_token.json
#    pairing, converter.log)
$appDir = Join-Path $env:LOCALAPPDATA "CAPP Binder Converter"
if (Test-Path $appDir) {
    Write-Host "Deleting $appDir ..." -ForegroundColor Yellow
    Remove-Item -Path $appDir -Recurse -Force -ErrorAction SilentlyContinue
    if (Test-Path $appDir) {
        Write-Host "  WARNING: could not fully delete - something is still holding it open." -ForegroundColor Red
        Write-Host "  Sign out and back in, then run this again." -ForegroundColor Red
    }
} else {
    Write-Host "No installed app folder found (already clean)."
}

# 3) Remove EVERY auto-start entry, not just the expected name.
#    Each old install registered its own, so there can be several.
Write-Host "Removing auto-start entries..." -ForegroundColor Yellow
$runKey = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run"
$removed = 0
try {
    $props = Get-ItemProperty -Path $runKey -ErrorAction SilentlyContinue
    if ($props) {
        foreach ($p in $props.PSObject.Properties) {
            if ($p.Name -like "*BinderConverter*" -or
                $p.Name -like "*CAPP_Binder*" -or
                ($p.Value -is [string] -and $p.Value -like "*CAPP Binder Converter*")) {
                Write-Host "  Removing '$($p.Name)'  ->  $($p.Value)"
                Remove-ItemProperty -Path $runKey -Name $p.Name -ErrorAction SilentlyContinue
                $removed++
            }
        }
    }
} catch {}
if ($removed -eq 0) { Write-Host "  (none found)" }

# 4) Clean up any downloaded copies sitting around (Downloads, Desktop, and
#    OneDrive-redirected Desktop, since these machines have one)
Write-Host "Removing old downloaded copies..." -ForegroundColor Yellow
$searchDirs = @(
    (Join-Path $env:USERPROFILE "Downloads"),
    (Join-Path $env:USERPROFILE "Desktop"),
    (Join-Path $env:USERPROFILE "OneDrive - USAFA (AFAcademy.AF.edu)\Desktop")
)
# Also catch any other OneDrive-redirected Desktop, whatever the tenant is named.
Get-ChildItem -Path $env:USERPROFILE -Directory -Filter "OneDrive*" -ErrorAction SilentlyContinue |
    ForEach-Object { $searchDirs += (Join-Path $_.FullName "Desktop") }

$deleted = 0
foreach ($dir in ($searchDirs | Select-Object -Unique)) {
    if (Test-Path $dir) {
        Get-ChildItem -Path $dir -Filter "CAPP_Binder_Converter*.exe" -File -ErrorAction SilentlyContinue |
            ForEach-Object {
                Write-Host "  Deleting $($_.FullName)"
                Remove-Item $_.FullName -Force -ErrorAction SilentlyContinue
                $deleted++
            }
    }
}
if ($deleted -eq 0) { Write-Host "  (none found)" }

# 5) Prove it worked. Saying "done" without checking is how a machine gets
#    declared clean while an old copy is still running.
Write-Host ""
Write-Host "Verifying..." -ForegroundColor Yellow
$still = Get-CimInstance Win32_Process |
    Where-Object { $_.Name -like "CAPP_Binder_Converter*.exe" }
$folderLeft = Test-Path $appDir
if ($still) {
    Write-Host "  STILL RUNNING:" -ForegroundColor Red
    foreach ($p in $still) { Write-Host "    $($p.Name)  PID $($p.ProcessId)" -ForegroundColor Red }
} else {
    Write-Host "  No converter processes running." -ForegroundColor Green
}
if ($folderLeft) {
    Write-Host "  Install folder still present: $appDir" -ForegroundColor Red
} else {
    Write-Host "  Install folder removed." -ForegroundColor Green
}

Write-Host ""
if (-not $still -and -not $folderLeft) {
    Write-Host "=== Done. This machine is now fully unpaired and clean. ===" -ForegroundColor Green
    Write-Host "Go back to the Binder, sign in, and 'Complete Setup' will fire again for a fresh install." -ForegroundColor White
} else {
    Write-Host "=== NOT fully clean - see the red lines above. ===" -ForegroundColor Red
    Write-Host "Usually means a copy is running as another user, or a file is locked." -ForegroundColor White
}
Write-Host ""
