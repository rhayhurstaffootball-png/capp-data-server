# Test 1 - Save a file (should return 200 with "saved")
Write-Host "TEST 1: Saving test file..." -ForegroundColor Cyan
Invoke-WebRequest `
    -Uri "https://api.cappvcs.com/storage/save?client_id=test&filename=test.json" `
    -Method POST `
    -Headers @{"X-API-Key"="3f58fbec-3222-447b-8ddd-1103df95d6af";"Content-Type"="application/json"} `
    -Body '{"test":"hello"}' `
    -UseBasicParsing

# Test 2 - Load it back (should return {"test":"hello"})
Write-Host "`nTEST 2: Loading test file back..." -ForegroundColor Cyan
Invoke-WebRequest `
    -Uri "https://api.cappvcs.com/storage/load?client_id=test&filename=test.json" `
    -Headers @{"X-API-Key"="3f58fbec-3222-447b-8ddd-1103df95d6af"} `
    -UseBasicParsing

# Test 3 - No API key (should return 401)
Write-Host "`nTEST 3: No API key (expect 401)..." -ForegroundColor Cyan
try {
    Invoke-WebRequest `
        -Uri "https://api.cappvcs.com/storage/load?client_id=test&filename=test.json" `
        -UseBasicParsing
} catch {
    Write-Host "Got expected error: $($_.Exception.Message)" -ForegroundColor Green
}
