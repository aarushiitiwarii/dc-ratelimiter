@echo off
rem Stops every RateGuard-DC JVM (use if a run was not closed with Ctrl+C).
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='java.exe'\" | Where-Object { $_.CommandLine -like '*rateguard-dc.jar*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force; Write-Host ('stopped pid ' + $_.ProcessId) }"
