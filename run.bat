@echo off
rem Start the whole RateGuard-DC cluster (no Docker, no Kubernetes): 3 coordinators,
rem 3 edge nodes, the load balancer, the upstream API and the console.
rem Ctrl+C stops everything. Pass --no-browser to skip opening the console.
cd /d "%~dp0"
if not exist target\rateguard-dc.jar (
  echo Building with Maven - the first run downloads dependencies...
  call mvn -q -B package -DskipTests || exit /b 1
)
java -jar target\rateguard-dc.jar launcher %*
