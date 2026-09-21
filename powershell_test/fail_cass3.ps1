$ErrorActionPreference = "Stop"

Write-Host "Stopping cass3 to simulate node failure..."
docker stop cass3 | Out-Host

Write-Host "`nContainer status:"
docker compose ps -a

Write-Host "`nCluster view from cass1:"
docker exec cass1 nodetool status

Write-Host "`nNode failure injected."
