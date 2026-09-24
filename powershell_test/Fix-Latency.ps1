$ErrorActionPreference = "Stop"

Write-Host "Removing network latency from cass3..."
docker exec --privileged cass3 tc qdisc del dev eth0 root | Out-Host

Write-Host "`nNetwork latency removed."
