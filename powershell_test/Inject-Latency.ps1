$ErrorActionPreference = "Stop"

Write-Host "Adding network latency to cass3..."
docker exec --privileged cass3 tc qdisc add dev eth0 root netem delay 500ms 50ms | Out-Host

Write-Host "`nNetwork latency added."
