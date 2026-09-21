$ErrorActionPreference = "Stop"

function Get-ContainerIP($name) {
    $ip = docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" $name
    if (-not $ip) {
        throw "Could not determine IP for $name"
    }
    return $ip.Trim()
}

$cass1 = Get-ContainerIP "cass1"
$cass2 = Get-ContainerIP "cass2"

Write-Host "Healing cass3 network partition..."

$commands = @(
    "iptables -D INPUT  -s $cass1 -j DROP 2>/dev/null || true",
    "iptables -D OUTPUT -d $cass1 -j DROP 2>/dev/null || true",
    "iptables -D INPUT  -s $cass2 -j DROP 2>/dev/null || true",
    "iptables -D OUTPUT -d $cass2 -j DROP 2>/dev/null || true"
)

foreach ($cmd in $commands) {
    docker exec -u 0 cass3 sh -lc $cmd
}

Write-Host "`nRemaining cass3 firewall rules:"
docker exec -u 0 cass3 sh -lc "iptables -S INPUT; echo '---'; iptables -S OUTPUT"

Write-Host "`nWaiting briefly for gossip/cluster membership to converge..."
Start-Sleep -Seconds 10

Write-Host "`nCluster status from cass1:"
docker exec cass1 nodetool status

Write-Host "`nPartition healed."
