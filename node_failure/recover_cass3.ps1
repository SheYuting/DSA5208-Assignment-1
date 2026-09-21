$ErrorActionPreference = "Stop"

Write-Host "Starting cass3..."
docker start cass3 | Out-Host

Write-Host "Waiting for Cassandra on cass3 to become ready..."
$ready = $false

for ($i = 0; $i -lt 60; $i++) {
    docker exec cass3 cqlsh -e "SELECT release_version FROM system.local;" *> $null
    if ($LASTEXITCODE -eq 0) {
        $ready = $true
        break
    }
    Start-Sleep -Seconds 2
}

if (-not $ready) {
    throw "cass3 did not become CQL-ready within the wait period."
}

Write-Host "`ncass3 is CQL-ready."
Write-Host "`nCluster status:"
docker exec cass1 nodetool status
