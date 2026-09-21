$ErrorActionPreference = "Continue"

Write-Host "=== Docker containers ==="
docker compose ps -a

Write-Host "`n=== Cassandra cluster from cass1 ==="
docker exec cass1 nodetool status

Write-Host "`n=== Host CQL ports ==="
foreach ($p in 9042, 9043, 9044) {
    $r = Test-NetConnection 127.0.0.1 -Port $p -WarningAction SilentlyContinue
    Write-Host ("127.0.0.1:{0} -> {1}" -f $p, $r.TcpTestSucceeded)
}
