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
$cass3 = Get-ContainerIP "cass3"

Write-Host "cass1 = $cass1"
Write-Host "cass2 = $cass2"
Write-Host "cass3 = $cass3"

Write-Host "`nChecking iptables inside cass3..."
docker exec -u 0 cass3 sh -lc "command -v iptables >/dev/null 2>&1"
if ($LASTEXITCODE -ne 0) {
    Write-Host "iptables not found. Installing it inside cass3..."
    docker exec -u 0 cass3 sh -lc "apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y iptables"
    if ($LASTEXITCODE -ne 0) {
        throw "Could not install iptables inside cass3."
    }
}

Write-Host "`nInjecting network partition: cass3 <-> cass1/cass2"

$commands = @(
    "iptables -C INPUT  -s $cass1 -j DROP 2>/dev/null || iptables -I INPUT 1 -s $cass1 -j DROP",
    "iptables -C OUTPUT -d $cass1 -j DROP 2>/dev/null || iptables -I OUTPUT 1 -d $cass1 -j DROP",
    "iptables -C INPUT  -s $cass2 -j DROP 2>/dev/null || iptables -I INPUT 1 -s $cass2 -j DROP",
    "iptables -C OUTPUT -d $cass2 -j DROP 2>/dev/null || iptables -I OUTPUT 1 -d $cass2 -j DROP"
)

foreach ($cmd in $commands) {
    docker exec -u 0 cass3 sh -lc $cmd
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to apply partition rule: $cmd"
    }
}

Write-Host "`nPartition rules on cass3:"
docker exec -u 0 cass3 sh -lc "iptables -S INPUT; echo '---'; iptables -S OUTPUT"

Write-Host "`nChecking that cass3 is still reachable from the Windows host on port 9044..."
$test = Test-NetConnection 127.0.0.1 -Port 9044 -WarningAction SilentlyContinue

if (-not $test.TcpTestSucceeded) {
    Write-Warning "Port 9044 is not reachable. This is not the desired partition topology."
} else {
    Write-Host "Host -> cass3:9044 is still reachable."
}

Write-Host "`nNetwork partition injected."
Write-Host "Now return to consistency_suite_fixed.py and press ENTER."
