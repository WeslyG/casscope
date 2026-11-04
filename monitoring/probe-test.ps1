# Host-side reproduction of the separate Casspoke / gossip test.
$ErrorActionPreference = 'Stop'
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    try {
        docker exec cassandra-lab-cassandra3-1 nodetool disablebinary
        if ($LASTEXITCODE -ne 0) { throw 'disablebinary failed' }
        Start-Sleep -Seconds 20
        docker exec cassandra-lab-cassandra1-1 nodetool status > research/probe-status-cql-disabled.txt
        docker exec cassandra-lab-cassandra1-1 python3 -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:9504/metrics',timeout=20).read().decode())" > research/casspoke-cql-disabled.prom
        docker exec cassandra-lab-cassandra1-1 python3 -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:9500/metrics',timeout=20).read().decode())" > research/probe-gossip-cql-disabled.prom
    } finally {
        docker exec cassandra-lab-cassandra3-1 nodetool enablebinary
    }
    docker exec cassandra-lab-cassandra3-1 nodetool statusbinary
} finally {
    Pop-Location
}
