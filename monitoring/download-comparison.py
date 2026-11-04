"""Download the version-pinned artifacts used by the comparison."""
import hashlib
import json
import pathlib
import urllib.request

root = pathlib.Path(__file__).resolve().parents[1]
cache = root / '.cache'
cache.mkdir(exist_ok=True)
locations = {
    'cassandra_exporter-2.3.8.jar': 'criteo/cassandra_exporter/releases/download/2.3.8',
    'cassandra-exporter-agent-0.9.14.jar': 'instaclustr/cassandra-exporter/releases/download/v0.9.14',
    'cassandra-exporter-standalone-0.9.14.jar': 'instaclustr/cassandra-exporter/releases/download/v0.9.14',
    'casspoke-1.3.6.jar': 'criteo/casspoke/releases/download/1.3.6',
    'jmx_prometheus_standalone-1.7.0.jar': 'prometheus/jmx_exporter/releases/download/1.7.0',
    'datastax-mcac-agent-0.3.6-4.1-beta1.tar.gz': 'datastax/metric-collector-for-apache-cassandra/releases/download/v0.3.6',
}
for artifact in json.loads((root / 'research/artifact-hashes.json').read_text(encoding='utf-8-sig')):
    name = artifact['name']
    expected = artifact['Hash'].lower()
    path = cache / name
    data = path.read_bytes() if path.exists() else urllib.request.urlopen(
        f'https://github.com/{locations[name]}/{name}', timeout=120).read()
    if hashlib.sha256(data).hexdigest() != expected:
        raise SystemExit(f'SHA256 mismatch: {name}')
    if not path.exists():
        path.write_bytes(data)
    print(f'OK {name}')
