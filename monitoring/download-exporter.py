"""Fetch the exact standalone exporter artifact used in this lab."""
import hashlib
import pathlib
import urllib.request

target = pathlib.Path(__file__).resolve().parents[1] / '.cache' / 'cassandra-exporter-standalone-0.9.14.jar'
expected = '924078d5bf90fe0661b857668802fb375b4896c547c739dd3ce8ba6379ed602a'
target.parent.mkdir(exist_ok=True)
if not target.exists():
    url = 'https://github.com/instaclustr/cassandra-exporter/releases/download/v0.9.14/' + target.name
    with urllib.request.urlopen(url, timeout=120) as response:
        data = response.read()
    if hashlib.sha256(data).hexdigest() != expected:
        raise SystemExit('Unexpected SHA256; artifact not saved')
    target.write_bytes(data)
if hashlib.sha256(target.read_bytes()).hexdigest() != expected:
    raise SystemExit('Cached artifact SHA256 mismatch')
print(target)
