"""Run read-only scrapes and reversible fault tests against this lab only."""
import json
import pathlib
import statistics
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / 'research'
TARGET = 'cassandra-lab-cassandra3-1'
SOURCES = {
    'instaclustr-standalone': ('cassandra-lab-cassandra1-1', 9500),
    'instaclustr-agent': ('cassandra-lab-cassandra2-1', 9500),
    'jmx': ('cassandra-lab-cassandra1-1', 9501),
    'criteo': ('cassandra-lab-cassandra1-1', 9502),
    'criteo-status': ('cassandra-lab-cassandra1-1', 9503),
    'mcac': ('cassandra-lab-cassandra2-1', 9103),
}

def docker(*args):
    p = subprocess.run(['docker', *args], text=True, capture_output=True, timeout=120)
    if p.returncode:
        raise RuntimeError(f'{args}: {p.stderr[-1500:]}')
    return p.stdout

def status(stage):
    result = docker('exec', 'cassandra-lab-cassandra1-1', 'nodetool', 'status')
    (OUT / f'comparison-status-{stage}.txt').write_text(result.rstrip() + '\n', encoding='utf-8')
    return result

def scrape(name, stage):
    container, port = SOURCES[name]
    code = ("import urllib.request,time,json; t=time.monotonic(); "
            f"b=urllib.request.urlopen('http://127.0.0.1:{port}/metrics',timeout=90).read(); "
            "print(json.dumps({'seconds':time.monotonic()-t,'body':b.decode()}))")
    result = json.loads(docker('exec', container, 'python3', '-c', code))
    body = result.pop('body')
    (OUT / f'comparison-{name}-{stage}.prom').write_text(body, encoding='utf-8')
    lines = [line for line in body.splitlines() if line and not line.startswith('#')]
    result.update(samples=len(lines), bytes=len(body.encode()),
                  families=len({line.split('{')[0].split(' ')[0] for line in lines}))
    return result

results = {'sources': SOURCES, 'stages': {}, 'limits': [
    'Small idle cluster; different coverage and collection/cache intervals.',
    'HTTP timings are not CPU overhead or a production performance benchmark.',
    'Two agents coexist on node2, Instaclustr must initialize before MCAC.',
    'Raw MCAC metrics use collectd names before Prometheus relabeling.',
]}
try:
    current = status('baseline')
    if sum(line.startswith('UN ') for line in current.splitlines()) != 3:
        raise RuntimeError('Expected three UN nodes before fault tests')
    print('Scraping healthy baseline', flush=True)
    for name in SOURCES:
        samples = [scrape(name, f'baseline-{n}') for n in range(3)]
        results['stages'].setdefault('baseline', {})[name] = {
            'runs': samples, 'median_http_seconds': statistics.median(s['seconds'] for s in samples)}
        print(name, results['stages']['baseline'][name], flush=True)
    print('Pausing node3 for failure detector observation', flush=True)
    docker('pause', TARGET)
    try:
        time.sleep(35)
        status('paused')
        results['stages']['paused'] = {name: scrape(name, 'paused') for name in SOURCES}
    finally:
        docker('unpause', TARGET)
    print('Node3 resumed; waiting for recovery', flush=True)
    for _ in range(12):
        time.sleep(5)
        if sum(line.startswith('UN ') for line in status('recovered').splitlines()) == 3:
            break
    else:
        raise RuntimeError('Node3 did not recover')
    results['stages']['recovered'] = {name: scrape(name, 'recovered') for name in SOURCES}
    print('Disabling only CQL on node3', flush=True)
    docker('exec', TARGET, 'nodetool', 'disablebinary')
    try:
        status('cql-disabled')
        results['cql_status_disabled'] = docker('exec', TARGET, 'nodetool', 'statusbinary').strip()
        results['stages']['cql-disabled'] = {name: scrape(name, 'cql-disabled') for name in SOURCES}
    finally:
        docker('exec', TARGET, 'nodetool', 'enablebinary')
    results['cql_status_restored'] = docker('exec', TARGET, 'nodetool', 'statusbinary').strip()
    status('final')
    results['success'] = True
finally:
    (OUT / 'comparison-results.json').write_text(json.dumps(results, indent=2) + '\n', encoding='utf-8')
print('Completed, node3 restored; results saved', flush=True)
