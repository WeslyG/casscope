"""Keep compact evidence while full metric snapshots remain git-ignored."""
import pathlib
import re

root = pathlib.Path(__file__).resolve().parents[1] / 'research'
patterns = {
    'instaclustr': r'^cassandra_endpoint_(active|phi|downtime_seconds_total)',
    'jmx': r'^org_apache_cassandra_(net_failuredetector_phivalues_phi|db_storageservice_gossiprunning)',
    'criteo-status': r'^cassandra_stats.*:(livenodes|unreachablenodes|joiningnodes|leavingnodes|movingnodes)"',
    'mcac': r'^collectd_mcac_gauge.*thread_pools.completed_tasks.internal.gossip_stage',
    'casspoke': r'^cassandra_up',
    'probe-gossip': r'^cassandra_endpoint_active',
}
parts = []
for path in sorted(root.glob('*.prom')):
    if not (path.name.startswith('comparison-') or path.name.startswith('casspoke-') or path.name.startswith('probe-')):
        continue
    matching = next((pattern for name, pattern in patterns.items() if name in path.name), None)
    if matching:
        try:
            content = path.read_text(encoding='utf-8-sig')
        except UnicodeDecodeError:
            content = path.read_text(encoding='cp1251')
        lines = [s for s in content.splitlines() if re.search(matching, s)]
        if lines:
            parts.extend([f'FILE: {path.name}', *lines, ''])
(root / 'comparison-evidence.txt').write_text('\n'.join(parts).rstrip() + '\n', encoding='utf-8')
for path in root.glob('*status-*.txt'):
    text = '\n'.join(line.rstrip() for line in path.read_text(encoding='utf-8-sig').splitlines()).rstrip() + '\n'
    path.write_text(text, encoding='utf-8')
