"""Host-side checks for the existing three-node lab; always restore node3."""
import concurrent.futures
import hashlib
import json
import pathlib
import re
import subprocess
import time
import zipfile

ROOT = pathlib.Path(__file__).resolve().parent
OUT = ROOT.parent / 'research' / 'membership-validation'
OUT.mkdir(exist_ok=True)

def docker(*args):
    return subprocess.check_output(['docker', *args], text=True, encoding='utf-8', timeout=100)

def metrics():
    code = "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:9505/metrics',timeout=5).read().decode())"
    return docker('exec', 'cassandra-lab-cassandra1-1', 'python3', '-c', code)

def value(body, metric):
    return float(next(line.rsplit(' ', 1)[1] for line in body.splitlines() if line.startswith(metric+'{')))

def wait_for(predicate, timeout=80):
    until=time.monotonic()+timeout
    while time.monotonic()<until:
        body=metrics()
        if predicate(body):return body
        time.sleep(2)
    raise AssertionError('Expected metric state did not arrive')

result={}
jar=ROOT/'target/cassandra-membership-exporter.jar'
result['jar_sha256']=hashlib.sha256(jar.read_bytes()).hexdigest()
with zipfile.ZipFile(jar) as z:
    versions={name:int.from_bytes(z.read(name)[6:8], 'big') for name in z.namelist()
              if name.endswith('.class') and not name.startswith('META-INF/versions/') and name!='module-info.class'}
    assert max(versions.values())<=52, 'A base class requires Java newer than 8'
    result['base_classes']=len(versions)
    result['max_base_class_version']=max(versions.values())
print('Waiting for a healthy Java 11 snapshot',flush=True)
base=wait_for(lambda b:value(b,'cassandra_membership_collection_success')==1)
(OUT/'healthy.prom').write_text(base,encoding='utf-8')
result['healthy_jmx_reads']=value(base,'cassandra_membership_jmx_reads_total')
print('400 HTTP requests against the live exporter',flush=True)
storm="""import concurrent.futures,urllib.request,json,time
def get(_):
 with urllib.request.urlopen('http://127.0.0.1:9505/metrics',timeout=5) as r:return len(r.read())
t=time.monotonic()
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:values=list(pool.map(get,range(400)))
print(json.dumps({'requests':len(values),'seconds':time.monotonic()-t,'min_bytes':min(values)}))
"""
before=metrics();result['http_storm']=json.loads(docker('exec','cassandra-lab-cassandra1-1','python3','-c',storm));after=metrics()
result['http_storm']['jmx_reads_delta']=value(after,'cassandra_membership_jmx_reads_total')-value(before,'cassandra_membership_jmx_reads_total')
# One scheduled tick is allowed across a wall-clock boundary, never one read per scrape.
assert result['http_storm']['jmx_reads_delta']<=1
print('Pausing only node3; waiting for the bounded background poll',flush=True)
target=docker('inspect','--format','{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}','cassandra-lab-cassandra3-1').strip()+':7000'
try:
    docker('pause','cassandra-lab-cassandra3-1')
    body=wait_for(lambda b:any('endpoint="'+target+'"' in line and 'status="down"' in line and line.endswith(' 1.0') for line in b.splitlines()))
    (OUT/'peer-paused.prom').write_text(body,encoding='utf-8')
    result['peer_down_detected']=True
finally:
    docker('unpause','cassandra-lab-cassandra3-1')
print('Node3 resumed; waiting for UP',flush=True)
body=wait_for(lambda b:any('endpoint="'+target+'"' in line and 'status="up"' in line and line.endswith(' 1.0') for line in b.splitlines()))
(OUT/'recovered.prom').write_text(body,encoding='utf-8')
result['peer_recovered']=True
result['status']=docker('exec','cassandra-lab-cassandra1-1','nodetool','status')
(OUT/'lab-results.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps(result,indent=2),flush=True)
