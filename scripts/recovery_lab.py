"""Real Cassandra + Prometheus recovery scenario, executed only via Molecule.

No shortened retry settings. Artifacts survive destroy. Every mutation checks
the unique fixture label; firewall rules affect only the test node namespace.
"""
import concurrent.futures
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import re
import sys
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request
import docker

ROOT=Path(os.environ['CASSCOPE_PROJECT'])
OUT=Path(os.environ['CASSCOPE_RECOVERY_RUN'])
NAME=os.environ['CASSCOPE_RECOVERY_NAME']
assert OUT.resolve().is_relative_to((ROOT/'.artifacts/casscope').resolve())
assert re.fullmatch(r'cc-recovery-[a-z0-9-]+',NAME)
CLIENT=docker.from_env(timeout=120)
NODE=NAME+'-cassandra';EXP=NAME+'-exporter';PROM=NAME+'-prometheus';HELPER=NAME+'-netfault'
LABEL={'casscope.recovery':NAME}
BASE='cassandra:4.1.12@sha256:b877532a79865d0212c860352bb9c3b0be97db9628a662ea815fb90ea4033158'
PROM_IMAGE='prom/prometheus:v3.6.0@sha256:76947e7ef22f8a698fc638f706685909be425dbe09bd7a2cd7aca849f79b5f64'

def now():return datetime.now(timezone.utc).isoformat()
def save(path,value):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value,indent=2)+'\n')
def event(kind,**values):
    with (OUT/'timeline.jsonl').open('a') as f:f.write(json.dumps({'time':now(),'event':kind,**values})+'\n')
def owned(name):
    c=CLIENT.containers.get(name);assert c.labels.get('casscope.recovery')==NAME;return c
def phase(label):
    p=OUT/'phases'/(datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')+'-'+label)
    p.mkdir(parents=True);event('phase_start',phase=p.name);return p
def seal(p):
    (p/'SHA256SUMS').write_text('\n'.join(hashlib.sha256(f.read_bytes()).hexdigest()+'  '+f.relative_to(p).as_posix()
        for f in sorted(p.rglob('*')) if f.is_file() and f.name!='SHA256SUMS')+'\n')
    event('phase_sealed',phase=p.name)
def assertion(p,test,condition,**data):
    item={'test':test,'verdict':'PASS' if condition else 'FAIL',**data}
    with (p/'assertions.jsonl').open('a') as f:f.write(json.dumps(item)+'\n')
    event('assertion',**item)
    if not condition:raise AssertionError(json.dumps(item))
def execute(name,argv,p,check=True):
    ident=str(time.time_ns());start=now();r=owned(name).exec_run(argv,demux=True)
    stdout,stderr=r.output or (None,None);stdout=stdout or b'';stderr=stderr or b''
    (p/(ident+'.stdout')).write_bytes(stdout);(p/(ident+'.stderr')).write_bytes(stderr)
    with (p/'commands.jsonl').open('a') as f:f.write(json.dumps({'start':start,'end':now(),'container':name,'argv':argv,
        'exit':r.exit_code,'stdout':ident+'.stdout','stderr':ident+'.stderr'})+'\n')
    if check and r.exit_code:raise RuntimeError(stderr.decode(errors='replace')[-1500:]+stdout.decode(errors='replace')[-1500:])
    return (stdout+stderr).decode(errors='replace')
def put(c,files):
    data=io.BytesIO()
    with tarfile.open(fileobj=data,mode='w') as archive:
        for name,value in files.items():
            info=tarfile.TarInfo(name);info.size=len(value);info.mode=0o644;archive.addfile(info,io.BytesIO(value))
    c.put_archive('/tmp',data.getvalue())
def fetch_file(c,path):
    stream,_=c.get_archive(path)
    with tarfile.open(fileobj=io.BytesIO(b''.join(stream))) as archive:
        entries=[m for m in archive.getmembers() if m.isfile()];assert len(entries)==1
        return archive.extractfile(entries[0]).read()
IPS={}
def ip(name):
    if name not in IPS:IPS[name]=owned(name).attrs['NetworkSettings']['Networks'][NAME]['IPAddress']
    return IPS[name]
def http(name,path,prefix,p):
    start=time.time();url='http://'+ip(name)+(':'+'9090' if name==PROM else ':9505')+path
    try:
        with urllib.request.urlopen(url,timeout=5) as response:code=response.status;data=response.read().decode()
    except urllib.error.HTTPError as exc:code=exc.code;data=exc.read().decode()
    (p/(prefix+'-'+str(time.time_ns())+'.body')).write_text(data)
    with (p/'http.jsonl').open('a') as f:f.write(json.dumps({'start':start,'end':time.time(),'path':path,'container':name,'status':code})+'\n')
    return code,data
def metric(body,key):
    match=re.search(r'^cassandra_membership_'+re.escape(key)+r'(?:\{[^\n]*\})? ([^\s]+)',body,re.M)
    if not match:raise ValueError('Missing metric '+key)
    return float(match.group(1))
def diag(p):
    status,body=http(EXP,'/metrics','metrics-with-diagnostics',p);assert status==200;return body

CASSANDRA_FAMILIES='endpoint_status|endpoint_ring_state|gossip_running|native_transport_running|initialized|endpoints|observer_info'

def no_cassandra_values(body):
    return re.search(r'^cassandra_membership_('+CASSANDRA_FAMILIES+r')(?:\{| )',body,re.M) is None

def alert_firing(p,name):
    status,body=http(PROM,'/api/v1/alerts','prometheus-alerts',p)
    return status==200 and any(a['labels'].get('alertname')==name and a['state']=='firing' for a in json.loads(body)['data']['alerts'])
def query(expr,p):
    status,body=http(PROM,'/api/v1/query?'+urllib.parse.urlencode({'query':expr}),'prometheus-query',p)
    assert status==200
    result=json.loads(body)['data']['result']
    return float(result[0]['value'][1]) if result else None
def wait(fn,seconds=120):
    until=time.monotonic()+seconds;last=None
    while time.monotonic()<until:
        try:
            last=fn()
            if last:return last
        except (OSError,ValueError,docker.errors.APIError) as exc:last=repr(exc)
        time.sleep(1)
    raise AssertionError('Wait timed out: '+str(last)[:500])
def reference(p,*args):return execute(NODE,['java','-cp','/tmp','Reference',*args],p)
def cql(p):return execute(NODE,['cqlsh','-e','SELECT release_version FROM system.local;'],p)
def identity():
    c=owned(EXP);return {'id':c.id,'started':c.attrs['State']['StartedAt']}

def preflight():
    p=phase('preflight')
    mem=Path('/proc/meminfo').read_text();info=CLIENT.info()
    save(p/'resources.json',{'meminfo':mem,'cpu':info['NCPU'],'memory':info['MemTotal'],'pressure':{x:Path('/proc/pressure/'+x).read_text() for x in ('cpu','memory','io')}})
    available=int(re.search(r'^MemAvailable:\s+(\d+)',mem,re.M).group(1))*1024
    assert available>3500*1024**2,'Need >3500 MiB available for this isolated recovery fixture'
    jar=OUT/'inputs/casscope-1.1.0.jar';assert jar.exists()
    save(OUT/'inputs/images.json',{'cassandra':BASE,'prometheus':PROM_IMAGE,'jar_sha256':hashlib.sha256(jar.read_bytes()).hexdigest()})
    seal(p)

def prepare():
    p=phase('prepare')
    CLIENT.networks.get(NAME).connect(CLIENT.containers.get(os.environ['HOSTNAME']))
    if not (OUT/'inputs/Reference.class').exists():
        # Fresh checkout: build the independent recorder instead of requiring a past run.
        compiler=CLIENT.containers.create('maven:3.9.11-eclipse-temurin-11',name=NAME+'-compiler',
            entrypoint=['sleep','infinity'],network_mode='none',mem_limit='256m',labels=LABEL)
        try:
            put(compiler,{'Reference.java':(OUT/'inputs/Reference.java').read_bytes()});compiler.start()
            execute(compiler.name,['javac','--release','8','/tmp/Reference.java'],p)
            (OUT/'inputs/Reference.class').write_bytes(fetch_file(compiler,'/tmp/Reference.class'))
        finally:compiler.remove(force=True,v=True)
    data=(OUT/'inputs/Reference.class').read_bytes()
    jul=(OUT/'inputs/jul.properties').read_bytes()
    put(owned(NODE),{'Reference.class':data,'jul.properties':jul})
    owned(NODE).start()
    def ready():
        try:return '4.1.12' in cql(p)
        except RuntimeError:return False
    wait(ready,300)
    save(p/'reference.json',json.loads(reference(p)))
    reference(p,'logging','FINER');reference(p)
    (p/'jvm.txt').write_text(execute(NODE,['java','-version'],p))
    seal(p)

def config(observer='recovery'):
    return ('cluster: '+NAME+'\nobserver: '+observer+'\njmx_url: service:jmx:rmi:///jndi/rmi://'+NODE+':7199/jmxrmi\n'
        'listen_address: 0.0.0.0\nport: 9505\npoll_interval_seconds: 30\n'
        'timeout_retry_seconds: 60\nmax_timeout_backoff_seconds: 900\nsocket_timeout_ms: 3000\n')
def create_exporter(name,image,observer):
    c=CLIENT.containers.create(image,name=name,entrypoint=['java','-Xms16m','-Xmx64m','-jar','/tmp/casscope.jar','--config','/tmp/exporter.yaml'],
        network=NAME,mem_limit='128m',nano_cpus=500_000_000,labels=LABEL)
    put(c,{'casscope.jar':(OUT/'inputs/casscope-1.1.0.jar').read_bytes(),'exporter.yaml':config(observer).encode()});c.start();return c
def converge():
    p=phase('converge');changed=False
    try:owned(EXP)
    except docker.errors.NotFound:create_exporter(EXP,BASE,'recovery');changed=True
    (OUT/'inputs/exporter.yaml').write_text(config())
    wait(lambda:metric(diag(p),'collection_success')==1)
    try:owned(PROM)
    except docker.errors.NotFound:
        c=CLIENT.containers.create(PROM_IMAGE,name=PROM,network=NAME,mem_limit='256m',nano_cpus=500_000_000,labels=LABEL,
            command=['--config.file=/tmp/prometheus.yml','--storage.tsdb.path=/prometheus','--storage.tsdb.retention.time=1h','--log.level=warn'])
        prom=('global:\n  scrape_interval: 1s\n  evaluation_interval: 1s\nrule_files: ["/tmp/casscope-alerts.yml"]\n'
            'scrape_configs:\n  - job_name: casscope\n    scrape_timeout: 800ms\n    static_configs:\n      - targets: ["'+EXP+':9505"]\n')
        put(c,{'prometheus.yml':prom.encode(),'casscope-alerts.yml':(OUT/'inputs/casscope-alerts.yml').read_bytes()})
        (OUT/'inputs/prometheus.yml').write_text(prom);c.start();changed=True
        execute(PROM,['/bin/promtool','check','rules','/tmp/casscope-alerts.yml'],p)
    wait(lambda:query('up{job="casscope"}',p)==1)
    status,body=http(PROM,'/api/v1/targets','prometheus-targets',p)
    targets=json.loads(body)['data']['activeTargets']
    assertion(p,'single-primary-scrape-job',status==200 and len(targets)==1 and targets[0]['labels']['job']=='casscope' and targets[0]['scrapeUrl'].endswith('/metrics'))
    actual=identity();prior=OUT/'exporter-identity.json'
    if prior.exists():assertion(p,'idempotence-no-restart',json.loads(prior.read_text())==actual)
    else:save(prior,actual)
    assertion(p,'fresh-primary-200',http(EXP,'/metrics','primary',p)[0]==200)
    seal(p);print('CHANGED' if changed else 'UNCHANGED')

def exercise():
    p=phase('http-and-real-jmx-blackhole');original=identity()
    c=CLIENT.containers.run(CLIENT.containers.get(os.environ['HOSTNAME']).image.id,name=HELPER,entrypoint=['sleep','infinity'],
        user='0:0',network_mode='container:'+NODE,cap_add=['NET_ADMIN'],mem_limit='96m',labels=LABEL,detach=True)
    # Allow TCP/JRMP handshakes and small ACKs. Drop large response frames so the
    # actual getAttributes is dispatched before its result is lost.
    exporter_ip=ip(EXP);rule=['OUTPUT','-p','tcp','--sport','7199','-d',exporter_ip,'-m','length','--length','256:65535','-j','DROP']
    initial=diag(p);prefix=fetch_file(owned(NODE),'/var/log/cassandra/jmx-calls.log.0')
    try:
        before=metric(initial,'polls_total')
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as workers:
            healthy=list(workers.map(lambda _:http(EXP,'/metrics','healthy-storm',p)[0],range(200)))
        assertion(p,'healthy-scrapes-do-not-force-jmx',all(code==200 for code in healthy) and metric(diag(p),'polls_total')-before<=1,requests=len(healthy))
        (p/'iptables-before.txt').write_text(execute(HELPER,['iptables-save'],p))
        execute(HELPER,['iptables','-I',*rule],p);event('blackhole_applied',destination=exporter_ip)
        opened=wait(lambda:(lambda b:b if metric(b,'circuit_open')==1 and metric(b,'poll_in_flight')==0 else None)(diag(p)),100)
        delay=metric(opened,'next_poll_delay_seconds');assertion(p,'first-retry-at-least-60s',60<=delay<=66,delay=delay)
        status,body=http(EXP,'/metrics','failed-primary',p)
        assertion(p,'failed-main-scrape-200-with-diagnostics',status==200 and metric(body,'collection_success')==0 and metric(body,'timeouts_total')>=1 and metric(body,'retry_after_seconds')>0)
        assertion(p,'failed-readiness-503',http(EXP,'/-/ready','failed-ready',p)[0]==503)
        assertion(p,'process-liveness-200',http(EXP,'/-/healthy','liveness',p)[0]==200)
        assertion(p,'prometheus-up-one-during-jmx-failure',wait(lambda:query('up{job="casscope"}',p)==1,15))
        assertion(p,'prometheus-collection-failure-ingested',wait(lambda:query('cassandra_membership_collection_success{job="casscope"}',p)==0,15))
        assertion(p,'no-stale-cassandra-values-in-http',metric(opened,'collection_success')==0 and no_cassandra_values(body))
        assertion(p,'no-stale-cassandra-values-in-prometheus',wait(lambda:query('count({job="casscope",__name__=~"cassandra_membership_('+CASSANDRA_FAMILIES+')"}) or vector(0)',p)==0,15))
        assertion(p,'diagnostic-counters-ingested',wait(lambda:(query('cassandra_membership_timeouts_total{job="casscope"}',p) or 0)>=1 and (query('cassandra_membership_retry_after_seconds{job="casscope"}',p) or 0)>0,15))
        delivered=fetch_file(owned(NODE),'/var/log/cassandra/jmx-calls.log.0')[len(prefix):].decode()
        dispatched=[l for l in delivered.splitlines() if 'RMIConnectionImpl getAttributes ' in l and re.search(r'connectionId=rmi://'+re.escape(exporter_ip)+r'\s',l)]
        (p/'server-jmx-during-fault.log').write_text(delivered)
        assertion(p,'fault-after-real-getAttributes-dispatch',len(dispatched)>=1,dispatched=len(dispatched))
        cql(p);save(p/'reference-during-fault.json',json.loads(reference(p)))
        polls=metric(opened,'polls_total')
        def scrape(_):
            code,body=http(EXP,'/metrics','storm-primary',p)
            return code==200 and metric(body,'collection_success')==0 and no_cassandra_values(body)
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as workers:codes=list(workers.map(scrape,range(200)))
        assertion(p,'failed-scrapes-do-not-force-retry',all(codes) and metric(diag(p),'polls_total')==polls,requests=len(codes))
        second=wait(lambda:(lambda b:b if metric(b,'recovery_attempts_total')>metric(initial,'recovery_attempts_total') and metric(b,'consecutive_failures')>=2 and metric(b,'poll_in_flight')==0 else None)(diag(p)),100)
        delay=metric(second,'next_poll_delay_seconds');assertion(p,'second-backoff-at-least-120s',120<=delay<=132,delay=delay)
        assertion(p,'real-prometheus-alert-firing',wait(lambda:alert_firing(p,'CasscopeCollectionUnavailable'),20))
        (p/'iptables-during.txt').write_text(execute(HELPER,['iptables','-nvx','-L','OUTPUT'],p))
    finally:
        execute(HELPER,['iptables','-D',*rule],p,check=False)
        (p/'iptables-restored.txt').write_text(execute(HELPER,['iptables-save'],p));event('blackhole_removed')
        c.remove(force=True,v=True)
    recovered=wait(lambda:(lambda b:b if metric(b,'collection_success')==1 and metric(b,'circuit_open')==0 else None)(diag(p)),170)
    assertion(p,'automatic-recovery-no-restart',identity()==original,identity=identity())
    assertion(p,'recovery-counter',metric(recovered,'recoveries_total')==metric(initial,'recoveries_total')+1)
    assertion(p,'restored-main-and-ready-200',http(EXP,'/metrics','recovered-primary',p)[0]==200 and http(EXP,'/-/ready','recovered-ready',p)[0]==200)
    assertion(p,'prometheus-collection-restored',wait(lambda:query('up{job="casscope"}',p)==1 and query('cassandra_membership_collection_success{job="casscope"}',p)==1,15))
    assertion(p,'alert-cleared',wait(lambda:query('ALERTS{alertname="CasscopeCollectionUnavailable",alertstate="firing"}',p) is None,15))
    data=fetch_file(owned(NODE),'/var/log/cassandra/jmx-calls.log.0');assert data.startswith(prefix)
    raw=data[len(prefix):].decode();(p/'server-jmx-window.log').write_text(raw)
    calls=[l for l in raw.splitlines() if 'RMIConnectionImpl getAttributes ' in l and re.search(r'connectionId=rmi://'+re.escape(exporter_ip)+r'\s',l)]
    reads=metric(recovered,'jmx_reads_total')-metric(initial,'jmx_reads_total')
    assertion(p,'server-side-application-count',len(calls)==reads,server_getAttributes=len(calls),client_reads_delta=reads)
    cql(p);seal(p)
    p=phase('same-jar-runtime-matrix')
    for version in (8,17,21):
        name=NAME+'-java'+str(version);c=create_exporter(name,'eclipse-temurin:'+str(version)+'-jre','java'+str(version))
        try:
            def runtime_ready():
                code,body=http(name,'/metrics','java'+str(version),p)
                return code==200 and 'cassandra_membership_endpoint_status{' in body
            assertion(p,'real-cassandra-java'+str(version),wait(runtime_ready,100))
            (p/('java'+str(version)+'-version.txt')).write_text(execute(name,['java','-version'],p))
            (p/('java'+str(version)+'.log')).write_bytes(c.logs(timestamps=True))
        finally:c.remove(force=True,v=True)
    seal(p)
    p=phase('real-cql-disabled')
    before_cql=identity()
    try:
        execute(NODE,['nodetool','disablebinary'],p);event('cql_disabled')
        def disabled():
            status,body=http(EXP,'/metrics','cql-disabled',p)
            return status==200 and metric(body,'collection_success')==1 and metric(body,'native_transport_running')==0
        assertion(p,'cql-disabled-is-valid-collection',wait(disabled,100))
        assertion(p,'cql-disabled-prometheus-values',wait(lambda:query('up{job="casscope"}',p)==1 and query('cassandra_membership_collection_success{job="casscope"}',p)==1 and query('cassandra_membership_native_transport_running{job="casscope"}',p)==0,15))
        assertion(p,'real-cql-service-alert-firing',wait(lambda:alert_firing(p,'CassandraLocalServiceUnavailable'),80))
        assertion(p,'no-collection-alert-for-disabled-cql',not alert_firing(p,'CasscopeCollectionUnavailable'))
    finally:
        execute(NODE,['nodetool','enablebinary'],p);event('cql_enabled')
    assertion(p,'cql-recovered-without-exporter-restart',wait(lambda:query('cassandra_membership_native_transport_running{job="casscope"}',p)==1,100) and identity()==before_cql)
    assertion(p,'cql-service-alert-cleared',wait(lambda:not alert_firing(p,'CassandraLocalServiceUnavailable'),15))
    cql(p);seal(p)
    p=phase('exporter-process-unavailable')
    # This intentional process stop is separate from the already-proven JMX
    # automatic recovery above and cannot be mistaken for its recovery mechanism.
    try:
        owned(EXP).stop(timeout=10);event('exporter_intentionally_stopped')
        assertion(p,'stopped-exporter-up-zero',wait(lambda:query('up{job="casscope"}',p)==0,20))
        assertion(p,'stopped-exporter-alert-firing',wait(lambda:alert_firing(p,'CasscopeCollectionUnavailable'),80))
    finally:
        owned(EXP).start();event('exporter_intentionally_started')
    assertion(p,'restarted-exporter-collection-restored',wait(lambda:query('up{job="casscope"}',p)==1 and query('cassandra_membership_collection_success{job="casscope"}',p)==1,100))
    assertion(p,'stopped-exporter-alert-cleared',wait(lambda:not alert_firing(p,'CasscopeCollectionUnavailable'),15))
    seal(p);save(OUT/'exercise-complete.json',{'time':now(),'jmx_recovery_without_restart':True,'separate_process_stop_test':True})

def capture():
    p=phase('capture');errors=[]
    for c in CLIENT.containers.list(all=True,filters={'label':'casscope.recovery='+NAME}):
        try:
            c.reload();save(p/(c.name+'-inspect.json'),{k:c.attrs[k] for k in ('Id','Image','State','NetworkSettings')})
            (p/(c.name+'.log')).write_bytes(c.logs(timestamps=True))
            if c.name==NODE:
                data,_=c.get_archive('/var/log/cassandra');archive=b''.join(data)
                with tarfile.open(fileobj=io.BytesIO(archive)) as logs:
                    files=[m.name for m in logs.getmembers() if m.isfile()]
                assert any('/gc.log' in name for name in files),'GC capture is incomplete'
                (p/'cassandra-physical-logs.tar').write_bytes(archive);save(p/'captured-log-files.json',files)
        except Exception as exc:errors.append({'container':c.name,'error':repr(exc)})
    save(p/'capture-errors.json',errors);seal(p)
    if errors:raise RuntimeError(json.dumps(errors))
    save(OUT/'capture-complete.json',{'phase':p.name,'time':now()})
def verify():
    checks=[json.loads(l) for f in (OUT/'phases').glob('*/assertions.jsonl') for l in f.read_text().splitlines()]
    ok=bool(checks) and all(x['verdict']=='PASS' for x in checks) and (OUT/'exercise-complete.json').exists()
    save(OUT/'result.json',{'verdict':'PASS' if ok else 'FAIL','checks':checks,'time':now()})
    assert ok
def destroy():
    capture();assert (OUT/'capture-complete.json').exists()
    for c in CLIENT.containers.list(all=True,filters={'label':'casscope.recovery='+NAME}):
        assert c.labels.get('casscope.recovery')==NAME and c.name.startswith(NAME+'-');c.remove(force=True,v=True)
    net=CLIENT.networks.get(NAME);assert net.attrs['Labels'].get('casscope.recovery')==NAME
    net.disconnect(CLIENT.containers.get(os.environ['HOSTNAME']),force=True);net.remove()
    save(OUT/'destroy-complete.json',{'time':now(),'remaining':len(CLIENT.containers.list(all=True,filters={'label':'casscope.recovery='+NAME}))})

if __name__=='__main__':
    try:globals()[sys.argv[1]]()
    except Exception as exc:event('stage_failed',stage=sys.argv[1],error=repr(exc));raise
