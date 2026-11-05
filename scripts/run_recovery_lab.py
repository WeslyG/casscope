"""Native Molecule recovery run with local artifacts and a frozen candidate.

Run inside ansible14: python scripts/run_recovery_lab.py
No commits, publication, host firewall changes, or unrelated container stops.
"""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import uuid
import argparse
import docker

ROOT=Path(__file__).resolve().parents[1]
parser=argparse.ArgumentParser()
parser.add_argument('--resume-fixture',type=Path)
parser.add_argument('--jar',type=Path)
args=parser.parse_args()
candidate_name='working-tree'
jar=args.jar.resolve() if args.jar else ROOT/'membership-exporter/target/casscope-1.1.0.jar'
assert jar.exists(),'Build the final candidate and pass Java 11 tests first'
ident=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-recovery-molecule-'+uuid.uuid4().hex[:6]
out=ROOT/'.artifacts/casscope'/ident;inputs=out/'inputs';inputs.mkdir(parents=True)
name='cc-recovery-'+datetime.now(timezone.utc).strftime('%Y%m%dt%H%M%Sz')+'-'+uuid.uuid4().hex[:6]
parent=None
if args.resume_fixture:
    parent=args.resume_fixture.resolve();assert parent.is_relative_to(ROOT/'.artifacts/casscope')
    parent_manifest=json.loads((parent/'manifest.json').read_text());name=parent_manifest['name']
    assert parent_manifest['jar_sha256']==hashlib.sha256(jar.read_bytes()).hexdigest(),'Cannot resume with a different product'
source=inputs/'harness'
(source/'scripts').mkdir(parents=True)
shutil.copyfile(ROOT/'scripts/recovery_lab.py',source/'scripts/recovery_lab.py')
shutil.copytree(ROOT/'molecule/recovery',source/'molecule/recovery',ignore=shutil.ignore_patterns('__pycache__'))
shutil.copyfile(jar,inputs/'casscope-1.1.0.jar')
shutil.copyfile(ROOT/'molecule/recovery/files/jul.properties',inputs/'jul.properties')
shutil.copyfile(ROOT/'examples/prometheus/casscope-alerts.yml',inputs/'casscope-alerts.yml')
shutil.copyfile(ROOT/'molecule/recovery/files/Reference.java',inputs/'Reference.java')
client=docker.from_env(timeout=120)
image=client.images.get('cassandra@sha256:b877532a79865d0212c860352bb9c3b0be97db9628a662ea815fb90ea4033158').id
env=dict(os.environ,CASSCOPE_PROJECT=str(ROOT),CASSCOPE_RECOVERY_RUN=str(out),CASSCOPE_RECOVERY_NAME=name,CASSCOPE_RECOVERY_IMAGE=image,
         CASSCOPE_RECOVERY_SOURCE=str(source),MOLECULE_EPHEMERAL_DIRECTORY='/tmp/'+name,PYTHONUNBUFFERED='1')
old=['cassandra-lab-membership','cassandra-lab-casspoke','cassandra-lab-criteo-status','cassandra-lab-jmx','cassandra-lab-criteo',
     'cassandra-lab-exporter-standalone','cassandra-lab-cassandra3-1','cassandra-lab-cassandra2-1','cassandra-lab-cassandra1-1']
stopped=list(parent_manifest['stopped_old_lab']) if parent else []
def run(argv,filename):
    with (out/filename).open('w',buffering=1) as log:
        process=subprocess.Popen(argv,cwd=source,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
        for line in process.stdout:log.write(line);print(line,end='',flush=True)
        return process.wait()
print('ARTIFACTS='+str(out),flush=True)
(ROOT/'.cache').mkdir(exist_ok=True)
(ROOT/'.cache/recovery-molecule-current.txt').write_text(str(out)+'\n')
try:
    for n in old:
        try:
            c=client.containers.get(n)
            if c.status=='running':
                print('Stopping known old lab: '+n,flush=True);c.stop(timeout=30)
                if n not in stopped:stopped.append(n)
        except docker.errors.NotFound:pass
    (out/'manifest.json').write_text(json.dumps({'id':ident,'name':name,'created':datetime.now(timezone.utc).isoformat(),
        'candidate':candidate_name,'resumed_fixture_from':str(parent) if parent else None,'jar_sha256':hashlib.sha256(jar.read_bytes()).hexdigest(),'stopped_old_lab':stopped,
        'scope':'One real Cassandra 4.1.12 observer + real Prometheus, fault/recovery, same JAR on Java 8/11/17/21',
        'metrics_contract':'HTTP 200, collection_success=0 plus diagnostics, no stale Cassandra values; one scrape job',
        'timeout_retry_min_seconds':60,'max_timeout_backoff_seconds':900,
        'fixture_heap':'1 GiB','fixture_cpus':2,'fixture_memory':'2 GiB','fake_jmx':False},indent=2)+'\n')
    result=run(['molecule','test','-s','recovery','--destroy','never','--workers','1','--report'],'molecule-test.log')
    captured=run([sys.executable,'scripts/recovery_lab.py','capture'],'wrapper-capture.log')
    completed=result==0 and captured==0
    if completed:completed=run(['molecule','destroy','-s','recovery'],'molecule-destroy.log')==0
    if not (out/'result.json').exists():(out/'result.json').write_text(json.dumps({'verdict':'INFRA_ERROR','molecule_exit':result})+'\n')
    (out/'wrapper-result.json').write_text(json.dumps({'completed':completed,'molecule_exit':result,'capture_exit':captured,'kept':not completed},indent=2)+'\n')
    print('RUN_RESULT='+json.dumps({'completed':completed,'artifacts':str(out)}),flush=True)
    sys.exit(0 if completed else 1)
finally:
    restored=[]
    if not client.containers.list(all=True,filters={'label':'casscope.recovery='+name}):
        for n in reversed(stopped):
            try:client.containers.get(n).start();restored.append(n)
            except docker.errors.APIError as exc:restored.append({'name':n,'error':repr(exc)})
    (out/'old-lab-restored.json').write_text(json.dumps(restored,indent=2)+'\n')
    files=sorted(f for f in out.rglob('*') if f.is_file() and f!=out/'SHA256SUMS')
    (out/'SHA256SUMS').write_text('\n'.join(hashlib.sha256(f.read_bytes()).hexdigest()+'  '+f.relative_to(out).as_posix() for f in files)+'\n')
