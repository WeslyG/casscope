"""Same built JAR, JMX to lab Cassandra 4.1.12, no runtime installation on host."""
import hashlib
import json
import pathlib
import subprocess
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import uuid

root=pathlib.Path(__file__).resolve().parent
run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-runtime-'+uuid.uuid4().hex[:8]
out=root.parent/'.artifacts'/'casscope'/run_id
out.mkdir(parents=True)
print('Artifacts: '+str(out),flush=True)
configs=root/'target/matrix'
configs.mkdir(exist_ok=True)
version=ET.parse(root/'pom.xml').findtext('{http://maven.apache.org/POM/4.0.0}version')
jar_name=f'casscope-{version}.jar'
results={'jar_sha256':hashlib.sha256((root/'target'/jar_name).read_bytes()).hexdigest(),'runtimes':{}}
started=[]
def run(*args):
    return subprocess.run(['docker',*args],capture_output=True,text=True,encoding='utf-8',timeout=100,check=True)
try:
    for version in (8,17,21):
        port=19500+version
        config=configs/f'java{version}.yaml'
        config.write_text(f'observer: java{version}\nport: {port}\npoll_interval_seconds: 15\n',encoding='utf-8')
        name=f'cassandra-lab-membership-java{version}'
        run('run','-d','--name',name,'--network','container:cassandra-lab-cassandra1-1','--memory','128m','--cpus','0.25',
            '--mount',f'type=bind,source={root},target=/exporter,readonly',f'eclipse-temurin:{version}-jre',
            'java','-Xms16m','-Xmx64m','-jar',f'/exporter/target/{jar_name}','--config',f'/exporter/target/matrix/java{version}.yaml')
        started.append(name)
        results['runtimes'][str(version)]={'version':run('exec',name,'java','-version').stderr.strip()}
    for version in (8,17,21):
        deadline=time.monotonic()+40
        marker=f'cassandra_membership_collection_success{{cluster="cassandra",observer="java{version}"}} 1.0'
        while time.monotonic()<deadline:
            code=f"import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:{19500+version}/metrics',timeout=5).read().decode())"
            body=run('exec','cassandra-lab-cassandra1-1','python3','-c',code).stdout
            if marker in body:
                assert 'state="normal"' in body
                results['runtimes'][str(version)]['jmx_and_http']='passed'
                print(f'Java {version}: JMX and HTTP passed',flush=True)
                break
            time.sleep(1)
        else:
            raise AssertionError(f'Java {version} has no successful JMX snapshot')
finally:
    for name in started:
        subprocess.run(['docker','rm','-f',name],capture_output=True,timeout=30)
    (out/'runtime-matrix.json').write_text(json.dumps(results,indent=2)+'\n',encoding='utf-8')
