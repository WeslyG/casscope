"""Bounded read-only workload inside the lab Cassandra image, not a saturation benchmark."""
import glob
import json
import socket
import sys
import time
import urllib.request

driver=glob.glob('/opt/cassandra/lib/cassandra-driver-internal-only-*.zip')[0]
version=driver.rsplit('internal-only-',1)[1][:-4]
sys.path.insert(0,driver+'/cassandra-driver-'+version)
for name in ('six-','pure_sasl-'):
    for path in glob.glob('/opt/cassandra/lib/'+name+'*.zip'):sys.path.insert(0,path)
from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy

target=socket.gethostbyname('cassandra1')
cluster=Cluster([target],protocol_version=4,load_balancing_policy=WhiteListRoundRobinPolicy([target]),
                schema_metadata_enabled=False,token_metadata_enabled=False)
latencies=[]
try:
    session=cluster.connect()
    statement=session.prepare('SELECT value FROM lab_smoke.probe WHERE id = ?')
    start=time.monotonic()
    for index in range(900):
        pause=start+index/20-time.monotonic()
        if pause>0:time.sleep(pause)
        now=time.monotonic()
        row=session.execute(statement,(1,),timeout=2).one()
        assert row.value=='ready'
        latencies.append(time.monotonic()-now)
    print(json.dumps({'queries':len(latencies),'duration_seconds':time.monotonic()-start,
                     'target_qps_limit':20,'p95_seconds':sorted(latencies)[int(len(latencies)*.95)],
                     'max_seconds':max(latencies),'read_only':True}),flush=True)
finally:
    cluster.shutdown()
