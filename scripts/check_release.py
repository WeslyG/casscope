"""Validate the release version/tag and optional built JAR; no network access."""
import argparse
import os
import pathlib
import re
import xml.etree.ElementTree as ET
import zipfile

root = pathlib.Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('--jar', type=pathlib.Path)
args = parser.parse_args()
version = ET.parse(root / 'membership-exporter/pom.xml').findtext('{http://maven.apache.org/POM/4.0.0}version')
if not version or not re.fullmatch(r'\d+\.\d+\.\d+', version):
    raise SystemExit('Expected a stable x.y.z project version')
ref = os.environ.get('GITHUB_REF', '')
if ref.startswith('refs/tags/') and ref != 'refs/tags/v' + version:
    raise SystemExit('Tag must equal v' + version)
if args.jar:
    if args.jar.name != f'casscope-{version}.jar':
        raise SystemExit('Unexpected release asset name')
    with zipfile.ZipFile(args.jar) as jar:
        manifest = jar.read('META-INF/MANIFEST.MF').decode().replace('\r\n ', '').replace('\n ', '')
        if f'Implementation-Version: {version}\n' not in manifest.replace('\r\n', '\n'):
            raise SystemExit('JAR manifest version does not match project version')
        for name in jar.namelist():
            if name.endswith('.class') and not name.startswith('META-INF/versions/') and name != 'module-info.class':
                if int.from_bytes(jar.read(name)[6:8], 'big') > 52:
                    raise SystemExit('Base class requires Java newer than 8: ' + name)
print(version)
