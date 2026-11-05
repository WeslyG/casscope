"""Validate release documentation and local links using only Python stdlib."""
from pathlib import Path
import re
from urllib.parse import unquote
import xml.etree.ElementTree as ET

root=Path(__file__).resolve().parents[1]
version=ET.parse(root/'membership-exporter/pom.xml').findtext('{http://maven.apache.org/POM/4.0.0}version')
documents=[root/'README.md',root/'membership-exporter/README.md',*sorted((root/'docs').glob('*.md'))]
for path in documents:
    text=path.read_text(encoding='utf-8')
    for release in re.findall(r'casscope-(\d+\.\d+\.\d+)\.jar',text):
        assert release==version,f'{path}: outdated JAR version {release}'
    for target in re.findall(r'\[[^\]]*\]\(([^)]+)\)',text):
        target=unquote(target.strip('<>').split('#',1)[0])
        if not target or re.match(r'[a-zA-Z][a-zA-Z0-9+.-]*:',target):continue
        assert (path.parent/target).exists(),f'{path}: broken link {target}'
    assert text.count('```')%2==0,f'{path}: unclosed code block'
config=(root/'membership-exporter/src/main/java/local/cassandra/membership/Config.java').read_text(encoding='utf-8')
keys=set(re.findall(r'case "([a-z_]+)":',config))
documented=set(re.findall(r'^\| `([a-z_]+)` \|',(root/'docs/configuration.md').read_text(encoding='utf-8'),re.M))
assert keys<=documented,f'Undocumented config keys: {keys-documented}'
print(f'Documentation OK: {len(documents)} files; version {version}; {len(keys)} config keys')
