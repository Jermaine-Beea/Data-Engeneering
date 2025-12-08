#!/usr/bin/env python3
"""Validate that documented docker compose service names exist in docker-compose.yml.

Scans text files under `docs/` and looks for `docker compose up` / `docker-compose up` usages,
extracts service names, and verifies they exist in `docker-compose.yml`.

Exit code 0 when all referenced services are present; non-zero if mismatches found.
"""
import re
import sys
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / 'docker-compose.yml'
DOCS = ROOT / 'docs'

svc_pattern = re.compile(r"(?:docker\s+compose|docker-compose)\s+up(\s+[^\n`]+)")

def load_compose_services():
    if not COMPOSE.exists():
        print(f"docker-compose file not found: {COMPOSE}")
        sys.exit(2)
    data = yaml.safe_load(COMPOSE.read_text())
    services = data.get('services') or {}
    return set(services.keys())

def find_doc_service_references():
    refs = set()
    if not DOCS.exists():
        return refs
    for p in DOCS.rglob('*.md'):
        text = p.read_text()
        for m in svc_pattern.finditer(text):
            tail = m.group(1)
            # split tokens, ignore flags (starting with '-') and code fences
            tokens = [t for t in re.split(r"\s+", tail.strip()) if t and not t.startswith('-')]
            for t in tokens:
                # stop at markdown/code fence markers
                if t.startswith('```'):
                    break
                # remove trailing punctuation
                t = t.rstrip('`,.')
                refs.add(t)
    return refs

def main():
    services = load_compose_services()
    refs = find_doc_service_references()
    if not refs:
        print('No documented `docker compose up` service references found under docs/ — nothing to check.')
        return 0

    missing = sorted([r for r in refs if r not in services])
    if missing:
        print('The following documented services are MISSING from docker-compose.yml:')
        for s in missing:
            print(' -', s)
        print('\nAvailable services in docker-compose.yml:')
        for s in sorted(services):
            print(' -', s)
        return 1

    print('All documented services exist in docker-compose.yml:', ', '.join(sorted(refs)))
    return 0

if __name__ == '__main__':
    sys.exit(main())
