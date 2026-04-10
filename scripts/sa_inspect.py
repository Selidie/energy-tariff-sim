#!/usr/bin/env python3
"""
sa_inspect.py — Inspect the internal structure of a Solar Assistant backup.
Run this first to understand the backup format before running sa_import.py.

Usage:
    python3 sa_inspect.py /path/to/backup.zip
"""
import sys
import io
import json
import zipfile
import tarfile

backup = sys.argv[1]

print(f"\n{'='*60}")
print(f"Inspecting: {backup}")
print(f"{'='*60}\n")

with zipfile.ZipFile(backup, 'r') as zf:
    members = zf.namelist()
    meta_files     = sorted([m for m in members if m.endswith('.meta')])
    manifest_files = sorted([m for m in members if m.endswith('.manifest')])
    seg_files      = sorted([m for m in members if m.endswith('.tar.gz')])

    print(f"Total members : {len(members)}")
    print(f"  .meta files     : {len(meta_files)}")
    print(f"  .manifest files : {len(manifest_files)}")
    print(f"  .tar.gz segments: {len(seg_files)}\n")

    # Print all meta files
    print("=== META FILES ===")
    for mf in meta_files[:5]:
        print(f"\n--- {mf} ---")
        try:
            content = zf.read(mf)
            print(content.decode('utf-8', errors='replace')[:800])
        except Exception as e:
            print(f"  Error: {e}")

    # Print all manifest files
    print("\n=== MANIFEST FILES ===")
    for mf in manifest_files[:5]:
        print(f"\n--- {mf} ---")
        try:
            content = zf.read(mf)
            print(content.decode('utf-8', errors='replace')[:800])
        except Exception as e:
            print(f"  Error: {e}")

    # Inspect ALL segments (show inner file names)
    print(f"\n=== SEGMENT CONTENTS (all {len(seg_files)} segments) ===")
    all_inner_files = {}
    for sf in seg_files:
        try:
            seg_data = zf.read(sf)
            with tarfile.open(fileobj=io.BytesIO(seg_data), mode='r:gz') as tf:
                inner = tf.getmembers()
                for m in inner:
                    print(f"  [{sf}]  {m.name:60s}  {m.size:>12,} bytes  type={m.type}")
                    all_inner_files[m.name] = all_inner_files.get(m.name, 0) + m.size
        except Exception as e:
            print(f"  [{sf}] Error: {e}")

    print(f"\n=== UNIQUE INNER FILE PATHS (across all segments) ===")
    for name, total_size in sorted(all_inner_files.items()):
        print(f"  {name:60s}  {total_size:>12,} bytes total")
