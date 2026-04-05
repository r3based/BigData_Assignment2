#!/usr/bin/env python3
"""MapReduce job 2 mapper: postings -> term-keyed rows; doc stats -> __STATS__."""
import sys

for raw in sys.stdin:
    line = raw.rstrip("\n")
    if not line:
        continue
    if line.startswith("p\t"):
        parts = line.split("\t", 3)
        if len(parts) < 4:
            continue
        _, term, doc_id, tf = parts
        print(f"{term}\tPOST\t{doc_id}\t{tf}")
    elif line.startswith("d\t"):
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        _, doc_id, dl = parts
        print(f"__STATS__\tDL\t{doc_id}\t{dl}")
    elif line.startswith("t\t"):
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        _, doc_id, title = parts
        print(f"__STATS__\tTITLE\t{doc_id}\t{title}")
