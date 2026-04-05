#!/usr/bin/env python3
"""MapReduce job 1 reducer: sum tf per term#doc; pass through title and dl."""
import sys


def flush(key, vals):
    if key is None or not vals:
        return
    if key.startswith("__TITLE__#"):
        doc_id = key[len("__TITLE__#") :]
        title = vals[-1]
        print(f"t\t{doc_id}\t{title}")
    elif key.startswith("__DL__#"):
        doc_id = key[len("__DL__#") :]
        total = sum(int(x) for x in vals)
        print(f"d\t{doc_id}\t{total}")
    else:
        total = sum(int(x) for x in vals)
        term, doc_id = key.split("#", 1)
        print(f"p\t{term}\t{doc_id}\t{total}")


cur_key = None
vals = []

for raw in sys.stdin:
    line = raw.rstrip("\n")
    if not line or "\t" not in line:
        continue
    key, val = line.split("\t", 1)
    if key != cur_key:
        flush(cur_key, vals)
        cur_key = key
        vals = [val]
    else:
        vals.append(val)

flush(cur_key, vals)
