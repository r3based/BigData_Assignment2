#!/usr/bin/env python3
"""MapReduce job 1 mapper: tabbed input doc_id, doc_title, doc_text -> posting counts and doc metadata."""
import re
import sys


def tokenize(text: str):
    return re.findall(r"[a-z0-9]+", text.lower())


for raw in sys.stdin:
    line = raw.rstrip("\n")
    if not line:
        continue
    parts = line.split("\t", 2)
    if len(parts) < 3:
        continue
    doc_id, title, text = parts[0], parts[1], parts[2]
    tokens = tokenize(text)
    dl = len(tokens)
    safe_title = title.replace("\t", " ").replace("\n", " ")
    print(f"__TITLE__#{doc_id}\t{safe_title}")
    print(f"__DL__#{doc_id}\t{dl}")
    for term in tokens:
        print(f"{term}#{doc_id}\t1")
