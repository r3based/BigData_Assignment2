#!/usr/bin/env python3
"""MapReduce job 2 reducer: df + vocab/postings; corpus N and avg_dl."""
import sys


def flush_term(term, vals):
    postings = []
    for v in vals:
        if not v.startswith("POST\t"):
            continue
        _, doc_id, tf = v.split("\t", 2)
        postings.append((doc_id, int(tf)))
    df = len(postings)
    print(f"VOCAB\t{term}\t{df}")
    for doc_id, tf in postings:
        print(f"POST\t{term}\t{doc_id}\t{tf}")


def flush_stats(vals):
    doc_dl = {}
    doc_title = {}
    for v in vals:
        parts = v.split("\t", 1)
        if len(parts) < 2:
            continue
        kind, rest = parts[0], parts[1]
        if kind == "DL":
            doc_id, dl_s = rest.split("\t", 1)
            doc_dl[doc_id] = int(dl_s)
        elif kind == "TITLE":
            doc_id, title = rest.split("\t", 1)
            doc_title[doc_id] = title
    n_docs = len(doc_dl)
    if n_docs == 0:
        return
    sum_dl = sum(doc_dl.values())
    avg_dl = sum_dl / n_docs
    print(f"STAT\tn_docs\t{n_docs}")
    print(f"STAT\tavg_dl\t{avg_dl}")
    print(f"STAT\tsum_dl\t{sum_dl}")
    for doc_id, dl in sorted(doc_dl.items()):
        print(f"DOC_LEN\t{doc_id}\t{dl}")
    for doc_id, title in sorted(doc_title.items()):
        print(f"DOC_TITLE\t{doc_id}\t{title}")


cur_key = None
vals = []

for raw in sys.stdin:
    line = raw.rstrip("\n")
    if not line or "\t" not in line:
        continue
    key, val = line.split("\t", 1)
    if key != cur_key:
        if cur_key is not None:
            if cur_key == "__STATS__":
                flush_stats(vals)
            else:
                flush_term(cur_key, vals)
        cur_key = key
        vals = [val]
    else:
        vals.append(val)

if cur_key is not None:
    if cur_key == "__STATS__":
        flush_stats(vals)
    else:
        flush_term(cur_key, vals)
