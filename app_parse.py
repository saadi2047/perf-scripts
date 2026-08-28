#!/usr/bin/env python3
# app_parse.py — summarize filtered log signals per service into findings.
import sys, os, csv
from collections import defaultdict
outdir = sys.argv[1]

def svc_of(pod):
    parts = pod.split("-")
    core = parts[:-2] if len(parts) >= 3 else parts
    for tok in core:
        if "service" in tok.lower(): return tok
    return core[1] if len(core) > 1 else (core[0] if core else pod)

tot = defaultdict(lambda: defaultdict(int))  # svc -> severity -> count
path = os.path.join(outdir, "log_signal_counts.csv")
if os.path.exists(path):
    for r in csv.DictReader(open(path)):
        svc = svc_of(r["pod"])
        for k in ("errors","warns","exceptions","gc","conn_pool","http503"):
            try: tot[svc][k] += int(r[k] or 0)
            except: pass

with open(os.path.join(outdir, "log_summary.csv"), "w") as f:
    f.write("service,errors,warns,exceptions,gc,conn_pool,http503\n")
    for svc in sorted(tot):
        t = tot[svc]
        f.write(f"{svc},{t['errors']},{t['warns']},{t['exceptions']},{t['gc']},{t['conn_pool']},{t['http503']}\n")

findings = []
for svc in sorted(tot):
    t = tot[svc]
    if t["conn_pool"] > 0:
        findings.append(f"{svc}: {t['conn_pool']} connection-pool warning(s) in logs (Hikari exhaustion / unable to acquire).")
    if t["http503"] > 0:
        findings.append(f"{svc}: {t['http503']} HTTP 503 / service-unavailable line(s).")
    if t["exceptions"] > 0:
        findings.append(f"{svc}: {t['exceptions']} exception/stacktrace occurrence(s).")
    if t["gc"] > 0:
        findings.append(f"{svc}: {t['gc']} GC-pause log line(s).")
    if t["errors"] > 20:
        findings.append(f"{svc}: {t['errors']} ERROR-level line(s) — elevated.")

with open(os.path.join(outdir, "findings.txt"), "w") as f:
    if findings:
        for x in findings: f.write(x + "\n")
    else:
        f.write("No significant error, exception, connection-pool, 503, or GC signals in filtered logs.\n")

print(f"[app] {len(tot)} services with log signals, {len(findings)} findings")
for x in findings[:8]: print("   -", x)
