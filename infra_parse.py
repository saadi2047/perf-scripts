#!/usr/bin/env python3
# infra_parse.py — turns per-sample CSVs into per-service trends + findings.
# Computes, per service (namespace): CPU/mem peak & settled (last quartile),
# max restart delta, pod-count range, and a plain-language findings list.
import sys, os, csv
from collections import defaultdict

outdir = sys.argv[1]

def svc_of(ns, pod):
    # Real pods: <prefix>-<servicename>-<replicaset>-<suffix>
    #   txn-transactionservice-9456f7669-5h9cp -> transactionservice
    #   payment-paymentservice-8449769dd9-4bdbx -> paymentservice
    # Strategy: drop the last two tokens (replicaset hash + pod suffix), then take the
    # token containing 'service' if present, else the 2nd token, else the join.
    parts = pod.split("-")
    if len(parts) >= 3:
        core = parts[:-2]           # strip rs-hash + suffix
    else:
        core = parts
    for tok in core:
        if "service" in tok.lower():
            return tok
    return core[1] if len(core) > 1 else (core[0] if core else pod)

# ---- load pod_usage ----
usage = defaultdict(lambda: defaultdict(list))  # svc -> sample -> [(cpu,mem)]
samples_seen = set()
path = os.path.join(outdir, "pod_usage.csv")
if os.path.exists(path):
    for r in csv.DictReader(open(path)):
        try:
            s = int(r["sample"]); cpu = float(r["cpu_m"] or 0); mem = float(r["mem_mi"] or 0)
        except: continue
        svc = svc_of(r["namespace"], r["pod"])
        usage[svc][s].append((cpu, mem))
        samples_seen.add(s)

nsamp = max(samples_seen) if samples_seen else 0
# "settled" = last 25% of samples; "peak window" = the rest
q = max(1, nsamp // 4)
peak_samples = set(range(1, nsamp - q + 1))
settled_samples = set(range(nsamp - q + 1, nsamp + 1))

def svc_agg(svc):
    per_sample_cpu = {}   # sample -> total cpu across pods
    per_sample_mem = {}
    per_sample_pods = {}
    for s, lst in usage[svc].items():
        per_sample_cpu[s] = sum(c for c, m in lst)
        per_sample_mem[s] = sum(m for c, m in lst)
        per_sample_pods[s] = len(lst)
    def mx(d, samples): 
        vals=[v for s,v in d.items() if s in samples]; return max(vals) if vals else 0
    def av(d, samples):
        vals=[v for s,v in d.items() if s in samples]; return sum(vals)/len(vals) if vals else 0
    return {
        "cpu_peak": mx(per_sample_cpu, peak_samples),
        "cpu_settled": av(per_sample_cpu, settled_samples),
        "mem_peak": mx(per_sample_mem, peak_samples),
        "mem_settled": av(per_sample_mem, settled_samples),
        "pods_max": max(per_sample_pods.values()) if per_sample_pods else 0,
        "pods_min": min(per_sample_pods.values()) if per_sample_pods else 0,
    }

# ---- restarts: max delta over run ----
restart_delta = defaultdict(int)
path = os.path.join(outdir, "pod_status.csv")
first_rc, last_rc = {}, {}
if os.path.exists(path):
    for r in csv.DictReader(open(path)):
        key = (r["namespace"], r["pod"])
        try: rc = int(r["restarts"])
        except: continue
        if key not in first_rc: first_rc[key] = rc
        last_rc[key] = rc
    for key in first_rc:
        d = last_rc[key] - first_rc[key]
        if d > 0:
            svc = svc_of(key[0], key[1])
            restart_delta[svc] += d

# ---- scheduler events: totals by reason ----
sched = defaultdict(lambda: defaultdict(int))  # ns -> reason -> count
path = os.path.join(outdir, "scheduler_events.csv")
if os.path.exists(path):
    for r in csv.DictReader(open(path)):
        try: sched[r["namespace"]][r["reason"]] += int(r["count"])
        except: continue

# ---- write per-service trend CSV ----
services = sorted(usage.keys())
with open(os.path.join(outdir, "service_trends.csv"), "w") as f:
    f.write("service,cpu_peak_m,cpu_settled_m,mem_peak_mi,mem_settled_mi,pods_min,pods_max,restart_delta\n")
    for svc in services:
        a = svc_agg(svc)
        f.write(f"{svc},{a['cpu_peak']:.0f},{a['cpu_settled']:.0f},{a['mem_peak']:.0f},{a['mem_settled']:.0f},{a['pods_min']},{a['pods_max']},{restart_delta.get(svc,0)}\n")

# ---- findings (plain language) ----
findings = []
for svc in services:
    a = svc_agg(svc)
    if a["cpu_peak"] > 0 and a["cpu_settled"] > 0:
        ratio = a["cpu_peak"] / max(a["cpu_settled"], 1)
        if ratio >= 1.5:
            findings.append(f"{svc}: CPU rose to {a['cpu_peak']:.0f}m at peak load, settling to ~{a['cpu_settled']:.0f}m — a {ratio:.1f}x swing.")
    if a["pods_max"] > a["pods_min"]:
        findings.append(f"{svc}: scaled from {a['pods_min']} to {a['pods_max']} pods during the run.")
    if restart_delta.get(svc, 0) > 0:
        findings.append(f"{svc}: {restart_delta[svc]} pod restart(s) occurred during the hour.")

# scheduler findings — separate real cpu pressure from noise
for ns, reasons in sched.items():
    cpu = reasons.get("Insufficient_cpu", 0)
    noise = reasons.get("node_affinity",0) + reasons.get("pvc_volume",0) + reasons.get("taint",0)
    if cpu > 0:
        findings.append(f"{ns}: {cpu} 'Insufficient cpu' scheduling event(s) — real CPU scheduling pressure.")
    if noise > 0:
        findings.append(f"{ns}: {noise} scheduling event(s) from affinity/PVC/taint — NOT cpu-related (excluded from capacity signal).")

with open(os.path.join(outdir, "findings.txt"), "w") as f:
    if findings:
        for x in findings: f.write(x + "\n")
    else:
        f.write("No significant CPU swings, scaling, restarts, or scheduling pressure detected during the run.\n")


# ---- node-level utilization over the run (avg + peak per node) ----
node_samples = {}  # node -> list of (cpu_pct, mem_pct, cpu_m, mem_mi)
npath = os.path.join(outdir, "node_usage.csv")
if os.path.exists(npath):
    for r in csv.DictReader(open(npath)):
        try:
            node_samples.setdefault(r["node"], []).append(
                (float(r["cpu_pct"] or 0), float(r["mem_pct"] or 0), float(r["cpu_m"] or 0), float(r["mem_mi"] or 0)))
        except: pass
with open(os.path.join(outdir, "node_trends.csv"), "w") as f:
    f.write("node,cpu_pct_avg,cpu_pct_peak,mem_pct_avg,mem_pct_peak,cpu_m_avg,cpu_m_peak\n")
    for node in sorted(node_samples):
        v = node_samples[node]
        n = len(v)
        cavg = sum(x[0] for x in v)/n; cpk = max(x[0] for x in v)
        mavg = sum(x[1] for x in v)/n; mpk = max(x[1] for x in v)
        cmavg = sum(x[2] for x in v)/n; cmpk = max(x[2] for x in v)
        f.write(f"{node},{cavg:.1f},{cpk:.1f},{mavg:.1f},{mpk:.1f},{cmavg:.0f},{cmpk:.0f}\n")
print(f"[infra] node utilization aggregated for {len(node_samples)} nodes")


# ---- run time window (for the report header) ----
tss=[]
pu=os.path.join(outdir,"pod_usage.csv")
if os.path.exists(pu):
    for r in csv.DictReader(open(pu)):
        if r.get("ts"): tss.append(r["ts"])
if tss:
    with open(os.path.join(outdir,"window.txt"),"w") as f:
        f.write(f"start={min(tss)}\n")
        f.write(f"end={max(tss)}\n")
        f.write(f"samples={nsamp}\n")

print(f"[infra] {len(services)} services, {nsamp} samples")
print(f"[infra] {len(findings)} findings written")
for x in findings[:6]: print("   -", x)
