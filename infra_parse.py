#!/usr/bin/env python3
# infra_parse.py — turns per-sample CSVs into per-service trends + findings.
# Computes, per service (namespace): CPU/mem peak & settled (last quartile),
# max restart delta, pod-count range, and a plain-language findings list.
import sys, os, csv
from collections import defaultdict

outdir = sys.argv[1]

def fmt_cpu_m(m):
    m=float(m)
    return (f"{m/1000:.1f}".rstrip("0").rstrip(".")+" cores") if m>=1000 else f"{m:.0f}m"

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
            findings.append(f"{svc}: CPU peaked at {fmt_cpu_m(a['cpu_peak'])} and averaged {fmt_cpu_m(a['cpu_settled'])} when settled ({ratio:.1f}x higher at peak than settled).")
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


# ============================================================================
# EXTENDED CAPTURES (v2): utilization avg/peak+timestamp, restart reasons,
# spin-up, node distribution, HPA timeline. Appended outputs for the report.
# ============================================================================
import csv as _csv, os as _os
from collections import defaultdict as _dd

_out = outdir

def _ts_short(iso):
    try:
        import datetime
        d=datetime.datetime.strptime(iso,"%Y-%m-%dT%H:%M:%SZ")
        ist=d+datetime.timedelta(hours=5,minutes=30)
        return ist.strftime("%H:%M:%S IST")
    except: return iso

# ---- 1. Per-service utilization: avg + peak(with timestamp) over the whole run ----
usage_rows=[]
pu=_os.path.join(_out,"pod_usage.csv")
if _os.path.exists(pu):
    for r in _csv.DictReader(open(pu)):
        usage_rows.append(r)

def _svc(pod):
    parts=pod.split("-"); core=parts[:-2] if len(parts)>=3 else parts
    for t in core:
        if "service" in t.lower(): return t
    return core[1] if len(core)>1 else pod

# aggregate per service per sample (sum across pods), then avg/peak across samples
svc_sample_cpu=_dd(lambda:_dd(float))
svc_sample_mem=_dd(lambda:_dd(float))
sample_ts={}
for r in usage_rows:
    try:
        s=int(r["sample"]); cpu=float(r["cpu_m"] or 0); mem=float(r["mem_mi"] or 0)
    except: continue
    svc=_svc(r["pod"]); svc_sample_cpu[svc][s]+=cpu; svc_sample_mem[svc][s]+=mem
    sample_ts[s]=r["ts"]

with open(_os.path.join(_out,"utilization_summary.csv"),"w") as f:
    f.write("service,cpu_avg_m,cpu_peak_m,cpu_peak_ts,mem_avg_mi,mem_peak_mi,mem_peak_ts\n")
    for svc in sorted(svc_sample_cpu):
        cser=svc_sample_cpu[svc]; mser=svc_sample_mem[svc]
        cavg=sum(cser.values())/len(cser) if cser else 0
        cpk_s=max(cser,key=cser.get) if cser else 0; cpk=cser.get(cpk_s,0)
        mavg=sum(mser.values())/len(mser) if mser else 0
        mpk_s=max(mser,key=mser.get) if mser else 0; mpk=mser.get(mpk_s,0)
        f.write(f"{svc},{cavg:.0f},{cpk:.0f},{_ts_short(sample_ts.get(cpk_s,''))},{mavg:.0f},{mpk:.0f},{_ts_short(sample_ts.get(mpk_s,''))}\n")

# ---- 2. Restart reasons (dedup to worst per pod) ----
rr=_os.path.join(_out,"restart_reasons.csv")
if _os.path.exists(rr):
    worst={}
    for r in _csv.DictReader(open(rr)):
        k=(r["namespace"],r["pod"])
        try: rc=int(r["restart_count"])
        except: rc=0
        if k not in worst or rc>worst[k][0]:
            worst[k]=(rc,r.get("reason",""),r.get("exit_code",""))
    with open(_os.path.join(_out,"restart_summary.csv"),"w") as f:
        f.write("namespace,pod,restarts,reason,exit_code,interpretation\n")
        for (ns,pod),(rc,reason,ec) in sorted(worst.items(),key=lambda x:-x[1][0]):
            interp=""
            if str(ec)=="143": interp="SIGTERM (liveness probe kill or graceful stop)"
            elif str(ec)=="137": interp="SIGKILL (OOMKilled or forced)"
            elif reason=="OOMKilled": interp="Out of memory"
            elif reason=="Error": interp="Container error/crash"
            f.write(f"{ns},{pod},{rc},{reason},{ec},{interp}\n")

# ---- 3. Spin-up times (avg per service) ----
sp=_os.path.join(_out,"pod_spinup.csv")
if _os.path.exists(sp):
    svc_spin=_dd(list)
    svc_ns={}
    seen=set()
    for r in _csv.DictReader(open(sp)):
        key=(r["namespace"],r["pod"])
        if key in seen: continue
        try:
            s=float(r["spinup_s"])
            if s>0:
                svc=_svc(r["pod"]); svc_spin[svc].append(s); svc_ns[svc]=r["namespace"]; seen.add(key)
        except: pass
    with open(_os.path.join(_out,"spinup_summary.csv"),"w") as f:
        f.write("namespace,service,pods,spinup_avg_s,spinup_max_s\n")
        for svc in sorted(svc_spin,key=lambda x:svc_ns.get(x,"")):
            v=svc_spin[svc]
            f.write(f"{svc_ns.get(svc,'')},{svc},{len(v)},{sum(v)/len(v):.1f},{max(v):.1f}\n")

# ---- 4. Node distribution: per-node, per-namespace, per-sample counts ----
# Produces node_distribution.csv with first(before)/peak/last(after) counts,
# broken down by namespace. Peak = the sample with the most total app pods.
pp=_os.path.join(_out,"pod_placement.csv")
if _os.path.exists(pp):
    # sample -> node -> ns -> count
    snc=_dd(lambda:_dd(lambda:_dd(int)))
    # sample -> total app pods (to find the peak sample)
    sample_total=_dd(int)
    samples=set()
    for r in _csv.DictReader(open(pp)):
        try: s=int(r["sample"])
        except: continue
        node=r["node"]; ns=r.get("namespace","")
        if node and node!="?" and ("worker" in node or "gemfire" in node):
            snc[s][node.split('.')[0]][ns]+=1
            sample_total[s]+=1
            samples.add(s)
    if samples:
        first_s=min(samples); last_s=max(samples)
        peak_s=max(sample_total,key=sample_total.get)
        # all nodes seen
        allnodes=set()
        for s in snc:
            allnodes.update(snc[s].keys())
        # all namespaces seen (short)
        def _shortns(n): return n.replace("perf-","")
        allns=set()
        for s in snc:
            for nd in snc[s]:
                allns.update(snc[s][nd].keys())
        allns=sorted(allns)
        # node-level before/peak/after totals + per-ns peak breakdown
        with open(_os.path.join(_out,"node_distribution.csv"),"w") as f:
            f.write("node,pods_before,pods_peak,pods_after\n")
            def _nodetotal(s,node): return sum(snc[s][node].values())
            for node in sorted(allnodes,key=lambda n:_nodetotal(peak_s,n),reverse=True):
                b=_nodetotal(first_s,node); p=_nodetotal(peak_s,node); a=_nodetotal(last_s,node)
                f.write(f"{node},{b},{p},{a}\n")
        # per-node per-namespace peak breakdown (long form for the report)
        with open(_os.path.join(_out,"node_distribution_ns.csv"),"w") as f:
            f.write("node,namespace,pods_before,pods_peak,pods_after\n")
            for node in sorted(allnodes):
                for ns in allns:
                    b=snc[first_s][node].get(ns,0); p=snc[peak_s][node].get(ns,0); a=snc[last_s][node].get(ns,0)
                    if b or p or a:
                        f.write(f"{node},{_shortns(ns)},{b},{p},{a}\n")
        # record which samples were used
        with open(_os.path.join(_out,"distribution_window.txt"),"w") as f:
            f.write(f"before_sample={first_s}\npeak_sample={peak_s}\nafter_sample={last_s}\n")

# ---- 5. HPA timeline (scaling events: when did each service change replicas) ----
ht=_os.path.join(_out,"hpa_timeline.csv")
if _os.path.exists(ht):
    svc_scaling=_dd(list)
    for r in _csv.DictReader(open(ht)):
        try: cur=int(r["current"] or 0)
        except: continue
        svc_scaling[r["hpa"]].append((int(r["sample"]),cur,r["ts"],r["max"]))
    with open(_os.path.join(_out,"hpa_scaling.csv"),"w") as f:
        f.write("hpa,start_replicas,peak_replicas,max_replicas,scaled_up,hit_max\n")
        for hpa,pts in sorted(svc_scaling.items()):
            reps=[c for _,c,_,_ in pts]
            mx=pts[0][3]
            start=reps[0]; peak=max(reps)
            hit="YES" if str(peak)==str(mx) else "no"
            f.write(f"{hpa},{start},{peak},{mx},{'YES' if peak>start else 'no'},{hit}\n")

print(f"[infra-v2] extended captures written: utilization, restarts, spinup, distribution, hpa-scaling")
