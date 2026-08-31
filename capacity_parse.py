#!/usr/bin/env python3
# Reads raw/nodes.json + raw/describe_nodes.txt, classifies nodes by real topology,
# computes per-node allocatable/reserved/used/free, writes capacity CSVs.
import json, sys, os, re

outdir = sys.argv[1]
raw = os.path.join(outdir, "raw")

def cpu_to_millicores(v):
    if v is None: return 0
    v = str(v)
    if v.endswith("m"): return int(v[:-1])
    try: return int(float(v) * 1000)
    except: return 0

def mem_to_mi(v):
    if v is None: return 0
    v = str(v)
    units = {"Ki": 1/1024, "Mi": 1, "Gi": 1024, "Ti": 1024*1024,
             "K": 1000/1024/1024*1024, "M": 1000*1000/1024/1024, "G": 1000**3/1024/1024}
    for u, mult in units.items():
        if v.endswith(u):
            try: return float(v[:-len(u)]) * mult
            except: return 0
    try: return float(v) / 1024 / 1024  # bytes
    except: return 0

# ---- classify a node from its labels + taints ----
def has_noschedule(taints):
    return any(t.get("effect") in ("NoSchedule","NoExecute") for t in (taints or []))

def classify(labels, taints):
    role_master = "node-role.kubernetes.io/master" in labels or "node-role.kubernetes.io/control-plane" in labels
    role_infra  = "node-role.kubernetes.io/infra" in labels
    wl = labels.get("workload-type", "")
    if role_master: return "master"
    if wl == "kafka": return "kafka"
    # gemfire nodes are used as regular workers (gemfire plan was dropped).
    # Classify as app-worker; the tainted flag is tracked separately for a report note.
    if wl == "gemfire":
        return "app-worker"
    if labels.get("node") in ("loki","quay"): return "infra-dedicated"
    if role_infra: return "infra"
    if "node-role.kubernetes.io/worker" in labels: return "app-worker"
    return "other"

_nodes_path = os.path.join(raw, "nodes.json")
if not os.path.exists(_nodes_path) or os.path.getsize(_nodes_path) == 0:
    print(f"    [parser] WARN: {_nodes_path} missing or empty — capacity parsing skipped "
          f"(oc get nodes returned nothing). Re-run this phase for capacity data.")
    sys.exit(0)
try:
    with open(_nodes_path) as f:
        nodes = json.load(f)["items"]
except Exception as e:
    print(f"    [parser] WARN: could not read nodes.json ({e}) — capacity parsing skipped.")
    sys.exit(0)

rows = []
for n in nodes:
    name = n["metadata"]["name"]
    labels = n["metadata"].get("labels", {})
    taints = n.get("spec", {}).get("taints", [])
    cls = classify(labels, taints)
    is_gem_tainted = (labels.get("workload-type")=="gemfire" and has_noschedule(taints))
    alloc = n["status"].get("allocatable", {})
    cap = n["status"].get("capacity", {})
    alloc_cpu = cpu_to_millicores(alloc.get("cpu"))
    alloc_mem = mem_to_mi(alloc.get("memory"))
    rows.append({
        "node": name, "class": cls,
        "alloc_cpu_m": alloc_cpu, "alloc_mem_mi": round(alloc_mem),
        "instance": labels.get("node.kubernetes.io/instance-type", ""),
        "gem_tainted": is_gem_tainted,
    })

# reserved (sum of requests) + used come from describe_nodes.txt "Allocated resources"
# parse describe for per-node requests
reserved = {}
cur = None
try:
    with open(os.path.join(raw, "describe_nodes.txt")) as f:
        for line in f:
            m = re.match(r"^Name:\s+(\S+)", line)
            if m: cur = m.group(1); continue
            # "  cpu                 12500m (78%)"
            m = re.match(r"^\s+cpu\s+(\d+)m?\s+\((\d+)%\)", line)
            if m and cur and cur not in reserved:
                reserved[cur] = {"cpu_m": int(m.group(1)), "cpu_pct": int(m.group(2))}
except FileNotFoundError:
    pass

for r in rows:
    rr = reserved.get(r["node"], {})
    r["reserved_cpu_m"] = rr.get("cpu_m", "")
    r["reserved_cpu_pct"] = rr.get("cpu_pct", "")
    r["free_cpu_m"] = (r["alloc_cpu_m"] - rr["cpu_m"]) if "cpu_m" in rr else ""

# write per-node CSV
with open(os.path.join(outdir, "node_capacity.csv"), "w") as f:
    f.write("node,class,instance,alloc_cpu_m,alloc_mem_mi,reserved_cpu_m,reserved_cpu_pct,free_cpu_m\n")
    for r in rows:
        f.write(f"{r['node']},{r['class']},{r['instance']},{r['alloc_cpu_m']},{r['alloc_mem_mi']},{r['reserved_cpu_m']},{r['reserved_cpu_pct']},{r['free_cpu_m']}\n")

# ---- capacity summary BY CLASS (the fix for 232-vs-186) ----
from collections import defaultdict
byc = defaultdict(lambda: {"n":0,"alloc":0,"resv":0,"free":0,"amem":0})
for r in rows:
    c = byc[r["class"]]
    c["n"] += 1; c["alloc"] += r["alloc_cpu_m"]; c["amem"] += r["alloc_mem_mi"]
    if isinstance(r["reserved_cpu_m"], int): c["resv"] += r["reserved_cpu_m"]
    if isinstance(r["free_cpu_m"], int): c["free"] += r["free_cpu_m"]

with open(os.path.join(outdir, "capacity_by_class.csv"), "w") as f:
    f.write("class,nodes,alloc_cores,reserved_cores,free_cores,alloc_mem_gb\n")
    for c, d in sorted(byc.items()):
        f.write(f"{c},{d['n']},{d['alloc']/1000:.1f},{d['resv']/1000:.1f},{d['free']/1000:.1f},{d['amem']/1024:.0f}\n")

# largest single-node free slot among app-workers (can a 3000m pod fit?)
APP_CLASSES = ("app-worker",)
app_free = [r["free_cpu_m"] for r in rows if r["class"] in APP_CLASSES and isinstance(r["free_cpu_m"],int)]
largest = max(app_free) if app_free else 0
with open(os.path.join(outdir, "fit_check.txt"), "w") as f:
    f.write(f"app_worker_nodes={sum(1 for r in rows if r['class'] in APP_CLASSES)}\n")
    f.write(f"largest_single_node_free_cpu_m={largest}\n")
    f.write(f"can_fit_3000m_txn_pod={'YES' if largest>=3000 else 'NO'}\n")
    f.write(f"total_app_worker_free_cores={sum(app_free)/1000:.1f}\n")


# ---- node-level USED cpu/mem from top_nodes.txt (before/after utilization) ----
node_used = {}
tp = os.path.join(raw, "top_nodes.txt")
if os.path.exists(tp):
    for line in open(tp):
        p = line.split()
        # NAME CPU(cores) CPU% MEMORY(bytes) MEMORY%
        if len(p) >= 5 and p[1].endswith("m") and p[2].endswith("%"):
            name=p[0]
            node_used[name]={"cpu_m":p[1].rstrip("m"),"cpu_pct":p[2].rstrip("%"),
                             "mem_mi":p[3].replace("Mi",""),"mem_pct":p[4].rstrip("%")}
# merge into node_capacity rows + write a focused node_usage.csv (app-workers only, sorted by cpu%)
with open(os.path.join(outdir,"node_usage.csv"),"w") as f:
    f.write("node,class,cpu_used_m,cpu_pct,mem_used_mi,mem_pct\n")
    for r in rows:
        u=node_used.get(r["node"],{})
        f.write(f"{r['node']},{r['class']},{u.get('cpu_m','')},{u.get('cpu_pct','')},{u.get('mem_mi','')},{u.get('mem_pct','')}\n")
print(f"[parser] node usage parsed for {len(node_used)} nodes")

print(f"[parser] {len(rows)} nodes classified")
napp = sum(1 for r in rows if r['class'] in APP_CLASSES)
ngem = sum(1 for r in rows if r.get('gem_tainted'))
print(f"[parser] app pool: {napp} worker nodes ({ngem} carry a gemfire taint -> need matching toleration)")
with open(os.path.join(outdir,'node_notes.txt'),'w') as nf:
    nf.write(f"app_worker_total={napp}\n")
    nf.write(f"gemfire_tainted_workers={ngem}\n")
    nf.write("note=gemfire nodes run as regular workers (gemfire plan dropped); tainted ones need a matching toleration to receive a given service\n")
print(f"[parser] largest app-worker free slot: {largest}m (fit 3000m pod: {'YES' if largest>=3000 else 'NO'})")
