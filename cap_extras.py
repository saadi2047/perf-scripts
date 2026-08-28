#!/usr/bin/env python3
# Parses HPA (live min/max/current - catches repo-vs-live drift), Pending pods,
# per-namespace request totals. Appends to the capacity output dir.
import json, sys, os, re
outdir = sys.argv[1]; raw = os.path.join(outdir, "raw")

# ---- HPA: live vs repo drift ----
hpa_rows = []
try:
    with open(os.path.join(raw, "hpa_all.json")) as f:
        for h in json.load(f).get("items", []):
            ns = h["metadata"]["namespace"]; name = h["metadata"]["name"]
            spec = h.get("spec", {}); status = h.get("status", {})
            hpa_rows.append({
                "ns": ns, "name": name,
                "min": spec.get("minReplicas", ""), "max": spec.get("maxReplicas", ""),
                "current": status.get("currentReplicas", ""),
                "desired": status.get("desiredReplicas", ""),
            })
except FileNotFoundError:
    pass
with open(os.path.join(outdir, "hpa_state.csv"), "w") as f:
    f.write("namespace,hpa,min,max,current,desired\n")
    for h in hpa_rows:
        f.write(f"{h['ns']},{h['name']},{h['min']},{h['max']},{h['current']},{h['desired']}\n")

# ---- Pending pods ----
pending = []
try:
    with open(os.path.join(raw, "pods_all.txt")) as f:
        for line in f:
            if "Pending" in line:
                parts = line.split()
                if len(parts) >= 2:
                    pending.append((parts[0], parts[1]))  # ns, pod
except FileNotFoundError:
    pass
with open(os.path.join(outdir, "pending_pods.csv"), "w") as f:
    f.write("namespace,pod\n")
    for ns, pod in pending:
        f.write(f"{ns},{pod}\n")

print(f"[extras] HPA rows: {len(hpa_rows)}, Pending pods: {len(pending)}")
# flag any txn HPA drift (repo says max 15; live may be 30)
for h in hpa_rows:
    if "transaction" in h["name"].lower():
        print(f"[extras] TXN HPA live: min={h['min']} max={h['max']} current={h['current']}  (repo says max=15 — flag if different)")
