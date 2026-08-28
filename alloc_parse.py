#!/usr/bin/env python3
# alloc_parse.py — per-service CPU/mem request & limit from pod specs, x current & max HPA replicas.
import sys, os, json, glob
outdir = sys.argv[1]; raw = os.path.join(outdir, "raw")

def cpu_m(v):
    if not v: return 0
    v=str(v)
    if v.endswith("m"): return int(v[:-1])
    try: return int(float(v)*1000)
    except: return 0
def mem_mi(v):
    if not v: return 0
    v=str(v)
    u={"Ki":1/1024,"Mi":1,"Gi":1024,"Ti":1024*1024,"K":1000/1048576,"M":1000*1000/1048576,"G":1000**3/1048576}
    for k,m in u.items():
        if v.endswith(k):
            try: return float(v[:-len(k)])*m
            except: return 0
    try: return float(v)/1048576
    except: return 0

def svc_of(pod):
    parts=pod.split("-"); core=parts[:-2] if len(parts)>=3 else parts
    for t in core:
        if "service" in t.lower(): return t
    return core[1] if len(core)>1 else pod

# --- per-service request/limit from the FIRST app pod of each service (spec is identical per replica) ---
svc_res={}  # svc -> {cpu_req,cpu_lim,mem_req,mem_lim, ns}
for pj in glob.glob(os.path.join(raw,"pods_*.json")):
    try: data=json.load(open(pj))
    except: continue
    ns=os.path.basename(pj)[len("pods_"):-len(".json")]
    for pod in data.get("items",[]):
        name=pod["metadata"]["name"]
        svc=svc_of(name)
        # skip infra/monitoring pods that aren't app services
        low=name.lower()
        if any(j in low for j in ("grafana","collector","prometheus","loki","alertmanager","exporter","operator")):
            continue
        if svc in svc_res: continue  # one per service is enough
        # sum across app containers (skip istio-proxy)
        cr=cl=mr=ml=0
        for c in pod["spec"].get("containers",[]):
            if c["name"]=="istio-proxy": continue
            res=c.get("resources",{})
            cr+=cpu_m(res.get("requests",{}).get("cpu")); cl+=cpu_m(res.get("limits",{}).get("cpu"))
            mr+=mem_mi(res.get("requests",{}).get("memory")); ml+=mem_mi(res.get("limits",{}).get("memory"))
        if cr or cl or mr or ml:
            svc_res[svc]={"ns":ns,"cpu_req":cr,"cpu_lim":cl,"mem_req":mr,"mem_lim":ml}

# --- current & max replicas per service from hpa_all.json ---
# Match each service to its HPA within the SAME namespace, preferring the HPA whose
# scaleTargetRef service-token equals the service exactly (avoids frontend/pre-prod mismatches).
hpa_cur={}; hpa_max={}
hpa_list=[]
try:
    for h in json.load(open(os.path.join(raw,"hpa_all.json"))).get("items",[]):
        hpa_list.append({
            "ns": h["metadata"]["namespace"],
            "tgt": h.get("spec",{}).get("scaleTargetRef",{}).get("name",""),
            "cur": h.get("status",{}).get("currentReplicas",0),
            "max": h.get("spec",{}).get("maxReplicas",0),
        })
except: pass

for svc in svc_res:
    svc_ns = svc_res[svc]["ns"]
    best=None
    for hp in hpa_list:
        if hp["ns"] != svc_ns:          # must be same namespace as the pod
            continue
        tgt_svc = svc_of(hp["tgt"]) if "-" in hp["tgt"] else hp["tgt"]
        if tgt_svc == svc:              # exact service-token match — strongest
            best=hp; break
    if not best:                        # fallback: substring within same namespace
        for hp in hpa_list:
            if hp["ns"]==svc_ns and svc in hp["tgt"]:
                best=hp; break
    if best:
        hpa_cur[svc]=best["cur"]; hpa_max[svc]=best["max"]

# --- write resource_alloc.csv ---
with open(os.path.join(outdir,"resource_alloc.csv"),"w") as f:
    f.write("service,namespace,cpu_req_m,cpu_lim_m,mem_req_mi,mem_lim_mi,replicas_cur,replicas_max,"
            "cpu_req_cur_m,cpu_lim_cur_m,mem_req_cur_mi,mem_lim_cur_mi,"
            "cpu_req_max_m,cpu_lim_max_m,mem_req_max_mi,mem_lim_max_mi\n")
    tot={"crc":0,"clc":0,"mrc":0,"mlc":0,"crm":0,"clm":0,"mrm":0,"mlm":0}
    for svc in sorted(svc_res):
        r=svc_res[svc]; cur=hpa_cur.get(svc,1); mx=hpa_max.get(svc,1)
        crc=r["cpu_req"]*cur; clc=r["cpu_lim"]*cur; mrc=r["mem_req"]*cur; mlc=r["mem_lim"]*cur
        crm=r["cpu_req"]*mx;  clm=r["cpu_lim"]*mx;  mrm=r["mem_req"]*mx;  mlm=r["mem_lim"]*mx
        for k,v in zip(["crc","clc","mrc","mlc","crm","clm","mrm","mlm"],[crc,clc,mrc,mlc,crm,clm,mrm,mlm]):
            tot[k]+=v
        f.write(f"{svc},{r['ns']},{r['cpu_req']},{r['cpu_lim']},{r['mem_req']:.0f},{r['mem_lim']:.0f},"
                f"{cur},{mx},{crc},{clc},{mrc:.0f},{mlc:.0f},{crm},{clm},{mrm:.0f},{mlm:.0f}\n")
    f.write(f"TOTAL,,,,,,,,{tot['crc']},{tot['clc']},{tot['mrc']:.0f},{tot['mlc']:.0f},"
            f"{tot['crm']},{tot['clm']},{tot['mrm']:.0f},{tot['mlm']:.0f}\n")

print(f"[alloc] {len(svc_res)} services; totals cur cpu_req={tot['crc']}m mem_req={tot['mrc']:.0f}Mi | max cpu_req={tot['crm']}m mem_req={tot['mrm']:.0f}Mi")
