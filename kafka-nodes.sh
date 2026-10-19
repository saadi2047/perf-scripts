#!/usr/bin/env sh
# kafka-nodes.sh — CPU / memory / storage for the nodes that actually run Kafka.
#
# Derives the node list from the Kafka pods themselves rather than guessing a
# label, so it follows the cluster if pods move. Uses custom-columns and
# jsonpath throughout: `oc get -o wide` renders RESTARTS as "1 (51m ago)",
# which is 3 whitespace fields and shifts every column after it.
#
# usage: ./kafka-nodes.sh [-n namespace]
set -u

NS="${NS:-perf-kafka}"
while [ $# -gt 0 ]; do
  case "$1" in
    -n|--namespace) NS="$2"; shift 2 ;;
    -h|--help) sed -n '2,10p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

command -v oc >/dev/null 2>&1 || { echo "oc not found in PATH" >&2; exit 1; }
oc whoami >/dev/null 2>&1 || { echo "oc session invalid — log in again" >&2; exit 1; }

TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT INT TERM

printf '\n==================================================================================\n'
printf '   KAFKA NODE RESOURCES — %s IST   ns=%s\n' "$(TZ=Asia/Kolkata date '+%Y-%m-%d %H:%M:%S')" "$NS"
printf '==================================================================================\n'

# ---- which nodes run Kafka? ask the pods ------------------------------------
oc -n "$NS" get pods --no-headers \
   -o custom-columns='POD:.metadata.name,NODE:.spec.nodeName,PHASE:.status.phase' \
   2>/dev/null > "$TMP/pods"
[ -s "$TMP/pods" ] || { echo "  no pods found in namespace $NS" >&2; exit 1; }

# brokers and controllers only — cruise-control / entity-operator land on
# general workers and would drag unrelated nodes into the report
awk '$1 ~ /-(broker|controller)-[0-9]+$/ && $3=="Running" {print $2}' "$TMP/pods" \
  | sort -u > "$TMP/nodes"
[ -s "$TMP/nodes" ] || awk '$3=="Running"{print $2}' "$TMP/pods" | sort -u > "$TMP/nodes"

NODECOUNT=$(wc -l < "$TMP/nodes" | tr -d ' ')
printf '\n  Kafka broker/controller nodes: %s\n' "$NODECOUNT"
while read -r n; do
  [ -n "$n" ] && printf '    %s\n' "$n"
done < "$TMP/nodes"

# ---- live usage (metrics-server) --------------------------------------------
oc adm top node --no-headers 2>/dev/null > "$TMP/top" || : > "$TMP/top"

# ---- capacity, allocatable, pressure ---------------------------------------
printf '\n  [1] CAPACITY vs ALLOCATABLE vs ACTUAL USE\n'
printf '  ----------------------------------------------------------------------------------\n'
printf '  %-26s %8s %9s %8s %7s %10s %11s %9s %6s\n' \
       NODE cpuCap cpuAlloc cpuUsed 'cpu%' memCapGi memAllocGi memUsedGi 'mem%'
printf '  ---------------------------------------------------------------------------------------------------\n'

while read -r NODE; do
  [ -z "$NODE" ] && continue
  oc get node "$NODE" -o jsonpath='{.status.capacity.cpu}{"\t"}{.status.allocatable.cpu}{"\t"}{.status.capacity.memory}{"\t"}{.status.allocatable.memory}{"\n"}' \
     2>/dev/null > "$TMP/cap" || : > "$TMP/cap"
  grep -E "^${NODE}[[:space:]]" "$TMP/top" > "$TMP/t1" 2>/dev/null || : > "$TMP/t1"

  awk -v node="$NODE" '
    function cpu2m(v) {            # "8" -> 8000m ; "7500m" -> 7500
      if (v ~ /m$/) { sub(/m$/,"",v); return v+0 }
      return (v+0) * 1000
    }
    function mem2mi(v) {           # Ki/Mi/Gi/K/M/G -> MiB
      if (v ~ /Ki$/) { sub(/Ki$/,"",v); return (v+0)/1024 }
      if (v ~ /Mi$/) { sub(/Mi$/,"",v); return (v+0) }
      if (v ~ /Gi$/) { sub(/Gi$/,"",v); return (v+0)*1024 }
      if (v ~ /Ti$/) { sub(/Ti$/,"",v); return (v+0)*1048576 }
      if (v ~ /K$/)  { sub(/K$/,"",v);  return (v+0)/1024 }
      if (v ~ /M$/)  { sub(/M$/,"",v);  return (v+0) }
      if (v ~ /G$/)  { sub(/G$/,"",v);  return (v+0)*1024 }
      return (v+0)/1048576         # bare bytes
    }
    NR==FNR { cc=cpu2m($1); ca=cpu2m($2); mc=mem2mi($3); ma=mem2mi($4); next }
    {                              # oc adm top node: NAME cpu cpu% mem mem%
      cu=cpu2m($2); mu=mem2mi($4)
    }
    END {
      cpct = (ca>0 && cu>0) ? sprintf("%.1f%%", cu/ca*100) : "-"
      mpct = (ma>0 && mu>0) ? sprintf("%.1f%%", mu/ma*100) : "-"
      cus  = (cu>0) ? sprintf("%dm", cu) : "-"
      mus  = (mu>0) ? sprintf("%.1f", mu/1024) : "-"
      printf "  %-26s %8s %9s %8s %7s %10.1f %11.1f %9s %6s\n",
             node, sprintf("%dm",cc), sprintf("%dm",ca), cus, cpct,
             mc/1024, ma/1024, mus, mpct
    }' "$TMP/cap" "$TMP/t1"
done < "$TMP/nodes"

printf '      cpuAlloc/memAlloc are what the scheduler can hand out; capacity minus\n'
printf '      system reserved. cpu%%/mem%% are measured use against allocatable.\n'

# ---- what is requested/limited on each node --------------------------------
printf '\n  [2] SCHEDULED REQUESTS AND LIMITS ON THOSE NODES   (all namespaces)\n'
printf '  ----------------------------------------------------------------------------------\n'
printf '  %-26s %10s %10s %12s %12s %6s\n' NODE cpuReq cpuLim memReqGi memLimGi pods
printf '  ----------------------------------------------------------------------------------\n'

oc get pods -A --field-selector=status.phase=Running -o json 2>/dev/null > "$TMP/all.json" || : > "$TMP/all.json"
if [ -s "$TMP/all.json" ]; then
  python3 -I - "$TMP/all.json" "$TMP/nodes" <<'PY'
import json, sys

def cpu_m(v):
    if not v: return 0.0
    v = str(v)
    if v.endswith('m'):  return float(v[:-1])
    if v.endswith('n'):  return float(v[:-1]) / 1e6
    if v.endswith('u'):  return float(v[:-1]) / 1e3
    return float(v) * 1000

def mem_mi(v):
    if not v: return 0.0
    v = str(v)
    for suf, mul in (('Ki', 1/1024), ('Mi', 1), ('Gi', 1024), ('Ti', 1048576),
                     ('K', 1/1024), ('M', 1), ('G', 1024), ('T', 1048576)):
        if v.endswith(suf):
            return float(v[:-len(suf)]) * mul
    return float(v) / 1048576

want = {l.strip() for l in open(sys.argv[2]) if l.strip()}
agg = {n: {'cr': 0.0, 'cl': 0.0, 'mr': 0.0, 'ml': 0.0, 'n': 0} for n in want}
try:
    doc = json.load(open(sys.argv[1]))
except Exception:
    doc = {'items': []}

for p in doc.get('items', []):
    node = (p.get('spec') or {}).get('nodeName')
    if node not in agg:
        continue
    agg[node]['n'] += 1
    # initContainers run before app containers, so the node must reserve the
    # MAX of (sum of regular containers, largest init container) -- summing
    # both would overstate. Regular containers dominate here; keep it simple
    # and explicit rather than silently wrong.
    for c in (p['spec'].get('containers') or []):
        r = (c.get('resources') or {})
        req, lim = r.get('requests') or {}, r.get('limits') or {}
        agg[node]['cr'] += cpu_m(req.get('cpu'))
        agg[node]['cl'] += cpu_m(lim.get('cpu'))
        agg[node]['mr'] += mem_mi(req.get('memory'))
        agg[node]['ml'] += mem_mi(lim.get('memory'))

for n in sorted(agg):
    a = agg[n]
    print("  %-26s %10s %10s %12.1f %12.1f %6d" % (
        n, "%dm" % a['cr'], "%dm" % a['cl'], a['mr']/1024, a['ml']/1024, a['n']))
PY
else
  printf '  (could not list pods cluster-wide — needs permission on all namespaces)\n'
fi

printf '      Requests are the scheduler contract. cpuReq above cpuAlloc means the node\n'
printf '      is oversubscribed; cpuLim far above cpuAlloc means bursting can starve peers.\n'

# ---- storage: PVCs backing Kafka -------------------------------------------
printf '\n  [3] KAFKA PERSISTENT STORAGE\n'
printf '  ----------------------------------------------------------------------------------\n'
printf '  %-42s %-18s %10s %10s\n' PVC POD/NODE requestGi statusPhase
printf '  ----------------------------------------------------------------------------------\n'
oc -n "$NS" get pvc --no-headers \
   -o custom-columns='PVC:.metadata.name,REQ:.spec.resources.requests.storage,PHASE:.status.phase' \
   2>/dev/null | while read -r PVC REQ PHASE; do
  [ -z "${PVC:-}" ] && continue
  OWNER=$(awk -v p="$PVC" '
    { pod=$1; node=$2
      # data-0-perf-kafka-cluster-broker-0 -> perf-kafka-cluster-broker-0
      if (index(p, pod) > 0) { print node; exit } }' "$TMP/pods")
  printf '  %-42s %-18s %10s %10s\n' "$PVC" "${OWNER:--}" "${REQ:--}" "${PHASE:--}"
done

printf '\n      Live used%% per PVC comes from the run CSVs (kafka_disk_<tag>.csv) or:\n'
printf '        oc -n %s exec perf-kafka-cluster-broker-0 -c kafka -- df -h /var/lib/kafka/data-0\n' "$NS"

# ---- node conditions --------------------------------------------------------
printf '\n  [4] NODE CONDITIONS   (pressure flags abort a valid perf run)\n'
printf '  ----------------------------------------------------------------------------------\n'
printf '  %-26s %8s %10s %10s %10s %9s\n' NODE Ready MemPress DiskPress PIDPress Schedulable
printf '  ----------------------------------------------------------------------------------\n'
while read -r NODE; do
  [ -z "$NODE" ] && continue
  oc get node "$NODE" -o json 2>/dev/null \
  | python3 -I -c '
import json,sys
d=json.load(sys.stdin)
c={x["type"]: x["status"] for x in (d.get("status",{}).get("conditions") or [])}
unsched = (d.get("spec",{}) or {}).get("unschedulable", False)
print("  %-26s %8s %10s %10s %10s %9s" % (
    d["metadata"]["name"],
    c.get("Ready","?"), c.get("MemoryPressure","?"),
    c.get("DiskPressure","?"), c.get("PIDPressure","?"),
    "no" if unsched else "yes"))' 2>/dev/null \
  || printf '  %-26s %8s\n' "$NODE" "read failed"
done < "$TMP/nodes"

printf '\n==================================================================================\n'
printf '  Ready=True and all three pressures=False is the only state in which throughput\n'
printf '  numbers mean anything. MemoryPressure or DiskPressure True invalidates the run.\n'
printf '==================================================================================\n\n'
