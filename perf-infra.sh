#!/usr/bin/env sh
# ============================================================================
# perf-infra.sh — continuous infra/platform collector for a 1-hour load test
# SBI ePay 2.0 PERF
# ----------------------------------------------------------------------------
# Read-only. POSIX sh. Requires: oc (logged in), python3, zip.
# Samples every 60s for 60 min (configurable). Per-sample, per-namespace:
#   - pod CPU/memory (oc adm top pods)
#   - pod status + restart counts (delta detection)
#   - CPU throttling (from pod cgroup via metrics)
#   - Pending pods + scheduler events (split by reason x namespace)
#   - probe failures (503 readiness/liveness) from describe
# Each sample is wall-clock timestamped. Overrunning cycles are skipped+warned.
#
# Usage:
#   ./perf-infra.sh                       # 60 samples @ 60s = 1 hour
#   ./perf-infra.sh -i 60 -d 3600 -o /path
#   ./perf-infra.sh --samples 60
# ============================================================================
set -u

# count matches, always a clean integer (grep -c can emit trailing ws; guard empties)
count() { c=$(grep -cE "$1" "$2" 2>/dev/null); c=$(printf '%s' "$c" | tr -d ' \n\r'); [ -z "$c" ] && c=0; printf '%s' "$c"; }

INTERVAL=60
DURATION=3600
DEEP_EVERY=3        # run the expensive JSON/event captures every Nth sample (1 = every sample)
OUTBASE="."
SAMPLES=""
NAMESPACES="perf-transaction perf-admin perf-kms perf-merchant perf-simulators perf-notification perf-epay-common perf-frontend perf-istio-system perf-kafka perf-tempo"

while [ $# -gt 0 ]; do
  case "$1" in
    -i|--interval) INTERVAL="$2"; shift 2 ;;
    -d|--duration) DURATION="$2"; shift 2 ;;
    --samples) SAMPLES="$2"; shift 2 ;;
    -o) OUTBASE="$2"; shift 2 ;;
    -n) NAMESPACES="$2"; shift 2 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown arg: $1"; exit 1 ;;
  esac
done

[ -z "$SAMPLES" ] && SAMPLES=$(( DURATION / INTERVAL ))
# Hard wall-clock deadline — the run never exceeds the requested duration.
START_EPOCH=$(date +%s)
DEADLINE=$(( START_EPOCH + DURATION ))

command -v oc >/dev/null 2>&1 || { echo "FATAL: oc not found"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "FATAL: python3 not found"; exit 1; }
oc whoami >/dev/null 2>&1 || { echo "FATAL: not logged in"; exit 1; }

TS=$(date +%Y%m%d_%H%M%S)
OUTDIR="$OUTBASE/perf_infra_${TS}"
mkdir -p "$OUTDIR/samples"
echo "[*] perf-infra.sh  interval=${INTERVAL}s  samples=${SAMPLES}  (~$((SAMPLES*INTERVAL/60)) min)"
echo "[*] namespaces: $NAMESPACES"
echo "[*] out: $OUTDIR"

# rolling CSV headers
echo "sample,ts,namespace,pod,cpu_m,mem_mi" > "$OUTDIR/pod_usage.csv"
echo "sample,ts,namespace,pod,restarts,status" > "$OUTDIR/pod_status.csv"
echo "sample,ts,namespace,pod,node,ip" > "$OUTDIR/pod_placement.csv"
echo "sample,ts,namespace,pod,message" > "$OUTDIR/pending_events.csv"
echo "sample,ts,namespace,pod,container,cpu_m,mem_mi" > "$OUTDIR/container_usage.csv"
echo "sample,ts,namespace,pod,restart_count,reason,exit_code" > "$OUTDIR/restart_reasons.csv"
echo "sample,ts,namespace,pod,phase,created,scheduled,ready,spinup_s" > "$OUTDIR/pod_spinup.csv"
echo "sample,ts,namespace,hpa,min,max,current,desired,target_metric" > "$OUTDIR/hpa_timeline.csv"
echo "sample,ts,namespace,reason,count" > "$OUTDIR/scheduler_events.csv"
echo "sample,ts,namespace,pod,probe,failures" > "$OUTDIR/probe_failures.csv"
echo "sample,ts,node,cpu_m,cpu_pct,mem_mi,mem_pct" > "$OUTDIR/node_usage.csv"

NSFLAG=""
for ns in $NAMESPACES; do NSFLAG="$NSFLAG -n $ns"; done

# Capture-health counters. A long run can start failing halfway through (expired
# token, throttled API) and every oc call here is 2>/dev/null, so without this the
# CSVs just go quiet and the report looks like "nothing happened" instead of
# "we stopped being able to look".
EMPTY_NODES=0; EMPTY_PODS=0; EMPTY_TOP=0; SAMPLES_RUN=0

i=0
while [ "$i" -lt "$SAMPLES" ]; do
  if [ "$(date +%s)" -ge "$DEADLINE" ]; then
    echo "[*] duration limit reached (${DURATION}s) after $i sample(s) — stopping on schedule."
    break
  fi
  i=$((i+1))
  cyc_start=$(date +%s)
  now=$(date -u +%FT%TZ)
  sdir="$OUTDIR/samples/$(printf 'snap_%03d' "$i")"
  mkdir -p "$sdir"
  printf "[%s] sample %d/%d ... " "$now" "$i" "$SAMPLES"

  # --- node-level usage (whole cluster), once per sample ---
  oc adm top nodes --no-headers 2>/dev/null > "$sdir/top_nodes.txt"
  if [ ! -s "$sdir/top_nodes.txt" ]; then
    EMPTY_NODES=$((EMPTY_NODES+1))
    echo ""; echo "    [!] sample $i: 'oc adm top nodes' returned nothing (metrics API down, or session expired?)"
  fi
  while read -r nname ncpu ncpupct nmem nmempct _r; do
    [ -z "$nname" ] && continue
    echo "$i,$now,$nname,$(echo "$ncpu"|sed 's/m//'),$(echo "$ncpupct"|sed 's/%//'),$(echo "$nmem"|sed 's/Mi//'),$(echo "$nmempct"|sed 's/%//')" >> "$OUTDIR/node_usage.csv"
  done < "$sdir/top_nodes.txt"

  # --- HPA timeline: replica counts per perf HPA each sample ---
  oc get hpa -A -o json 2>/dev/null > "$sdir/hpa.json"
  if [ -s "$sdir/hpa.json" ]; then
    python3 - "$sdir/hpa.json" "$i" "$now" >> "$OUTDIR/hpa_timeline.csv" 2>/dev/null <<'PYHPA'
import json,sys
f,samp,ts=sys.argv[1:4]
try: data=json.load(open(f))
except: sys.exit()
for h in data.get("items",[]):
    ns=h["metadata"]["namespace"]
    if not ns.startswith("perf-"): continue
    nm=h["metadata"]["name"]; sp=h.get("spec",{}); st=h.get("status",{})
    tm=""
    try: tm=str(st.get("currentMetrics",[{}])[0].get("resource",{}).get("current",{}).get("averageUtilization",""))
    except: pass
    print(f"{samp},{ts},{ns},{nm},{sp.get('minReplicas','')},{sp.get('maxReplicas','')},{st.get('currentReplicas','')},{st.get('desiredReplicas','')},{tm}")
PYHPA
  fi

  for ns in $NAMESPACES; do
    # --- CPU/mem via adm top ---
    oc -n "$ns" adm top pods --no-headers 2>/dev/null > "$sdir/top_${ns}.txt"
    [ -s "$sdir/top_${ns}.txt" ] || EMPTY_TOP=$((EMPTY_TOP+1))
    # per-container split (app vs istio-proxy sidecar) — lets the report show app-only CPU,
    # which is what Dynatrace's per-container view reports.
    oc -n "$ns" adm top pods --containers --no-headers 2>/dev/null > "$sdir/topc_${ns}.txt"
    if [ -s "$sdir/topc_${ns}.txt" ]; then
      while read -r pod cont cpu mem _rest; do
        [ -z "$cont" ] && continue
        cc=$(echo "$cpu" | sed 's/m//'); cm=$(echo "$mem" | sed 's/Mi//')
        echo "$i,$now,$ns,$pod,$cont,${cc:-0},${cm:-0}" >> "$OUTDIR/container_usage.csv"
      done < "$sdir/topc_${ns}.txt"
    fi
    while read -r pod cpu mem _rest; do
      [ -z "$pod" ] && continue
      cpu_m=$(echo "$cpu" | sed 's/m//')
      mem_mi=$(echo "$mem" | sed 's/Mi//')
      echo "$i,$now,$ns,$pod,$cpu_m,$mem_mi" >> "$OUTDIR/pod_usage.csv"
    done < "$sdir/top_${ns}.txt"

    # --- status + restarts + placement ---
    # custom-columns, NOT -o wide. In `-o wide` a RESTARTS cell renders as
    # "1 (51m ago)" — three whitespace fields — which shifted every later column,
    # so `node` silently received the pod's AGE ("51m") and pod_placement.csv (and
    # therefore node_distribution.csv in the report) was wrong for every pod that
    # had restarted. custom-columns has one field per column, always.
    # restartCount is per container, so it arrives as "0,3" and has to be summed;
    # `-o wide` used to do that sum for us.
    oc -n "$ns" get pods --no-headers 2>/dev/null \
       -o custom-columns='POD:.metadata.name,PHASE:.status.phase,RC:.status.containerStatuses[*].restartCount,IP:.status.podIP,NODE:.spec.nodeName' \
       > "$sdir/pods_${ns}.txt"
    [ -s "$sdir/pods_${ns}.txt" ] || EMPTY_PODS=$((EMPTY_PODS+1))
    while read -r pod status rcs ip node _rest; do
      [ -z "$pod" ] && continue
      rc=$(echo "${rcs:-0}" | awk -F, '{s=0; for(j=1;j<=NF;j++) s+=$j+0; print s}')
      echo "$i,$now,$ns,$pod,${rc:-0},$status" >> "$OUTDIR/pod_status.csv"
      # placement + network: pod -> node, ip (for distribution + traceability)
      echo "$i,$now,$ns,$pod,${node:-?},${ip:-?}" >> "$OUTDIR/pod_placement.csv"
    done < "$sdir/pods_${ns}.txt"

    # --- deep pod details via JSON: restart reasons (OOM/exit), spin-up timings ---
    # (expensive: run every DEEP_EVERY-th sample, and always on the first/last)
    if [ $(( (i - 1) % DEEP_EVERY )) -eq 0 ] || [ "$i" -eq "$SAMPLES" ]; then
    oc -n "$ns" get pods -o json 2>/dev/null > "$sdir/podsjson_${ns}.json"
    if [ -s "$sdir/podsjson_${ns}.json" ]; then
      python3 - "$sdir/podsjson_${ns}.json" "$i" "$now" "$ns" >> "$OUTDIR/restart_reasons.csv" 2>/dev/null <<'PYJSON'
import json,sys,datetime
f,samp,ts,ns=sys.argv[1:5]
try: data=json.load(open(f))
except: sys.exit()
for p in data.get("items",[]):
    nm=p["metadata"]["name"]
    for cs in p.get("status",{}).get("containerStatuses",[]):
        rc=cs.get("restartCount",0)
        if rc>0:
            last=cs.get("lastState",{}).get("terminated",{})
            reason=last.get("reason",""); ec=last.get("exitCode","")
            if reason or ec!="":
                print(f"{samp},{ts},{ns},{nm},{rc},{reason},{ec}")
PYJSON
      python3 - "$sdir/podsjson_${ns}.json" "$i" "$now" "$ns" >> "$OUTDIR/pod_spinup.csv" 2>/dev/null <<'PYJSON'
import json,sys,datetime
f,samp,ts,ns=sys.argv[1:5]
def parse(t):
    try: return datetime.datetime.strptime(t,"%Y-%m-%dT%H:%M:%SZ")
    except: return None
try: data=json.load(open(f))
except: sys.exit()
for p in data.get("items",[]):
    nm=p["metadata"]["name"]; st=p.get("status",{})
    phase=st.get("phase","")
    created=p["metadata"].get("creationTimestamp","")
    sched=ready=""
    for c in st.get("conditions",[]):
        if c["type"]=="PodScheduled": sched=c.get("lastTransitionTime","")
        if c["type"]=="Ready": ready=c.get("lastTransitionTime","")
    cd=parse(created); rd=parse(ready)
    spin=(rd-cd).total_seconds() if cd and rd else ""
    print(f"{samp},{ts},{ns},{nm},{phase},{created},{sched},{ready},{spin}")
PYJSON
    fi
    fi

    # --- liveness/readiness probe failures (explains SIGTERM restarts) ---
    # (events are cumulative, so sampling every DEEP_EVERY-th cycle loses nothing)
    if [ $(( (i - 1) % DEEP_EVERY )) -eq 0 ] || [ "$i" -eq "$SAMPLES" ]; then
    oc -n "$ns" get events --field-selector reason=Unhealthy --no-headers 2>/dev/null > "$sdir/probe_${ns}.txt"
    fi
    if [ -s "$sdir/probe_${ns}.txt" ]; then
      # count per pod, split liveness vs readiness
      awk -v s="$i" -v t="$now" -v n="$ns" '
        { pod=""; probe="unknown";
          for(f=1;f<=NF;f++){
            if($f ~ /^pod\//){ pod=$f; sub(/^pod\//,"",pod) }
            if($f ~ /[Ll]iveness/) probe="liveness";
            else if($f ~ /[Rr]eadiness/ && probe=="unknown") probe="readiness";
            else if($f ~ /[Ss]tartup/ && probe=="unknown") probe="startup";
          }
          if(pod!="") cnt[pod"|"probe]++
        }
        END { for(k in cnt){ split(k,a,"|"); print s","t","n","a[1]","a[2]","cnt[k] } }
      ' "$sdir/probe_${ns}.txt" >> "$OUTDIR/probe_failures.csv"
    fi

    # --- scheduler events split by reason ---
    # `oc get events` returns EVERY retained FailedScheduling event, including ones
    # that fired long before this run started — reading them each cycle counted the
    # same stale event once per sample and reported it as live CPU pressure.
    # Column 1 is the event's age (13s / 4m32s / 100m / 2d1h): keep only events
    # younger than the time elapsed since the run began.
    _elapsed=$(( $(date +%s) - START_EPOCH + 30 ))
    oc -n "$ns" get events --field-selector reason=FailedScheduling --no-headers 2>/dev/null \
      > "$sdir/sched_all_${ns}.txt"
    awk -v lim="$_elapsed" '
      function age(a,   n,v,u,tot,i,c) {
        tot=0; n="";
        for (i=1; i<=length(a); i++) {
          c=substr(a,i,1)
          if (c ~ /[0-9]/) { n = n c }
          else {
            v = n+0; n=""
            if      (c=="s") tot += v
            else if (c=="m") tot += v*60
            else if (c=="h") tot += v*3600
            else if (c=="d") tot += v*86400
            else if (c=="y") tot += v*31536000
            else return -1
          }
        }
        return (n=="") ? tot : -1
      }
      { a = age($1); if (a >= 0 && a <= lim) print }
    ' "$sdir/sched_all_${ns}.txt" > "$sdir/sched_${ns}.txt"
    # per-pod FailedScheduling message (the WHY a pod is Pending)
    if [ -s "$sdir/sched_${ns}.txt" ]; then
      awk -v s="$i" -v t="$now" -v n="$ns" '
        { pod="";
          for(f=1;f<=NF;f++) if($f ~ /^pod\//){ pod=$f; sub(/^pod\//,"",pod) }
          msg=""; k=index($0,"0/"); if(k>0) msg=substr($0,k);
          gsub(/,/,";",msg); gsub(/\r/,"",msg);
          if(pod!="" && msg!="") print s","t","n","pod","msg
        }' "$sdir/sched_${ns}.txt" >> "$OUTDIR/pending_events.csv"
    fi
    if [ -s "$sdir/sched_${ns}.txt" ]; then
      # bucket by the reason substring
      ins=$(count 'Insufficient cpu' "$sdir/sched_${ns}.txt")
      insm=$(count 'Insufficient memory' "$sdir/sched_${ns}.txt")
      aff=$(count 'affinity|selector' "$sdir/sched_${ns}.txt")
      pvc=$(count 'pvc|volume|persistentvolume' "$sdir/sched_${ns}.txt")
      taint=$(count 'taint|untolerated' "$sdir/sched_${ns}.txt")
      [ "$ins" -gt 0 ] && echo "$i,$now,$ns,Insufficient_cpu,$ins" >> "$OUTDIR/scheduler_events.csv"
      [ "$insm" -gt 0 ] && echo "$i,$now,$ns,Insufficient_memory,$insm" >> "$OUTDIR/scheduler_events.csv"
      [ "$aff" -gt 0 ] && echo "$i,$now,$ns,node_affinity,$aff" >> "$OUTDIR/scheduler_events.csv"
      [ "$pvc" -gt 0 ] && echo "$i,$now,$ns,pvc_volume,$pvc" >> "$OUTDIR/scheduler_events.csv"
      [ "$taint" -gt 0 ] && echo "$i,$now,$ns,taint,$taint" >> "$OUTDIR/scheduler_events.csv"
    fi
  done

  # cycle timing / overrun guard
  SAMPLES_RUN=$i
  cyc_end=$(date +%s)
  took=$((cyc_end - cyc_start))
  echo "done (${took}s)"
  if [ "$took" -ge "$INTERVAL" ]; then
    echo "    [!] WARN: cycle took ${took}s >= interval ${INTERVAL}s (no sleep; next starts immediately)"
  else
    if [ "$i" -lt "$SAMPLES" ]; then
      _rem=$(( DEADLINE - $(date +%s) ))
      _nap=$(( INTERVAL - took ))
      [ "$_nap" -gt "$_rem" ] && _nap="$_rem"
      [ "$_nap" -gt 0 ] && sleep "$_nap"
    fi
  fi
done

# ---------- capture health ----------
_ns_expected=$(echo "$NAMESPACES" | wc -w)
_pod_expected=$(( SAMPLES_RUN * _ns_expected ))
{
  echo "samples_run=$SAMPLES_RUN"
  echo "empty_top_nodes=$EMPTY_NODES"
  echo "empty_pod_lists=$EMPTY_PODS of $_pod_expected"
  echo "empty_top_pods=$EMPTY_TOP of $_pod_expected"
} > "$OUTDIR/capture_health.txt"
if [ "$EMPTY_NODES" -gt 0 ] || [ "$EMPTY_PODS" -gt 0 ] || [ "$EMPTY_TOP" -gt 0 ]; then
  echo ""
  echo "  ============================================================"
  echo "  [!] CAPTURE HEALTH WARNING — some samples came back empty"
  echo "      'oc adm top nodes' empty : $EMPTY_NODES of $SAMPLES_RUN samples"
  echo "      pod lists empty          : $EMPTY_PODS of $_pod_expected namespace-samples"
  echo "      'oc adm top pods' empty  : $EMPTY_TOP of $_pod_expected namespace-samples"
  echo "      Usual causes: the oc session expired mid-run, or metrics-server"
  echo "      was unavailable. Figures below are based on the samples that"
  echo "      DID return — treat them as a floor, not the full picture."
  echo "  ============================================================"
  echo ""
else
  echo "[✓] capture health: all $SAMPLES_RUN samples returned data"
fi

echo "[*] collection complete, parsing trends..."
SCRIPT_DIR=$(dirname "$0")
python3 "$SCRIPT_DIR/infra_parse.py" "$OUTDIR" 2>&1 | sed 's/^/    /'

cat > "$OUTDIR/manifest.txt" << MANIFEST
kind=infra
timestamp=$TS
interval_s=$INTERVAL
samples=$SAMPLES
namespaces=$NAMESPACES
MANIFEST

echo "[*] packaging..."
ZIPNAME="perf_infra_${TS}.zip"
( cd "$OUTBASE" && zip -qr "$ZIPNAME" "perf_infra_${TS}" )
echo "[✓] DONE  dir=$OUTDIR  zip=$OUTBASE/$ZIPNAME"
