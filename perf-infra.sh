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
OUTBASE="."
SAMPLES=""
NAMESPACES="perf-transaction perf-admin perf-kms perf-merchant perf-simulators perf-notification perf-epay-common perf-frontend"

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
echo "sample,ts,namespace,reason,count" > "$OUTDIR/scheduler_events.csv"
echo "sample,ts,namespace,pod,probe,failures" > "$OUTDIR/probe_failures.csv"
echo "sample,ts,node,cpu_m,cpu_pct,mem_mi,mem_pct" > "$OUTDIR/node_usage.csv"

NSFLAG=""
for ns in $NAMESPACES; do NSFLAG="$NSFLAG -n $ns"; done

i=0
while [ "$i" -lt "$SAMPLES" ]; do
  i=$((i+1))
  cyc_start=$(date +%s)
  now=$(date -u +%FT%TZ)
  sdir="$OUTDIR/samples/$(printf 'snap_%03d' "$i")"
  mkdir -p "$sdir"
  printf "[%s] sample %d/%d ... " "$now" "$i" "$SAMPLES"

  # --- node-level usage (whole cluster), once per sample ---
  oc adm top nodes --no-headers 2>/dev/null > "$sdir/top_nodes.txt"
  while read -r nname ncpu ncpupct nmem nmempct _r; do
    [ -z "$nname" ] && continue
    echo "$i,$now,$nname,$(echo "$ncpu"|sed 's/m//'),$(echo "$ncpupct"|sed 's/%//'),$(echo "$nmem"|sed 's/Mi//'),$(echo "$nmempct"|sed 's/%//')" >> "$OUTDIR/node_usage.csv"
  done < "$sdir/top_nodes.txt"

  for ns in $NAMESPACES; do
    # --- CPU/mem via adm top ---
    oc -n "$ns" adm top pods --no-headers 2>/dev/null > "$sdir/top_${ns}.txt"
    while read -r pod cpu mem _rest; do
      [ -z "$pod" ] && continue
      cpu_m=$(echo "$cpu" | sed 's/m//')
      mem_mi=$(echo "$mem" | sed 's/Mi//')
      echo "$i,$now,$ns,$pod,$cpu_m,$mem_mi" >> "$OUTDIR/pod_usage.csv"
    done < "$sdir/top_${ns}.txt"

    # --- status + restarts ---
    oc -n "$ns" get pods --no-headers 2>/dev/null > "$sdir/pods_${ns}.txt"
    while read -r pod ready status restarts age; do
      [ -z "$pod" ] && continue
      rc=$(echo "$restarts" | grep -oE '^[0-9]+' | head -1)
      echo "$i,$now,$ns,$pod,${rc:-0},$status" >> "$OUTDIR/pod_status.csv"
    done < "$sdir/pods_${ns}.txt"

    # --- scheduler events (this cycle) split by reason ---
    oc -n "$ns" get events --field-selector reason=FailedScheduling --no-headers 2>/dev/null > "$sdir/sched_${ns}.txt"
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
  cyc_end=$(date +%s)
  took=$((cyc_end - cyc_start))
  echo "done (${took}s)"
  if [ "$took" -ge "$INTERVAL" ]; then
    echo "    [!] WARN: cycle took ${took}s >= interval ${INTERVAL}s (no sleep; next starts immediately)"
  else
    [ "$i" -lt "$SAMPLES" ] && sleep $((INTERVAL - took))
  fi
done

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
