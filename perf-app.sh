#!/usr/bin/env sh
# ============================================================================
# perf-app.sh — filtered application-log collector for a 1-hour load test
# SBI ePay 2.0 PERF
# ----------------------------------------------------------------------------
# Read-only. POSIX sh. Requires: oc (logged in), python3, zip.
# Runs alongside perf-infra.sh for the load window. Every 60s it pulls NEW log
# lines (incremental, --since-time) from every pod in the perf namespaces and
# keeps only signal lines: ERROR / WARN / Exception / stack traces / GC pauses /
# 503 / connection-timeout / Hikari pool messages. Does NOT scrape actuator
# (app metrics come from Dynatrace) — logs only.
#
# Usage:
#   ./perf-app.sh                     # 60 samples @ 60s = 1 hour
#   ./perf-app.sh -i 60 -d 3600 -o /path
# ============================================================================
set -u

# clean integer count from a string via grep
scount() { c=$(printf '%s' "$1" | grep -ciE "$2" 2>/dev/null); c=$(printf '%s' "$c" | tr -d ' \n\r'); [ -z "$c" ] && c=0; printf '%s' "$c"; }

INTERVAL=60
DURATION=3600
OUTBASE="."
SAMPLES=""
# App-service namespaces only. istio/kafka/tempo are excluded on purpose: their logs are
# enormous (istiod alone dwarfs every app log) and they are not the services under test.
# Add them with -n if you ever need them.
NAMESPACES="perf-transaction perf-admin perf-kms perf-merchant perf-simulators perf-notification perf-epay-common perf-frontend"
# grep pattern for signal lines (case-insensitive)
PATTERN='ERROR|WARN|Exception|Caused by|\tat |FATAL|OutOfMemory|GC pause|Pause Young|Pause Full|Full GC|503|Service Unavailable|connection is not available|Connection is not available|HikariPool|timeout|Timeout|refused|Unable to acquire|deadlock'

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
OUTDIR="$OUTBASE/perf_app_${TS}"
mkdir -p "$OUTDIR/logs"
echo "[*] perf-app.sh  interval=${INTERVAL}s  samples=${SAMPLES}  (~$((SAMPLES*INTERVAL/60)) min)"
echo "[*] namespaces: $NAMESPACES"
echo "[*] out: $OUTDIR  (filtered logs only; app metrics via Dynatrace)"

# running tally CSV: which pod produced how many signal lines each sample, by severity
echo "sample,ts,namespace,pod,errors,warns,exceptions,gc,conn_pool,http503" > "$OUTDIR/log_signal_counts.csv"

# since-marker: first cycle pulls last 90s; later cycles pull since last cycle time
SINCE="90s"

i=0
while [ "$i" -lt "$SAMPLES" ]; do
  i=$((i+1))
  cyc_start=$(date +%s)
  now=$(date -u +%FT%TZ)
  printf "[%s] sample %d/%d ... " "$now" "$i" "$SAMPLES"

  for ns in $NAMESPACES; do
    pods=$(oc -n "$ns" get pods --no-headers 2>/dev/null | awk '$3=="Running"{print $1}')
    for pod in $pods; do
      # incremental pull, app container auto-detected (skip istio-proxy)
      appc=$(oc -n "$ns" get pod "$pod" -o jsonpath='{.spec.containers[*].name}' 2>/dev/null | tr ' ' '\n' | grep -v istio | head -1)
      raw=$(oc -n "$ns" logs "$pod" ${appc:+-c "$appc"} --since="$SINCE" --tail=2000 2>/dev/null)
      [ -z "$raw" ] && continue
      # filter to signal lines, append to per-pod file
      echo "$raw" | grep -iE "$PATTERN" >> "$OUTDIR/logs/${ns}__${pod}.log" 2>/dev/null

      # count severities this cycle
      chunk=$(echo "$raw" | grep -iE "$PATTERN")
      e=$(scount "$chunk" 'ERROR|FATAL')
      w=$(scount "$chunk" 'WARN')
      x=$(scount "$chunk" 'Exception|Caused by|OutOfMemory')
      g=$(scount "$chunk" 'GC pause|Pause Young|Pause Full|Full GC')
      c=$(scount "$chunk" 'HikariPool|connection is not available|Unable to acquire|deadlock')
      h=$(scount "$chunk" '503|Service Unavailable')
      : "${e:=0}" "${w:=0}" "${x:=0}" "${g:=0}" "${c:=0}" "${h:=0}"
      tot=$((e+w+x+g+c+h))
      [ "$tot" -gt 0 ] && echo "$i,$now,$ns,$pod,$e,$w,$x,$g,$c,$h" >> "$OUTDIR/log_signal_counts.csv"
    done
  done

  cyc_end=$(date +%s); took=$((cyc_end - cyc_start))
  # next cycle pulls since this cycle started (+small overlap)
  SINCE="$((took + 5))s"
  echo "done (${took}s)"
  if [ "$took" -ge "$INTERVAL" ]; then
    echo "    [!] WARN: cycle ${took}s >= interval ${INTERVAL}s"
  else
    [ "$i" -lt "$SAMPLES" ] && sleep $((INTERVAL - took))
  fi
done

echo "[*] parsing log signals..."
SCRIPT_DIR=$(dirname "$0")
python3 "$SCRIPT_DIR/app_parse.py" "$OUTDIR" 2>&1 | sed 's/^/    /'

cat > "$OUTDIR/manifest.txt" << MANIFEST
kind=app
timestamp=$TS
interval_s=$INTERVAL
samples=$SAMPLES
namespaces=$NAMESPACES
note=filtered logs only; 7 app metrics sourced from Dynatrace
MANIFEST

echo "[*] packaging..."
ZIPNAME="perf_app_${TS}.zip"
( cd "$OUTBASE" && zip -qr "$ZIPNAME" "perf_app_${TS}" )
echo "[✓] DONE  dir=$OUTDIR  zip=$OUTBASE/$ZIPNAME"
