#!/usr/bin/env sh
# ============================================================================
# perf-kafka.sh — perf-kafka load monitoring (Strimzi / AMQ Streams, KRaft)
# SBI ePay 2.0 PERF · Saadi M Muzzammil
# ----------------------------------------------------------------------------
#   ./perf-kafka.sh --check                 preflight: discover and test everything
#   ./perf-kafka.sh                         monitor, 15s cycle, Ctrl-C to stop
#   ./perf-kafka.sh 15 k1 -o DIR -d 3600    15s cycle, tag k1, into DIR, 1h then stop
#
# Flags:  -o DIR   output directory (default .)
#         -d SECS  stop by itself after SECS
#         -n NS    namespace (default perf-kafka)
#         -D N     run the expensive in-pod CLI captures every Nth cycle (default 4)
#         --no-cli skip in-pod CLI entirely (Thanos metrics only)
#
# Read-only. Nothing is created, changed or deleted in the cluster.
#
# NOTHING IS HARDCODED. The script discovers the Kafka CR name, the broker and
# controller pods, which JMX metric names your exporter actually publishes, and
# whether the in-pod CLI is usable — then collects only what exists and says so.
# Run --check first: it prints every resolved name so you can see what the
# report will and will not contain.
#
# CSVs written (one row per sample unless stated):
#   kafka_cluster_<tag>.csv   controller quorum, offline/under-replicated, totals
#   kafka_broker_<tag>.csv    per broker: throughput, saturation, queues, ISR
#   kafka_topic_<tag>.csv     per topic: bytes in/out, messages in
#   kafka_latency_<tag>.csv   per request type: p99/p95 total, queue, local time
#   kafka_jvm_<tag>.csv       per pod: heap, GC count and time
#   kafka_pod_<tag>.csv       per pod: cpu, mem, node, phase, restarts, ready
#   kafka_disk_<tag>.csv      per broker: PVC used/capacity, log dir bytes
#   kafka_lag_<tag>.csv       per group/topic: consumer lag        (CLI, every Nth)
#   kafka_health_<tag>.csv    under-replicated / offline from the CLI (every Nth)
# ============================================================================
set -u

CHECK=0
[ "${1:-}" = "--check" ] && { CHECK=1; shift; }

INT=15; TAG=""
case "${1:-}" in ''|-*) ;; *) INT="$1"; shift;; esac
case "${1:-}" in ''|-*) ;; *) TAG="$1"; shift;; esac
OUTDIR="."; DURATION=0; NS="perf-kafka"; DEEP_EVERY=4; USE_CLI=1
while [ $# -gt 0 ]; do
  case "$1" in
    -o) OUTDIR="${2:-.}"; shift 2;;
    -d) DURATION="${2:-0}"; shift 2;;
    -n) NS="${2:-perf-kafka}"; shift 2;;
    -D) DEEP_EVERY="${2:-4}"; shift 2;;
    --no-cli) USE_CLI=0; shift;;
    *)  shift;;
  esac
done
[ -z "$TAG" ] && TAG=$(date +%H%M)
mkdir -p "$OUTDIR" 2>/dev/null

TMP="/tmp/.pk_$$"; mkdir -p "$TMP"
MNAMES="$TMP/metric_names"
QUIET=0; [ -t 1 ] || QUIET=1
trap 'rm -rf "$TMP"' EXIT

die() { [ "$CHECK" -eq 1 ] && return 0; echo "$1"; exit 1; }

command -v oc      >/dev/null 2>&1 || { echo "ERROR: oc not found in PATH"; exit 1; }
command -v curl    >/dev/null 2>&1 || { echo "ERROR: curl not found in PATH"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 not found in PATH"; exit 1; }

TOKEN=$(oc whoami -t 2>/dev/null)
[ -z "$TOKEN" ] && die "ERROR: no oc token (run: oc login)"
THANOS=$(oc -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}' 2>/dev/null)
[ -z "$THANOS" ] && die "ERROR: thanos-querier route not found"

now_ms() {
  _t=$(date +%s%3N 2>/dev/null)
  case "$_t" in ''|*[!0-9]*) echo "$(date +%s)000" ;; *) echo "$_t" ;; esac
}

# ---------------------------------------------------------------- promQL ----
# qj QUERY LABEL[,LABEL...]  ->  tab-separated label values then the number.
# python3 -I: isolated, so nothing is imported from the current directory.
qj() {
  curl -sk --max-time 25 -H "Authorization: Bearer $TOKEN" \
    --data-urlencode "query=$1" "https://$THANOS/api/v1/query" 2>/dev/null \
  | LABELS="${2:-}" python3 -I -c '
import sys, json, os
labs=[l for l in os.environ.get("LABELS","").split(",") if l]
try: d=json.load(sys.stdin)
except Exception: sys.exit(0)
if d.get("status")!="success": sys.exit(0)
for r in d["data"].get("result", []):
    m=r.get("metric", {}); out=[]
    for l in labs:
        v=m.get(l,"")
        if v and l in ("instance","pod","kubernetes_pod_name"): v=v.split(":")[0]
        out.append(v if v else "-")
    try: val=float(r["value"][1])
    except Exception: val=0.0
    if val!=val or val in (float("inf"), float("-inf")): val=0.0
    out.append("%.6g" % val)
    print("\t".join(out))
'
}

# ---- metric-name discovery -------------------------------------------------
# Which names exist depends entirely on your JMX exporter rules, which differ
# between Strimzi versions and between hand-written configs. So we read the
# real list from Thanos and match against it instead of guessing.
load_metric_names() {
  for sel in 'kafka.*' 'jvm.*' 'kubelet_volume_stats.*'; do
    curl -sk --max-time 30 -H "Authorization: Bearer $TOKEN" \
      --data-urlencode "match[]={__name__=~\"$sel\"}" \
      "https://$THANOS/api/v1/label/__name__/values" 2>/dev/null \
    | python3 -I -c '
import sys, json
try: d=json.load(sys.stdin)
except Exception: sys.exit(0)
for n in (d.get("data") or []): print(n)'
  done | sort -u > "$MNAMES"
}

# find_metric PATTERN  -> the best matching metric name, or "" if none.
# Prefers a counter (_total) over a gauge snapshot of the same concept.
find_metric() {
  grep -iE "$1" "$MNAMES" 2>/dev/null | awk '
    { all[NR]=$0; if ($0 ~ /_total$/ && !t) t=$0; if ($0 ~ /_count$/ && !c) c=$0 }
    END { if (t) print t; else if (c) print c; else if (NR) print all[1] }'
}

# rate_or_value NAME WINDOW -> a promQL fragment: rate() for counters, raw for gauges
rate_expr() {
  case "$1" in
    *_total|*_count) echo "rate($1{namespace=\"$NS\"}[${2}])" ;;
    *)               echo "$1{namespace=\"$NS\"}" ;;
  esac
}

# ---------------------------------------------------------------- discovery -
# Kafka CR name(s)
KCLUSTERS=$(oc -n "$NS" get kafka --no-headers \
              -o custom-columns='N:.metadata.name' 2>/dev/null | grep -v '^$' | tr -d ' ')
# Pods. custom-columns, never -o wide: a RESTARTS cell like "1 (51m ago)" splits
# into three fields in -o wide and shifts every later column.
PODCOLS='POD:.metadata.name,PHASE:.status.phase,NODE:.spec.nodeName,RC:.status.containerStatuses[*].restartCount,READY:.status.containerStatuses[*].ready'
kpods() { oc -n "$NS" get pods --no-headers -o custom-columns="$PODCOLS" 2>/dev/null; }

# Broker / controller roles. Strimzi KRaft labels node pools with
# strimzi.io/broker-role and strimzi.io/controller-role; older ZK-based clusters
# name the pods <cluster>-kafka-N and <cluster>-zookeeper-N.
roles() {
  oc -n "$NS" get pods --no-headers -o custom-columns=\
'POD:.metadata.name,B:.metadata.labels.strimzi\.io/broker-role,C:.metadata.labels.strimzi\.io/controller-role,POOL:.metadata.labels.strimzi\.io/pool-name' 2>/dev/null
}
BROKERS=$(roles | awk '$2=="true"{print $1}')
CONTROLLERS=$(roles | awk '$3=="true"{print $1}')
if [ -z "$BROKERS" ]; then
  BROKERS=$(kpods | awk '$1 ~ /-kafka-[0-9]+$/ {print $1}')
fi
if [ -z "$CONTROLLERS" ]; then
  CONTROLLERS=$(kpods | awk '$1 ~ /-(controller|zookeeper)-[0-9]+$/ {print $1}')
fi
NBROKER=$(printf '%s\n' "$BROKERS" | grep -c . )
EXPORTER=$(kpods | awk '$1 ~ /kafka-exporter/ && $2=="Running"{print $1}' | head -1)
OPERATOR=$(oc get pods -A --no-headers \
             -o custom-columns='NS:.metadata.namespace,POD:.metadata.name,PHASE:.status.phase' 2>/dev/null \
           | awk '$2 ~ /strimzi-cluster-operator|amq-streams-cluster-operator/ && $3=="Running"{print $1"/"$2}' | head -1)

[ -z "$BROKERS" ] && die "ERROR: no Kafka broker pods found in namespace $NS"

# ---- in-pod CLI probe -----------------------------------------------------
BPOD=$(printf '%s\n' "$BROKERS" | head -1)
KBIN=""; BOOTSTRAP=""; CLI_OK=0
probe_cli() {
  [ "$USE_CLI" -eq 0 ] && return 1
  for d in /opt/kafka/bin /opt/kafka/kafka_*/bin /usr/bin; do
    if oc -n "$NS" exec "$BPOD" -c kafka -- sh -c "ls $d/kafka-consumer-groups.sh" >/dev/null 2>&1; then
      KBIN="$d"; break
    fi
  done
  [ -z "$KBIN" ] && return 1
  for c in $KCLUSTERS; do
    for b in "$c-kafka-bootstrap:9092" "$c-kafka-bootstrap.$NS.svc:9092"; do
      if oc -n "$NS" exec "$BPOD" -c kafka -- sh -c \
           "$KBIN/kafka-broker-api-versions.sh --bootstrap-server $b" >/dev/null 2>&1; then
        BOOTSTRAP="$b"; CLI_OK=1; return 0
      fi
    done
  done
  # last resort: localhost on the broker itself
  if oc -n "$NS" exec "$BPOD" -c kafka -- sh -c \
       "$KBIN/kafka-broker-api-versions.sh --bootstrap-server localhost:9092" >/dev/null 2>&1; then
    BOOTSTRAP="localhost:9092"; CLI_OK=1; return 0
  fi
  return 1
}

# ---------------------------------------------------------------- resolve ---
load_metric_names
NMET=$(grep -c . "$MNAMES" 2>/dev/null); NMET=${NMET:-0}

M_BYTESIN=$(find_metric 'brokertopicmetrics.*bytesin|bytes_in_per_sec|bytesinpersec')
M_BYTESOUT=$(find_metric 'brokertopicmetrics.*bytesout|bytes_out_per_sec|bytesoutpersec')
M_MSGIN=$(find_metric 'brokertopicmetrics.*messagesin|messages_in_per_sec|messagesinpersec')
M_HANDLER=$(find_metric 'requesthandler.*idle|request_handler.*idle')
M_NETPROC=$(find_metric 'networkprocessor.*idle|network_processor.*idle')
M_URP=$(find_metric 'underreplicatedpartitions|under_replicated_partitions')
M_OFFLINE=$(find_metric 'offlinepartitionscount|offline_partitions_count')
M_ACTIVECTL=$(find_metric 'activecontrollercount|active_controller_count')
M_UNCLEAN=$(find_metric 'uncleanleaderelection')
M_PARTS=$(find_metric 'replicamanager.*partitioncount|partition_count')
M_LEADERS=$(find_metric 'replicamanager.*leadercount|leader_count')
M_ISRSHRINK=$(find_metric 'isrshrink')
M_ISREXPAND=$(find_metric 'isrexpand')
M_REQQ=$(find_metric 'requestqueuesize|request_queue_size')
M_RESPQ=$(find_metric 'responsequeuesize|response_queue_size')
M_PURG=$(find_metric 'purgatorysize|purgatory_size')
M_TOTTIME=$(find_metric 'requestmetrics.*totaltime|total_time_ms')
M_QTIME=$(find_metric 'requestmetrics.*requestqueuetime|request_queue_time_ms')
M_LTIME=$(find_metric 'requestmetrics.*localtime|local_time_ms')
M_REQRATE=$(find_metric 'requestmetrics.*requestspersec|requests_per_sec')
M_FETCHLAG=$(find_metric 'fetcherlag|consumer_lag|replica.*lag')
M_LOGSIZE=$(find_metric 'log_size|logsize')
M_HEAP=$(find_metric '^jvm_memory_bytes_used$|^jvm_memory_used_bytes$')
M_GCCOUNT=$(find_metric '^jvm_gc_collection_seconds_count$|^jvm_gc_pause_seconds_count$')
M_GCTIME=$(find_metric '^jvm_gc_collection_seconds_sum$|^jvm_gc_pause_seconds_sum$')
M_CGLAG=$(find_metric '^kafka_consumergroup_lag$|consumergroup_lag')
M_PVCUSED=$(find_metric '^kubelet_volume_stats_used_bytes$')
M_PVCCAP=$(find_metric '^kubelet_volume_stats_capacity_bytes$')

# which label carries the pod name on the JMX series
POD_LABEL="pod"
if [ -n "$M_BYTESIN" ]; then
  for L in pod kubernetes_pod_name instance; do
    if [ -n "$(qj "$M_BYTESIN{namespace=\"$NS\"}" "$L" | awk -F'\t' '$1!="-"{print;exit}')" ]; then
      POD_LABEL="$L"; break
    fi
  done
fi
# which label carries the topic name
TOPIC_LABEL=""
if [ -n "$M_BYTESIN" ]; then
  for L in topic exported_topic; do
    if [ -n "$(qj "$M_BYTESIN{namespace=\"$NS\"}" "$L" | awk -F'\t' '$1!="-"{print;exit}')" ]; then
      TOPIC_LABEL="$L"; break
    fi
  done
fi

# ---------------------------------------------------------------- preflight -
selfcheck() {
  fail=0
  echo "PERF-KAFKA PREFLIGHT"
  echo "--------------------------------------------------------------------------"
  printf " 1. oc client          : "; v=$(oc version --client 2>&1 | head -1); echo "${v:-<no output>}"
  printf " 2. logged in          : "; [ -n "$TOKEN" ] && echo "yes ($(oc whoami 2>/dev/null))" || { echo "NO"; fail=1; }
  printf " 3. namespace          : "; if oc get ns "$NS" >/dev/null 2>&1; then echo "$NS"; else echo "$NS NOT FOUND"; fail=1; fi
  printf " 4. thanos route       : "; [ -n "$THANOS" ] && echo "$THANOS" || { echo "NOT FOUND"; fail=1; }
  printf " 5. Kafka CR           : "; [ -n "$KCLUSTERS" ] && echo "$(echo $KCLUSTERS | tr '\n' ' ')" || echo "none (pods matched by name instead)"
  printf " 6. broker pods        : "; if [ -n "$BROKERS" ]; then echo "$NBROKER"; printf '%s\n' "$BROKERS" | sed 's/^/      - /'; else echo "NONE"; fail=1; fi
  printf " 7. controller pods    : "
  if [ -n "$CONTROLLERS" ]; then echo "$(printf '%s\n' "$CONTROLLERS" | grep -c .)"; printf '%s\n' "$CONTROLLERS" | sed 's/^/      - /'
  else echo "none found (combined-role nodes, or label missing)"; fi
  printf " 8. kafka-exporter     : "; [ -n "$EXPORTER" ] && echo "$EXPORTER" || echo "not deployed  <- consumer-group lag will come from the CLI instead"
  printf " 9. cluster operator   : "; [ -n "$OPERATOR" ] && echo "$OPERATOR" || echo "not found"
  printf "10. node placement     :\n"
  kpods | awk -v b="$(echo $BROKERS $CONTROLLERS | tr ' ' '|')" '
    $1 ~ "^("b")$" { printf "      %-34s %-12s %s\n", $1, $2, $3 }'

  echo
  printf "11. metric names in Thanos matching kafka/jvm : %s\n" "$NMET"
  if [ "$NMET" -lt 5 ]; then
    echo "      ^ almost nothing found. Your Kafka CR probably has no metricsConfig,"
    echo "        so the JMX Prometheus exporter is not running. Without it this"
    echo "        script can still collect pod CPU/mem, placement, restarts and"
    echo "        (via the CLI) consumer lag and replication health — but not"
    echo "        throughput, saturation or latency. Enable spec.kafka.metricsConfig"
    echo "        on the Kafka CR for the full picture."
    fail=1
  fi
  echo "      resolved series:"
  for pair in "bytes in:$M_BYTESIN" "bytes out:$M_BYTESOUT" "messages in:$M_MSGIN" \
              "handler idle:$M_HANDLER" "network idle:$M_NETPROC" \
              "under-replicated:$M_URP" "offline parts:$M_OFFLINE" \
              "active controller:$M_ACTIVECTL" "unclean elections:$M_UNCLEAN" \
              "partitions:$M_PARTS" "leaders:$M_LEADERS" \
              "ISR shrink:$M_ISRSHRINK" "ISR expand:$M_ISREXPAND" \
              "request queue:$M_REQQ" "response queue:$M_RESPQ" "purgatory:$M_PURG" \
              "total time:$M_TOTTIME" "queue time:$M_QTIME" "local time:$M_LTIME" \
              "request rate:$M_REQRATE" "follower lag:$M_FETCHLAG" "log size:$M_LOGSIZE" \
              "JVM heap:$M_HEAP" "GC count:$M_GCCOUNT" "GC time:$M_GCTIME" \
              "consumer lag:$M_CGLAG" "PVC used:$M_PVCUSED"; do
    k=${pair%%:*}; v=${pair#*:}
    printf "        %-20s %s\n" "$k" "${v:-— not published —}"
  done
  printf "      pod label on JMX series : %s\n" "$POD_LABEL"
  printf "      topic label on JMX series: %s\n" "${TOPIC_LABEL:-— none, per-topic rows unavailable —}"

  echo
  printf "12. live metric read   : "
  if [ -n "$M_BYTESIN" ]; then
    n=$(qj "$(rate_expr "$M_BYTESIN" 2m)" "$POD_LABEL" | grep -c . )
    if [ "$n" -gt 0 ]; then echo "$n broker series returned"; else echo "query returned nothing"; fail=1; fi
  else echo "skipped (no throughput metric)"; fi

  printf "13. oc adm top pods    : "
  n=$(oc -n "$NS" adm top pods --no-headers 2>/dev/null | grep -c . )
  [ "$n" -gt 0 ] && echo "$n pods" || { echo "NO DATA (metrics-server?)"; fail=1; }

  printf "14. in-pod Kafka CLI   : "
  if [ "$USE_CLI" -eq 0 ]; then echo "disabled with --no-cli"
  elif probe_cli; then echo "$KBIN  via $BOOTSTRAP"
  else
    echo "NOT USABLE"
    echo "      tried kafka-consumer-groups.sh under /opt/kafka/bin and a bootstrap"
    echo "      on 9092. If your listeners are TLS/SCRAM only, the CLI needs a"
    echo "      --command-config properties file; consumer lag will be missing"
    echo "      unless kafka-exporter is deployed. Everything else still works."
    echo "      raw error:"
    oc -n "$NS" exec "$BPOD" -c kafka -- sh -c \
      "${KBIN:-/opt/kafka/bin}/kafka-broker-api-versions.sh --bootstrap-server ${BOOTSTRAP:-localhost:9092}" 2>&1 \
      | head -3 | sed 's/^/        /'
  fi
  echo "--------------------------------------------------------------------------"
  [ "$fail" -eq 0 ] && echo "READY — safe to run." || echo "FIX THE FAILING STEPS ABOVE (partial collection still works)."
  exit "$fail"
}
[ "$CHECK" -eq 1 ] && selfcheck

# Which concepts resolved. Passed to the renderer so a missing metric renders as
# "not published" instead of a reassuring zero — a fake "0 under-replicated" in a
# Kafka report is worse than an honest blank.
HAVE=""
for pair in "bytesin:$M_BYTESIN" "handler:$M_HANDLER" "netproc:$M_NETPROC" \
            "urp:$M_URP" "parts:$M_PARTS" "leaders:$M_LEADERS" \
            "reqq:$M_REQQ" "respq:$M_RESPQ" "purg:$M_PURG" \
            "isr:$M_ISRSHRINK" "fetchlag:$M_FETCHLAG" "heap:$M_HEAP" "gc:$M_GCCOUNT"; do
  [ -n "${pair#*:}" ] && HAVE="$HAVE ${pair%%:*}"
done
HAVE="${HAVE# }"

probe_cli || true

# ---------------------------------------------------------------- CSV setup -
C_CLU="$OUTDIR/kafka_cluster_${TAG}.csv"
C_BRK="$OUTDIR/kafka_broker_${TAG}.csv"
C_TOP="$OUTDIR/kafka_topic_${TAG}.csv"
C_LAT="$OUTDIR/kafka_latency_${TAG}.csv"
C_JVM="$OUTDIR/kafka_jvm_${TAG}.csv"
C_POD="$OUTDIR/kafka_pod_${TAG}.csv"
C_DSK="$OUTDIR/kafka_disk_${TAG}.csv"
C_LAG="$OUTDIR/kafka_lag_${TAG}.csv"
C_HLT="$OUTDIR/kafka_health_${TAG}.csv"

echo "ts_ist,elapsed_s,active_controllers,offline_partitions,under_replicated,unclean_elections,partitions_total,leaders_total,bytes_in_mb_s,bytes_out_mb_s,messages_in_s" > "$C_CLU"
echo "ts_ist,elapsed_s,pod,bytes_in_mb_s,bytes_out_mb_s,messages_in_s,handler_idle_pct,network_idle_pct,request_queue,response_queue,purgatory,under_replicated,partitions,leaders,isr_shrink_s,isr_expand_s,follower_lag" > "$C_BRK"
echo "ts_ist,elapsed_s,topic,bytes_in_mb_s,bytes_out_mb_s,messages_in_s" > "$C_TOP"
echo "ts_ist,elapsed_s,request,total_ms,queue_ms,local_ms,req_per_s" > "$C_LAT"
echo "ts_ist,elapsed_s,pod,heap_used_mb,gc_count_s,gc_time_ms_s" > "$C_JVM"
echo "ts_ist,elapsed_s,pod,role,node,phase,ready,restarts,cpu_m,mem_mi" > "$C_POD"
echo "ts_ist,elapsed_s,pod,pvc,used_gb,capacity_gb,used_pct" > "$C_DSK"
echo "ts_ist,elapsed_s,group,topic,lag,state,members" > "$C_LAG"
echo "ts_ist,elapsed_s,check,value,detail" > "$C_HLT"

role_of() {
  printf '%s\n' "$BROKERS"     | grep -qx "$1" && { echo broker; return; }
  printf '%s\n' "$CONTROLLERS" | grep -qx "$1" && { echo controller; return; }
  case "$1" in *kafka-exporter*) echo exporter;; *entity-operator*) echo entity-op;; *) echo other;; esac
}

T0=$(date +%s); T0_MS=$(now_ms)
DEADLINE=0
[ "$DURATION" -gt 0 ] 2>/dev/null && DEADLINE=$(( T0 + DURATION ))
START_IST=$(TZ=Asia/Kolkata date '+%Y-%m-%d %H:%M:%S')
PEAK_IN=0; PEAK_OUT=0; PEAK_MSG=0; MIN_IDLE=100; MAX_URP=0; MAX_LAG=0; SAMPLES=0

finish() {
  END_IST=$(TZ=Asia/Kolkata date '+%Y-%m-%d %H:%M:%S')
  EL=$(( $(date +%s) - T0 ))
  echo
  echo "=================================================================================="
  echo "   PERF-KAFKA WINDOW SUMMARY   (tag: $TAG, namespace: $NS)"
  echo "=================================================================================="
  printf "   START / END (IST) : %s  ->  %s\n" "$START_IST" "$END_IST"
  printf "   DURATION          : %ss (%s min) over %s samples\n" \
     "$EL" "$(awk -v e=$EL 'BEGIN{printf "%.1f", e/60}')" "$SAMPLES"
  printf "   BROKERS           : %s\n" "$NBROKER"
  printf "   PEAK THROUGHPUT   : in %s MB/s   out %s MB/s   %s msg/s\n" "$PEAK_IN" "$PEAK_OUT" "$PEAK_MSG"
  printf "   WORST HANDLER IDLE: %s%%   (under 30%% = brokers saturated)\n" "$MIN_IDLE"
  printf "   MAX UNDER-REPL    : %s   (must return to 0)\n" "$MAX_URP"
  printf "   MAX CONSUMER LAG  : %s\n" "$MAX_LAG"
  echo "=================================================================================="
  printf "   CSVs in %s  (tag %s)\n" "$OUTDIR" "$TAG"
  rm -rf "$TMP"
  exit 0
}
trap finish INT

echo "perf-kafka monitor — ns=$NS brokers=$NBROKER cycle=${INT}s tag=$TAG"
[ "$NMET" -lt 5 ] && echo "  [!] few/no JMX metrics in Thanos — run --check; collection will be partial"
[ "$CLI_OK" -eq 0 ] && [ "$USE_CLI" -eq 1 ] && echo "  [!] in-pod Kafka CLI unusable — consumer lag/health from CLI will be empty"

i=0
while :; do
  sleep "$INT"
  NOW=$(date +%s)
  if [ "$DEADLINE" -gt 0 ] && [ "$NOW" -ge "$DEADLINE" ]; then
    echo "[*] duration limit reached (${DURATION}s) — closing the window on schedule."
    finish
  fi
  i=$((i+1)); SAMPLES=$i
  W="${INT}s"; [ "$INT" -lt 60 ] && W="2m"   # rate() needs >= 2 scrapes

  # ---------- Thanos: broker throughput + saturation ----------
  : > "$TMP/brk"
  if [ -n "$M_BYTESIN" ]; then
    qj "sum by($POD_LABEL) ($(rate_expr "$M_BYTESIN" "$W"))"  "$POD_LABEL" > "$TMP/in"  || :
    qj "sum by($POD_LABEL) ($(rate_expr "$M_BYTESOUT" "$W"))" "$POD_LABEL" > "$TMP/out" || :
    qj "sum by($POD_LABEL) ($(rate_expr "$M_MSGIN" "$W"))"    "$POD_LABEL" > "$TMP/msg" || :
  else : > "$TMP/in"; : > "$TMP/out"; : > "$TMP/msg"; fi
  for m in HANDLER NETPROC REQQ RESPQ PURG URP PARTS LEADERS FETCHLAG; do
    eval "nm=\$M_$m"
    f="$TMP/$(echo $m | tr 'A-Z' 'a-z')"
    if [ -n "$nm" ]; then qj "sum by($POD_LABEL) ($nm{namespace=\"$NS\"})" "$POD_LABEL" > "$f" || :; else : > "$f"; fi
  done
  for m in ISRSHRINK ISREXPAND; do
    eval "nm=\$M_$m"
    f="$TMP/$(echo $m | tr 'A-Z' 'a-z')"
    if [ -n "$nm" ]; then qj "sum by($POD_LABEL) ($(rate_expr "$nm" "$W"))" "$POD_LABEL" > "$f" || :; else : > "$f"; fi
  done

  # ---------- Thanos: cluster scalars ----------
  sc() { [ -n "$1" ] && qj "sum($1{namespace=\"$NS\"})" "" | awk 'NR==1{print $1+0}' || echo ""; }
  ACTCTL=$(sc "$M_ACTIVECTL"); OFFP=$(sc "$M_OFFLINE"); UNCL=$(sc "$M_UNCLEAN")

  # ---------- Thanos: per-topic ----------
  : > "$TMP/topic"
  if [ -n "$M_BYTESIN" ] && [ -n "$TOPIC_LABEL" ]; then
    qj "sum by($TOPIC_LABEL) ($(rate_expr "$M_BYTESIN" "$W"))"  "$TOPIC_LABEL" > "$TMP/t_in"  || :
    qj "sum by($TOPIC_LABEL) ($(rate_expr "$M_BYTESOUT" "$W"))" "$TOPIC_LABEL" > "$TMP/t_out" || :
    qj "sum by($TOPIC_LABEL) ($(rate_expr "$M_MSGIN" "$W"))"    "$TOPIC_LABEL" > "$TMP/t_msg" || :
  else : > "$TMP/t_in"; : > "$TMP/t_out"; : > "$TMP/t_msg"; fi

  # ---------- Thanos: latency by request type ----------
  : > "$TMP/lat"
  if [ -n "$M_TOTTIME" ]; then
    qj "max by(request) ($M_TOTTIME{namespace=\"$NS\",quantile=\"0.99\"})" "request" > "$TMP/l_tot" 2>/dev/null \
      || qj "max by(request) ($M_TOTTIME{namespace=\"$NS\"})" "request" > "$TMP/l_tot" || :
    [ -n "$M_QTIME" ] && qj "max by(request) ($M_QTIME{namespace=\"$NS\",quantile=\"0.99\"})" "request" > "$TMP/l_q" 2>/dev/null || : > "$TMP/l_q"
    [ -n "$M_LTIME" ] && qj "max by(request) ($M_LTIME{namespace=\"$NS\",quantile=\"0.99\"})" "request" > "$TMP/l_l" 2>/dev/null || : > "$TMP/l_l"
    [ -n "$M_REQRATE" ] && qj "sum by(request) ($(rate_expr "$M_REQRATE" "$W"))" "request" > "$TMP/l_r" || : > "$TMP/l_r"
  else : > "$TMP/l_tot"; : > "$TMP/l_q"; : > "$TMP/l_l"; : > "$TMP/l_r"; fi

  # ---------- Thanos: JVM ----------
  if [ -n "$M_HEAP" ]; then
    qj "sum by($POD_LABEL) ($M_HEAP{namespace=\"$NS\",area=\"heap\"})" "$POD_LABEL" > "$TMP/heap" 2>/dev/null \
      || qj "sum by($POD_LABEL) ($M_HEAP{namespace=\"$NS\"})" "$POD_LABEL" > "$TMP/heap" || :
  else : > "$TMP/heap"; fi
  [ -n "$M_GCCOUNT" ] && qj "sum by($POD_LABEL) ($(rate_expr "$M_GCCOUNT" "$W"))" "$POD_LABEL" > "$TMP/gcc" || : > "$TMP/gcc"
  [ -n "$M_GCTIME" ]  && qj "sum by($POD_LABEL) ($(rate_expr "$M_GCTIME"  "$W"))" "$POD_LABEL" > "$TMP/gct" || : > "$TMP/gct"

  # ---------- Thanos: PVC ----------
  if [ -n "$M_PVCUSED" ]; then
    qj "$M_PVCUSED{namespace=\"$NS\"}" "persistentvolumeclaim" > "$TMP/pvcu" || :
    qj "$M_PVCCAP{namespace=\"$NS\"}"  "persistentvolumeclaim" > "$TMP/pvcc" || :
  else : > "$TMP/pvcu"; : > "$TMP/pvcc"; fi

  # ---------- Kubernetes: pods ----------
  kpods > "$TMP/pods" 2>/dev/null || : > "$TMP/pods"
  oc -n "$NS" adm top pods --no-headers > "$TMP/top" 2>/dev/null || : > "$TMP/top"

  READ_MS=$(now_ms); EL=$(( (READ_MS - T0_MS + 500) / 1000 ))
  TS=$(TZ=Asia/Kolkata date '+%Y-%m-%d %H:%M:%S')

  # ---------- render + write ----------
  OUT=$(TS="$TS" EL="$EL" TMPD="$TMP" NS="$NS" HAVE="$HAVE" \
        BROKERS="$BROKERS" CONTROLLERS="$CONTROLLERS" \
        ACTCTL="$ACTCTL" OFFP="$OFFP" UNCL="$UNCL" \
        C_CLU="$C_CLU" C_BRK="$C_BRK" C_TOP="$C_TOP" C_LAT="$C_LAT" \
        C_JVM="$C_JVM" C_POD="$C_POD" C_DSK="$C_DSK" \
        python3 -I "$(dirname "$0")/kafka_sample.py" 2>/dev/null)
  [ -z "$OUT" ] && OUT="  (renderer produced no output — is kafka_sample.py beside perf-kafka.sh?)"

  # keep running peaks for the closing summary (the renderer reports this sample
  # only; the shell is what remembers the window)
  CUR_IN=""; CUR_OUT=""; CUR_MSG=""; CUR_IDLE=""; CUR_URP=""
  eval "$(printf '%s\n' "$OUT" | sed -n 's/^##STAT //p')"
  keep_max() { awk -v a="${1:-}" -v b="${2:-}" 'BEGIN{ if(b=="")print a; else if(a=="")print b; else print (b+0>a+0)?b:a }'; }
  keep_min() { awk -v a="${1:-}" -v b="${2:-}" 'BEGIN{ if(b=="")print a; else if(a=="")print b; else print (b+0<a+0)?b:a }'; }
  PEAK_IN=$(keep_max  "$PEAK_IN"  "$CUR_IN")
  PEAK_OUT=$(keep_max "$PEAK_OUT" "$CUR_OUT")
  PEAK_MSG=$(keep_max "$PEAK_MSG" "$CUR_MSG")
  MAX_URP=$(keep_max  "$MAX_URP"  "$CUR_URP")
  MIN_IDLE=$(keep_min "$MIN_IDLE" "$CUR_IDLE")

  # ---------- expensive CLI captures, every Nth cycle ----------
  LAGLINE=""
  if [ "$CLI_OK" -eq 1 ] && [ $(( (i - 1) % DEEP_EVERY )) -eq 0 ]; then
    oc -n "$NS" exec "$BPOD" -c kafka -- sh -c \
      "$KBIN/kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --all-groups 2>/dev/null" \
      > "$TMP/cg" 2>/dev/null || : > "$TMP/cg"
    # GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
    awk -v ts="$TS" -v el="$EL" '
      $1=="GROUP" || NF<6 { next }
      { lag=$6+0; if (lag<0) lag=0; k=$1 SUBSEP $2; agg[k]+=lag; mem[k]++ }
      END { for (k in agg) { split(k,a,SUBSEP); printf "%s,%s,%s,%s,%d,,%d\n", ts, el, a[1], a[2], agg[k], mem[k] } }
    ' "$TMP/cg" >> "$C_LAG"
    TOTLAG=$(awk -F, -v t="$TS" '$1==t{s+=$5} END{print s+0}' "$C_LAG")
    awk -v m="$MAX_LAG" -v n="$TOTLAG" 'BEGIN{exit !(n>m)}' && MAX_LAG="$TOTLAG"
    LAGLINE="   consumer lag (all groups): $TOTLAG"

    oc -n "$NS" exec "$BPOD" -c kafka -- sh -c \
      "$KBIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --under-replicated-partitions 2>/dev/null" \
      > "$TMP/urp" 2>/dev/null || : > "$TMP/urp"
    nurp=$(grep -c 'Topic:' "$TMP/urp" 2>/dev/null); nurp=${nurp:-0}
    echo "$TS,$EL,under_replicated_partitions,$nurp,$(head -1 "$TMP/urp" | tr ',' ';' | cut -c1-120)" >> "$C_HLT"
    oc -n "$NS" exec "$BPOD" -c kafka -- sh -c \
      "$KBIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --unavailable-partitions 2>/dev/null" \
      > "$TMP/unav" 2>/dev/null || : > "$TMP/unav"
    nun=$(grep -c 'Topic:' "$TMP/unav" 2>/dev/null); nun=${nun:-0}
    echo "$TS,$EL,unavailable_partitions,$nun," >> "$C_HLT"
  fi

  if [ "$QUIET" -eq 1 ]; then
    printf "[kafka] %s  elapsed %5ss  in %s MB/s  out %s MB/s  hnd-idle %s%%  urp %s\n" \
      "$TS" "$EL" "${CUR_IN:-?}" "${CUR_OUT:-?}" "${CUR_IDLE:-?}" "${CUR_URP:-?}"
    continue
  fi
  clear
  echo "=================================================================================="
  echo "   PERF-KAFKA — $TS IST    elapsed ${EL}s    cycle ${INT}s    ns=$NS    tag=$TAG"
  echo "   window start $START_IST IST    sample $i"
  echo "=================================================================================="
  printf '%s\n' "$OUT" | grep -v '^##STAT '
  [ -n "$LAGLINE" ] && { echo; echo "$LAGLINE"; }
  echo "=================================================================================="
  printf "   Ctrl-C for the window summary   |   CSVs: %s/kafka_*_%s.csv\n" "$OUTDIR" "$TAG"
done
