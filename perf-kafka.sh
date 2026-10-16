#!/usr/bin/env sh
# ============================================================================
# perf-kafka.sh — perf-kafka load monitoring (Strimzi / AMQ Streams, KRaft)
# SBI ePay 2.0 PERF · Saadi M Muzzammil
# ----------------------------------------------------------------------------
#   ./perf-kafka.sh --check                 preflight: discover and test everything
#   ./perf-kafka.sh                         asks interval + duration, then runs
#   ./perf-kafka.sh -i 15 -d 3600           no prompts: 15s cycle, 1 hour
#
# Each run creates its own folder  ./kafka-test-<YYYYmmdd_HHMMSS>/  holding the
# CSVs and the HTML report, which is built automatically when the window closes
# (on the duration limit or on Ctrl-C).
#
# Flags:  -o DIR   output directory (default .)
#         -d SECS  stop by itself after SECS
#         -n NS    namespace (default perf-kafka)
#         -i SECS  sampling interval, 10 .. 300 (prompted when not given)
#         -D N     run the expensive in-pod CLI captures every Nth cycle (default 4)
#         --no-report  write the CSVs but do not build the HTML
#         -T HOST  thanos-querier host, if your user cannot list the route
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

# Bump on every change. Printed at startup, by --version and by --check, and
# written into each run folder, so there is never any doubt about which build
# produced a given set of CSVs.
PK_VERSION="2026-10-16.1 (per-topic produce/fetch outcomes, summary CSV)"

CHECK=0
case "${1:-}" in
  --version|-V) echo "perf-kafka.sh $PK_VERSION"; exit 0;;
esac
[ "${1:-}" = "--check" ] && { CHECK=1; shift; }

INT=15; TAG=""; GOT_INT=0; GOT_DUR=0; GOT_OUT=0
case "${1:-}" in ''|-*) ;; *) INT="$1"; GOT_INT=1; shift;; esac
case "${1:-}" in ''|-*) ;; *) TAG="$1"; shift;; esac
OUTDIR=""; DURATION=0; NS="perf-kafka"; DEEP_EVERY=4; USE_CLI=1; THANOS=""; NOREPORT=0
DUMP=0; DUMPPAT=""
MAXDUR=86400
while [ $# -gt 0 ]; do
  case "$1" in
    -o) OUTDIR="${2:-.}"; GOT_OUT=1; shift 2;;
    -d) DURATION="${2:-0}"; GOT_DUR=1; shift 2;;
    -i) INT="${2:-15}"; GOT_INT=1; shift 2;;
    -n) NS="${2:-perf-kafka}"; shift 2;;
    -D) DEEP_EVERY="${2:-4}"; shift 2;;
    -T) THANOS="${2:-}"; shift 2;;
    --metrics) DUMP=1; DUMPPAT="${2:-}"; case "$DUMPPAT" in -*|'') DUMPPAT=""; shift;; *) shift 2;; esac;;
    --no-cli) USE_CLI=0; shift;;
    --no-report) NOREPORT=1; shift;;
    *)  shift;;
  esac
done

fmt_dur() { awk -v d="$1" 'BEGIN{ if (d<3600) printf "%.0f min", d/60; else printf "%.1f hours", d/3600 }'; }
ask_yn() {
  while :; do
    printf "      %s [Y/n]: " "$1" >&2
    _y=""; read -r _y || _y=""
    case "$_y" in
      ''|y|Y|yes|Yes|YES) return 0;;
      n|N|no|No|NO)       return 1;;
      *) printf "      please answer Y or N.\n" >&2;;
    esac
  done
}
ask_int() {
  _lbl="$1"; _min="$2"; _max="$3"; _def="$4"
  while :; do
    printf "   %s [%s]: " "$_lbl" "$_def" >&2
    _a=""; read -r _a || _a=""
    [ -z "$_a" ] && _a="$_def"
    if echo "$_a" | grep -qE '^[0-9]+$' && [ "$_a" -ge "$_min" ] && [ "$_a" -le "$_max" ]; then
      echo "$_a"; return 0
    fi
    printf "      not valid — enter a whole number between %s and %s.\n" "$_min" "$_max" >&2
  done
}
chk() {
  if ! echo "$2" | grep -qE '^[0-9]+$' || [ "$2" -lt "$3" ] || [ "$2" -gt "$4" ]; then
    echo "ERROR: $1 must be a whole number between $3 and $4 (got '$2')"; exit 1
  fi
}
[ "$GOT_INT" -eq 1 ] && chk -i "$INT" 10 300
[ "$GOT_DUR" -eq 1 ] && chk -d "$DURATION" 60 "$MAXDUR"

# Ask for whatever was not supplied, when there is a terminal to ask on. A flag
# skips its prompt, so a scripted or scheduled run never blocks.
if [ "$CHECK" -eq 0 ] && [ "$DUMP" -eq 0 ] && [ -t 0 ]; then
  echo
  echo "   --------------------------------------------------------------"
  echo "   KAFKA CAPTURE SETTINGS"
  echo "   --------------------------------------------------------------"
  [ "$GOT_INT" -eq 0 ] && INT=$(ask_int \
    "1. Sampling interval, seconds (10-300, recommended 15)" 10 300 15)
  if [ "$GOT_DUR" -eq 0 ]; then
    while :; do
      DURATION=$(ask_int \
        "2. How long to run, seconds (60-86400 = up to 24h; 300=5m, 1800=30m, 3600=1h)" \
        60 "$MAXDUR" 3600)
      [ "$DURATION" -le 3600 ] && break
      printf "      that is %s — longer than an hour.\n" "$(fmt_dur "$DURATION")" >&2
      ask_yn "Run the capture for that long?" && break
    done
  fi
  echo "   --------------------------------------------------------------"
elif [ "$CHECK" -eq 0 ] && [ "$DUMP" -eq 0 ] && { [ "$GOT_INT" -eq 0 ] || [ "$GOT_DUR" -eq 0 ]; }; then
  [ "$DURATION" -eq 0 ] && DURATION=3600
  echo "[*] no terminal to prompt on — using defaults (-i $INT, -d $DURATION)"
fi
[ "$DURATION" -eq 0 ] && DURATION=3600

[ -z "$TAG" ] && TAG=$(date +%H%M)
# Every run gets its own timestamped folder, so repeated runs never overwrite
# each other and the CSVs always sit next to the report built from them.
RUNDIR=""
if [ "$GOT_OUT" -eq 0 ]; then
  RUNDIR="$(pwd)/kafka-test-$(date +%Y%m%d_%H%M%S)"
  OUTDIR="$RUNDIR"
fi
mkdir -p "$OUTDIR" 2>/dev/null || { echo "ERROR: cannot create $OUTDIR"; exit 1; }

SCRIPT_DIR=$(cd "$(dirname "$0")" 2>/dev/null && pwd || echo .)
TMP="/tmp/.pk_$$"; mkdir -p "$TMP"
MNAMES="$TMP/metric_names"
QUIET=0; [ -t 1 ] || QUIET=1
trap 'rm -rf "$TMP"' EXIT

die() { [ "$CHECK" -eq 1 ] && return 0; echo "$1"; exit 1; }

command -v oc      >/dev/null 2>&1 || { echo "ERROR: oc not found in PATH"; exit 1; }
command -v curl    >/dev/null 2>&1 || { echo "ERROR: curl not found in PATH"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 not found in PATH"; exit 1; }

TOKEN=$(oc whoami -t 2>/dev/null)
[ -z "$TOKEN" ] && die "ERROR: no oc token in the kubeconfig — run: oc login"

# `oc whoami -t` only READS the kubeconfig; it never calls the API, so an expired
# session still hands back a token string. Ask the API who we are — that is the
# call that actually proves the session, and it stops a dead token from being
# reported later as some unrelated "not found".
WHOERR="$TMP/whoerr"
WHO=$(oc whoami 2>"$WHOERR")
if [ -z "$WHO" ]; then
  die "ERROR: the oc session is no longer valid — run: oc login
         oc said: $(head -2 "$WHOERR" 2>/dev/null | tr '\n' ' ')"
fi

if [ -z "$THANOS" ]; then
  TERR="$TMP/throute"
  THANOS=$(oc -n openshift-monitoring get route thanos-querier \
             -o jsonpath='{.spec.host}' 2>"$TERR")
  if [ -z "$THANOS" ]; then
    die "ERROR: could not read the thanos-querier route in openshift-monitoring (as $WHO)
         oc said: $(head -2 "$TERR" 2>/dev/null | tr '\n' ' ')
         If your user cannot list routes there but you know the host, pass it:
           ./perf-kafka.sh -T thanos-querier-openshift-monitoring.apps.preprod.epay.sbi
         Then run ./perf-kafka.sh --check for the full picture."
  fi
fi

fmt_hms() {   # seconds -> 4m12s / 1h07m
  awk -v s="$1" 'BEGIN{ s=int(s); if (s<0) s=0;
    if (s<3600) printf "%dm%02ds", s/60, s%60; else printf "%dh%02dm", s/3600, (s%3600)/60 }'
}

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
    # %.6g gives six significant figures. On a cumulative counter around
    # 1.8e15 nanoseconds that is one-second resolution, so a 20s sample
    # quantises to ~5% error — enough to push a derived idle percentage past
    # 100%. The series names and values here are machine-read, not displayed,
    # so there is no reason to round at all.
    out.append(repr(val) if val != int(val) else "%d" % int(val))
    print("\t".join(out))
'
}

# ---- metric-name discovery -------------------------------------------------
# Which names exist depends entirely on your JMX exporter rules, which differ
# between Strimzi versions and between hand-written configs. So we read the
# real list from Thanos and match against it instead of guessing.
# The exporter on the broker is the authority on which series exist: it lists
# every name in its own `# TYPE` lines. Thanos' label API and a probe of guessed
# names are both incomplete next to that, which is why handler-idle and the queue
# depths came back blank on a cluster publishing 7,000+ kafka metrics.
# Read the exporter on a BROKER *and* on a CONTROLLER.
#
# This is why active-controller count, offline partitions and unclean elections
# all read "not published": they are KafkaController MBeans, and in KRaft with
# separate roles those live only on the controller JVMs. Scraping one broker's
# /metrics meant those names never entered the catalogue, so find_metric
# returned empty and the collector reported them as absent — when in fact it had
# never looked anywhere they could exist. A kafka-exporter pod, if deployed,
# adds the consumer-group lag series for the same reason.
exporter_names() {
  for _pod in ${EXP_PODS:-}; do
    [ -z "$_pod" ] && continue
    for _c in kafka ""; do
      _cargs=""; [ -n "$_c" ] && _cargs="-c $_c"
      for port in 9404 9308; do
        # shellcheck disable=SC2086
        oc -n "$NS" exec "$_pod" $_cargs -- \
          sh -c "curl -s --max-time 10 localhost:$port/metrics" 2>/dev/null \
        | sed -n 's/^# TYPE \([a-zA-Z_:][a-zA-Z0-9_:]*\) .*/\1/p'
      done
    done
  done
}

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

# The label-values API can come back empty even when the series exist: on OpenShift
# user-workload metrics it depends on how the token is scoped. So if the catalogue
# looks empty, ask for a shortlist of well-known names DIRECTLY and keep whichever
# actually return data. Discovery by query, not only by catalogue.
KNOWN="kafka_server_brokertopicmetrics_bytesin_total
kafka_server_brokertopicmetrics_bytesout_total
kafka_server_brokertopicmetrics_messagesin_total
kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent
kafka_network_socketserver_networkprocessoravgidlepercent
kafka_server_replicamanager_underreplicatedpartitions
kafka_server_replicamanager_partitioncount
kafka_server_replicamanager_leadercount
kafka_server_replicamanager_isrshrinks_total
kafka_server_replicamanager_isrexpands_total
kafka_controller_kafkacontroller_offlinepartitionscount
kafka_controller_kafkacontroller_activecontrollercount
kafka_controller_controllerstats_uncleanleaderelections_total
kafka_network_requestmetrics_requestqueuesize
kafka_network_requestmetrics_responsequeuesize
kafka_network_requestmetrics_totaltimems
kafka_network_requestmetrics_requestqueuetimems
kafka_network_requestmetrics_localtimems
kafka_network_requestmetrics_requestspersec
kafka_server_delayedoperationpurgatory_purgatorysize
kafka_server_fetcherlagmetrics_consumerlag
kafka_consumergroup_lag
jvm_memory_bytes_used
jvm_gc_collection_seconds_count
jvm_gc_collection_seconds_sum
kubelet_volume_stats_used_bytes
kubelet_volume_stats_capacity_bytes"

probe_known() {
  for c in $KNOWN; do
    grep -qx "$c" "$MNAMES" 2>/dev/null && continue
    case "$c" in
      kubelet_*) _n=$(qj "$c{namespace=\"$NS\"}" "" | grep -c . ) ;;
      *)         _n=$(qj "$c{namespace=\"$NS\"}" "" | grep -c . ) ;;
    esac
    [ "${_n:-0}" -gt 0 ] && echo "$c"
  done
}

# find_metric PATTERN [KIND]  -> the best matching metric name, or "".
#
# KIND matters. The exporter publishes several name variants for the same JMX
# attribute: a plain gauge AND a _total/_count/_sum aggregate. Picking the
# aggregate for a gauge concept yields an ever-growing cumulative number, which
# is how "handler idle" came out as 1971795720000000000% and request latency as
# 671,338,000 ms. So:
#   counter -> prefer _total/_count  (bytes, messages, elections: rates)
#   gauge   -> prefer the PLAIN name (idle %, queue depth, partition count,
#              purgatory, latency percentiles: instantaneous values)
find_metric() {
  grep -iE "$1" "$MNAMES" 2>/dev/null | awk -v kind="${2:-gauge}" '
    { n=$0
      agg = (n ~ /_(total|count|sum)$/)
      if (agg) { if (!c) c=n } else { if (!g) g=n }
      if (!f) f=n }
    END {
      if (kind=="counter") { print (c!="" ? c : (g!="" ? g : f)) }
      else                 { print (g!="" ? g : (c!="" ? c : f)) } }'
}

# rate_or_value NAME WINDOW -> a promQL fragment: rate() for counters, raw for gauges
rate_expr() {
  # $3, when given, is an extra label matcher such as 'topic=""'
  _sel="namespace=\"$NS\""
  [ -n "${3:-}" ] && _sel="$_sel,$3"
  case "$1" in
    *_total|*_count) echo "rate($1{$_sel}[${2}])" ;;
    *)               echo "$1{$_sel}" ;;
  esac
}

# Why the topic matcher matters — this cost a factor of two on every throughput
# figure the toolkit produced.
#
# BrokerTopicMetrics publishes an all-topics aggregate MBean AND one MBean per
# topic. The stock Strimzi exporter rules map all of them to the SAME Prometheus
# name (kafka_server_brokertopicmetrics_bytesin_total): one series with no topic
# label, and one per topic. So
#
#     sum by(pod) (rate(kafka_server_brokertopicmetrics_bytesin_total[2m]))
#
# adds the aggregate to the sum of its own parts and returns exactly 2x the real
# rate. Measured on a live capture: cluster/topic ratio 2.004, 2.001, 2.000.
#
# In PromQL a matcher of label="" selects series where that label is ABSENT, so
# {topic=""} is the broker aggregate alone and {topic!=""} the per-topic series.
# Some exporter configs publish only per-topic series; AGG_SEL is resolved once
# against the live data so either shape is handled.
AGG_SEL='topic=""'
agg_probe() {
  [ -z "$M_BYTESIN" ] && return 0
  if [ -z "$(qj "$M_BYTESIN{namespace=\"$NS\",topic=\"\"}" "" | head -1)" ]; then
    # no aggregate series — summing the per-topic ones is then correct, not double
    AGG_SEL='topic!=""'
  fi
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
# The column spec lives in a variable on ONE line. It used to be a backslash
# line-continuation followed by a quoted string, which is the first thing a
# terminal paste mangles: lose the backslash and `oc` is handed an empty
# -o custom-columns=, returns nothing, and the script reports "no broker pods
# found" on a cluster that plainly has them.
ROLECOLS='POD:.metadata.name,B:.metadata.labels.strimzi\.io/broker-role,C:.metadata.labels.strimzi\.io/controller-role,POOL:.metadata.labels.strimzi\.io/pool-name'
roles() { oc -n "$NS" get pods --no-headers -o custom-columns="$ROLECOLS" 2>/dev/null; }

BROKERS=$(roles | awk '$2=="true"{print $1}')
CONTROLLERS=$(roles | awk '$3=="true"{print $1}')
# Name-based fallback for when the labels are absent or the label query fails.
# `-kafka-N` alone only covers ZooKeeper-era naming; a KRaft node pool produces
# <cluster>-broker-N, so on this very cluster the fallback could never match.
if [ -z "$BROKERS" ]; then
  BROKERS=$(kpods | awk '$1 ~ /-(kafka|broker)-[0-9]+$/ {print $1}')
fi
if [ -z "$CONTROLLERS" ]; then
  CONTROLLERS=$(kpods | awk '$1 ~ /-(controller|zookeeper)-[0-9]+$/ {print $1}')
fi
NBROKER=$(printf '%s\n' "$BROKERS" | grep -c . )
EXPORTER=$(kpods | awk '$1 ~ /kafka-exporter/ && $2=="Running"{print $1}' | head -1)
OPERATOR=$(oc get pods -A --no-headers \
             -o custom-columns='NS:.metadata.namespace,POD:.metadata.name,PHASE:.status.phase' 2>/dev/null \
           | awk '$2 ~ /strimzi-cluster-operator|amq-streams-cluster-operator/ && $3=="Running"{print $1"/"$2}' | head -1)

if [ -z "$BROKERS" ]; then
  _np=$(kpods | grep -c . 2>/dev/null) || _np=0
  _names=$(kpods | awk '{print "           " $1}' | head -8)
  if [ "${_np:-0}" -eq 0 ]; then
    die "ERROR: no pods at all in namespace $NS
         Either the namespace is wrong (-n NS) or the session cannot see it:
           oc get pods -n $NS"
  fi
  die "ERROR: $_np pod(s) in $NS, but none identified as a Kafka broker.
         Pods found:
$_names
         A broker is identified by the strimzi.io/broker-role=true label, or by
         a name ending -kafka-N / -broker-N. Check the labels with:
           oc get pods -n $NS -L strimzi.io/broker-role -L strimzi.io/controller-role
         If the labels are there and this still fails, the script's own
         ROLECOLS line is damaged — re-copy the file with a heredoc, never vi:
           head -1 perf-kafka.sh   # must be: #!/usr/bin/env sh
           sh -n perf-kafka.sh     # must print nothing"
fi

# ---- in-pod CLI probe -----------------------------------------------------
BPOD=$(printf '%s\n' "$BROKERS" | head -1)
export BPOD
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
# One broker, one controller, and kafka-exporter if it is deployed. Each JVM
# publishes a different slice of the MBean tree, so the catalogue has to be the
# union — reading only a broker is what hid the controller metrics.
EXP_PODS="${BPOD:-}"
_ctl=$(printf '%s\n' "$CONTROLLERS" | grep -c . 2>/dev/null) || _ctl=0
[ "${_ctl:-0}" -gt 0 ] && EXP_PODS="$EXP_PODS $(printf '%s\n' "$CONTROLLERS" | head -1)"
[ -n "${EXPORTER:-}" ] && EXP_PODS="$EXP_PODS $EXPORTER"
export EXP_PODS
EXPN=0
if [ -n "$(printf '%s' "$EXP_PODS" | tr -d ' ')" ]; then
  exporter_names >> "$MNAMES" 2>/dev/null
  sort -u -o "$MNAMES" "$MNAMES"
  EXPN=$(grep -c . "$MNAMES" 2>/dev/null); EXPN=${EXPN:-0}
fi
NMET=$(grep -c . "$MNAMES" 2>/dev/null); NMET=${NMET:-0}
PROBED=0
if [ "$NMET" -lt 8 ]; then
  probe_known >> "$MNAMES"
  sort -u -o "$MNAMES" "$MNAMES"
  NEW=$(grep -c . "$MNAMES" 2>/dev/null); NEW=${NEW:-0}
  PROBED=$(( NEW - NMET ))
  NMET=$NEW
fi

M_BYTESIN=$(find_metric 'brokertopicmetrics.*bytesin|bytes_in_per_sec|bytesinpersec' counter)
M_BYTESOUT=$(find_metric 'brokertopicmetrics.*bytesout|bytes_out_per_sec|bytesoutpersec' counter)
M_MSGIN=$(find_metric 'brokertopicmetrics.*messagesin|messages_in_per_sec|messagesinpersec' counter)
# Per-topic request outcomes. Produce = what producers sent, Fetch = what
# consumers asked for; the Failed* counters are the broker-side rejections
# (NOT_ENOUGH_REPLICAS, unknown topic/partition, bad request).
M_PRODTOT=$(find_metric 'brokertopicmetrics.*totalproducerequests' counter)
M_PRODFAIL=$(find_metric 'brokertopicmetrics.*failedproducerequests' counter)
M_FETCHTOT=$(find_metric 'brokertopicmetrics.*totalfetchrequests' counter)
M_FETCHFAIL=$(find_metric 'brokertopicmetrics.*failedfetchrequests' counter)
M_HANDLER=$(find_metric 'requesthandler.*idle|request_handler.*idle' gauge)
# RequestHandlerAvgIdlePercent is a Yammer Meter. The stock Strimzi exporter
# rules map its MeanRate to a gauge (their kafka-metrics.yaml line 61 says so in
# a comment) — mean since BROKER START, which on a broker up for weeks is pinned
# near its long-run average and cannot move during a load window. The Count
# attribute is the useful one: cumulative nanoseconds of idle time already
# divided by thread count inside Kafka, so rate()/1e9 is the live idle fraction.
M_HANDLER_C=$(find_metric 'requesthandler.*idle.*_(count|total)$' counter)
M_NETPROC=$(find_metric 'networkprocessor.*idle|network_processor.*idle' gauge)
# NetworkProcessorAvgIdlePercent sums io-wait-ratio over the processors of EVERY
# data-plane listener and divides by their total count, and a processor with no
# recorded ratio contributes 0 (counted as fully busy). With 8 network threads
# across three listeners that reads ~24% on an idle cluster. The per-listener
# series is exact, and the stock rules already publish it.
M_IOWAIT=$(find_metric 'socket_server_metrics_io_wait_ratio$|socket.server.*io_wait_ratio$' gauge)
M_URP=$(find_metric 'underreplicatedpartitions|under_replicated_partitions' gauge)
M_OFFLINE=$(find_metric 'offlinepartitionscount|offline_partitions_count' gauge)
M_ACTIVECTL=$(find_metric 'activecontrollercount|active_controller_count' gauge)
M_UNCLEAN=$(find_metric 'uncleanleaderelection' counter)
# ANCHORED. `replicamanager.*partitioncount` also matches
# replicamanager_atminisrpartitioncount and _underminisrpartitioncount, and
# find_metric takes the first alphabetical hit — so "a" beat "p" and the toolkit
# reported AtMinIsrPartitionCount (2/2/4) as the partition count beside a leader
# count of 163.
M_PARTS=$(find_metric 'replicamanager_partitioncount$|^kafka_server_replicamanager_partitioncount$' gauge)
# AtMinIsr is worth having in its own right: those partitions have exactly
# min.insync.replicas in sync, so one more replica loss stops writes to them.
M_ATMINISR=$(find_metric 'replicamanager_atminisrpartitioncount$' gauge)
M_LEADERS=$(find_metric 'replicamanager.*leadercount|leader_count' gauge)
M_ISRSHRINK=$(find_metric 'isrshrink' counter)
M_ISREXPAND=$(find_metric 'isrexpand' counter)
M_REQQ=$(find_metric 'requestqueuesize|request_queue_size' gauge)
M_RESPQ=$(find_metric 'responsequeuesize|response_queue_size' gauge)
M_PURG=$(find_metric 'purgatorysize|purgatory_size' gauge)
M_TOTTIME=$(find_metric 'requestmetrics.*totaltime|total_time_ms' gauge)
# a Timer's _count/_sum are not latencies; drop them if that is all we matched
case "$M_TOTTIME" in *_count|*_sum|*_total) M_TOTTIME="";; esac
M_QTIME=$(find_metric 'requestmetrics.*requestqueuetime|request_queue_time_ms' gauge)
M_LTIME=$(find_metric 'requestmetrics.*localtime|local_time_ms' gauge)
case "$M_QTIME" in *_count|*_sum|*_total) M_QTIME="";; esac
case "$M_LTIME" in *_count|*_sum|*_total) M_LTIME="";; esac
M_REQRATE=$(find_metric 'requestmetrics.*requestspersec|requests_per_sec' counter)
M_FETCHLAG=$(find_metric 'fetcherlag|consumer_lag|replica.*lag' gauge)
M_LOGSIZE=$(find_metric 'log_size|logsize' gauge)
M_HEAP=$(find_metric '^jvm_memory_bytes_used$|^jvm_memory_used_bytes$' gauge)
M_GCCOUNT=$(find_metric '^jvm_gc_[a-z_]*_seconds_count$' counter)
M_GCTIME=$(find_metric '^jvm_gc_[a-z_]*_seconds_sum$' counter)
M_CGLAG=$(find_metric '^kafka_consumergroup_lag$|consumergroup_lag' gauge)
# PVC usage comes from the kubelet, not from Kafka. The name catalogue is built
# mostly from the broker's own JMX exporter, which obviously never publishes a
# kubelet series — so looking these up by catalogue membership always failed and
# the disk CSV came out empty with no error. Ask Thanos directly instead: if the
# query returns a sample, the series exists.
pvc_probe() {
  for n in "$1" "$2"; do
    [ -z "$n" ] && continue
    if [ -n "$(qj "$n{namespace=\"$NS\"}" "persistentvolumeclaim" | awk -F'\t' '$1!="-"{print;exit}')" ]; then
      echo "$n"; return 0
    fi
  done
  return 0
}
M_PVCUSED=$(pvc_probe 'kubelet_volume_stats_used_bytes' 'kubelet_volume_stats_used_bytes:sum')
M_PVCCAP=$(pvc_probe 'kubelet_volume_stats_capacity_bytes' 'kubelet_volume_stats_capacity_bytes:sum')
[ -z "$M_PVCUSED" ] || [ -n "$M_PVCCAP" ] || M_PVCCAP="kubelet_volume_stats_capacity_bytes"

# which label carries the pod name on the JMX series
POD_LABEL="pod"
if [ -n "$M_BYTESIN" ]; then
  for L in pod kubernetes_pod_name instance; do
    if [ -n "$(qj "$M_BYTESIN{namespace=\"$NS\"}" "$L" | awk -F'\t' '$1!="-"{print;exit}')" ]; then
      POD_LABEL="$L"; break
    fi
  done
fi
# Does the latency series actually carry a quantile label? Without one, the only
# thing available is the Max attribute, and calling that "p99" in a report would
# be a lie — JoinGroup's Max is the rebalance timeout, not a typical latency.
LAT_Q=0; LAT_KIND="max"
if [ -n "$M_TOTTIME" ]; then
  if [ -n "$(qj "$M_TOTTIME{namespace=\"$NS\",quantile=\"0.99\"}" "request" | head -1)" ]; then
    LAT_Q=1; LAT_KIND="p99"
  fi
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
# Decide once whether a broker-level aggregate series exists, so the throughput
# queries select it instead of summing it together with its own per-topic parts.
agg_probe

# ---------------------------------------------------------------- preflight -
selfcheck() {
  fail=0
  echo "PERF-KAFKA PREFLIGHT   —   script version $PK_VERSION"
  echo "--------------------------------------------------------------------------"
  printf " 1. oc client          : "; v=$(oc version --client 2>&1 | head -1); echo "${v:-<no output>}"
  printf " 2. session valid      : "
  if [ -n "${WHO:-}" ]; then echo "yes ($WHO)"
  else echo "NO — token present but the API rejected it. Run: oc login"
       [ -s "$TMP/whoerr" ] && sed 's/^/      /' "$TMP/whoerr" | head -2
       fail=1; fi
  printf " 3. namespace          : "; if oc get ns "$NS" >/dev/null 2>&1; then echo "$NS"; else echo "$NS NOT FOUND"; fail=1; fi
  printf " 4. thanos route       : "
  if [ -n "$THANOS" ]; then echo "$THANOS"
  else echo "NOT READABLE"
       [ -s "$TMP/throute" ] && sed 's/^/      /' "$TMP/throute" | head -2
       echo "      supply it with -T <host> if your user cannot list routes there"
       fail=1; fi
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
  printf "11. metric names in Thanos matching kafka/jvm : %s" "$NMET"
  [ "$PROBED" -gt 0 ] && printf "  (%s found by direct probe — the label catalogue was empty)" "$PROBED"
  echo
  if [ "$NMET" -lt 5 ]; then
    echo "      ^ almost nothing found. Your Kafka CR probably has no metricsConfig,"
    echo "        so the JMX Prometheus exporter is not running. Without it this"
    echo "        script can still collect pod CPU/mem, placement, restarts and"
    echo "        (via the CLI) consumer lag and replication health — but not"
    echo "        throughput, saturation or latency. Enable spec.kafka.metricsConfig"
    echo "        on the Kafka CR for the full picture."
    fail=1
  fi
  echo "      resolved series   (series = how many Thanos actually returns right now):"
  for pair in "bytes in:$M_BYTESIN" "bytes out:$M_BYTESOUT" "messages in:$M_MSGIN" \
              "handler idle:$M_HANDLER" "network idle:$M_NETPROC" \
              "under-replicated:$M_URP" "offline parts:$M_OFFLINE" \
              "active controller:$M_ACTIVECTL" "unclean elections:$M_UNCLEAN" \
              "partitions:$M_PARTS" "leaders:$M_LEADERS" \
              "ISR shrink:$M_ISRSHRINK" "ISR expand:$M_ISREXPAND" \
              "request queue:$M_REQQ" "response queue:$M_RESPQ" "purgatory:$M_PURG" \
              "total time:$M_TOTTIME" "queue time:$M_QTIME" "local time:$M_LTIME" \
              "request rate:$M_REQRATE" "follower lag:$M_FETCHLAG" "log size:$M_LOGSIZE" \
              "produce req total:$M_PRODTOT" "produce req failed:$M_PRODFAIL" \
              "fetch req total:$M_FETCHTOT" "fetch req failed:$M_FETCHFAIL" \
              "JVM heap:$M_HEAP" "GC count:$M_GCCOUNT" "GC time:$M_GCTIME" \
              "consumer lag:$M_CGLAG" "PVC used:$M_PVCUSED"; do
    k=${pair%%:*}; v=${pair#*:}
    if [ -z "$v" ]; then
      printf "        %-20s %-62s %s\n" "$k" "— no matching metric name —" "0"
    else
      _n=$(qj "$v{namespace=\"$NS\"}" "" | grep -c . ); _n=${_n:-0}
      _m=""; [ "$_n" -eq 0 ] && _m="  <- NAME EXISTS BUT NO DATA"
      printf "        %-20s %-62s %s%s\n" "$k" "$v" "$_n" "$_m"
    fi
  done
  printf "      latency quantiles  : %s\n" \
    "$([ "$LAT_Q" -eq 1 ] && echo "yes — p99 available" || echo "no — only the Max attribute, reported as max")"
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
if [ "${DUMP:-0}" -eq 1 ]; then
  echo "metric names published by $BPOD (and seen in Thanos), filter: ${DUMPPAT:-all}"
  if [ -n "${DUMPPAT:-}" ]; then grep -iE "$DUMPPAT" "$MNAMES"; else cat "$MNAMES"; fi
  echo "---"
  echo "total: $(grep -c . "$MNAMES") names"
  exit 0
fi
[ "$CHECK" -eq 1 ] && selfcheck

# Which concepts resolved. Passed to the renderer so a missing metric renders as
# "not published" instead of a reassuring zero — a fake "0 under-replicated" in a
# Kafka report is worse than an honest blank.
HAVE=""
for pair in "bytesin:$M_BYTESIN" "handler:$M_HANDLER" "netproc:$M_NETPROC" \
            "urp:$M_URP" "parts:$M_PARTS" "leaders:$M_LEADERS" "atminisr:$M_ATMINISR" \
            "reqq:$M_REQQ" "respq:$M_RESPQ" "purg:$M_PURG" \
            "purg_prod:$M_PURG" "handler_c:$M_HANDLER_C" "iowait:$M_IOWAIT" \
            "ptot:$M_PRODTOT" "pfail:$M_PRODFAIL" "ftot:$M_FETCHTOT" "ffail:$M_FETCHFAIL" \
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
echo "ts_ist,elapsed_s,pod,bytes_in_mb_s,bytes_out_mb_s,messages_in_s,handler_idle_pct,network_idle_pct,request_queue,response_queue,purgatory,under_replicated,partitions,leaders,isr_shrink_s,isr_expand_s,follower_lag,at_min_isr,purgatory_produce" > "$C_BRK"
echo "ts_ist,elapsed_s,topic,bytes_in_mb_s,bytes_out_mb_s,messages_in_s,produce_ok_s,produce_fail_s,fetch_ok_s,fetch_fail_s" > "$C_TOP"
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
# empty, not zero: "0 MB/s" and "100% idle" would both be lies when the metric
# was never published at all
PEAK_IN=""; PEAK_OUT=""; PEAK_MSG=""; MIN_IDLE=""; MAX_URP=""; MAX_LAG=""; SAMPLES=0
REMAIN=""; TOTSAMP=""; ETA=""; PROG=""

finish() {
  END_IST=$(TZ=Asia/Kolkata date '+%Y-%m-%d %H:%M:%S')
  EL=$(( $(date +%s) - T0 ))
  echo
  echo "=================================================================================="
  echo "   PERF-KAFKA WINDOW SUMMARY   (tag: $TAG, namespace: $NS)"
  echo "=================================================================================="
  printf "   START / END (IST) : %s  ->  %s\n" "$START_IST" "$END_IST"
  _dnote=""
  if [ "$DURATION" -gt 0 ] && [ "$EL" -gt "$DURATION" ]; then
    _dnote=$(printf "   (requested %ss; the final sample was allowed to finish)" "$DURATION")
  fi
  printf "   DURATION          : %ss (%s min) over %s samples%s\n" \
     "$EL" "$(awk -v e=$EL 'BEGIN{printf "%.1f", e/60}')" "$SAMPLES" "$_dnote"
  printf "   BROKERS           : %s\n" "$NBROKER"
  if [ -n "$PEAK_IN" ]; then
    printf "   PEAK THROUGHPUT   : in %s MB/s   out %s MB/s   %s msg/s\n" "$PEAK_IN" "$PEAK_OUT" "$PEAK_MSG"
  else
    printf "   PEAK THROUGHPUT   : not measured — no JMX metrics reached Thanos (run --check)\n"
  fi
  if [ -n "$MIN_IDLE" ]; then
    printf "   WORST HANDLER IDLE: %s%%   (under 30%% = brokers saturated)\n" "$MIN_IDLE"
  else
    printf "   WORST HANDLER IDLE: not measured\n"
  fi
  if [ -n "$MAX_URP" ]; then
    printf "   MAX UNDER-REPL    : %s   (must return to 0)\n" "$MAX_URP"
  else
    printf "   MAX UNDER-REPL    : not measured via JMX"
    if [ -s "$C_HLT" ]; then
      printf "  (CLI check: %s)\n" "$(awk -F, '$3=="under_replicated_partitions"{m=($4+0>m)?$4+0:m} END{print m+0" peak"}' "$C_HLT")"
    else printf "\n"; fi
  fi
  printf "   MAX CONSUMER LAG  : %s\n" "${MAX_LAG:-not measured}"
  echo "=================================================================================="
  printf "   CSVs in %s  (tag %s)\n" "$OUTDIR" "$TAG"
  rm -rf "$TMP"

  # Build the HTML straight away — the report is the deliverable, and a capture
  # that needs a second command to become readable gets forgotten.
  if [ "$NOREPORT" -eq 0 ]; then
    _rep="$SCRIPT_DIR/kafka_report.py"
    if [ -f "$_rep" ]; then
      _out="$OUTDIR/KAFKA_Report_$(date +%Y%m%d_%H%M%S).html"
      echo
      # One flat file per run, so a question about a pod or a topic does not
      # need nine CSVs joined by hand.
      _sum="$SCRIPT_DIR/kafka_summary.py"
      [ -f "$_sum" ] && python3 "$_sum" --dir "$OUTDIR" --tag "$TAG" 2>/dev/null || :
      echo "[*] building the HTML report..."
      if python3 "$_rep" --dir "$OUTDIR" --tag "$TAG" -o "$_out"; then
        echo "[OK] REPORT: $_out"
      else
        echo "[!] report could not be built — the CSVs in $OUTDIR are still complete."
        echo "    retry with: python3 $_rep --dir $OUTDIR --tag $TAG -o report.html"
      fi
    else
      echo "[!] kafka_report.py not found beside perf-kafka.sh — CSVs written, no HTML."
      echo "    build it with: python3 kafka_report.py --dir $OUTDIR --tag $TAG -o report.html"
    fi
  fi
  exit 0
}
trap finish INT

echo
echo "[*] perf-kafka.sh version $PK_VERSION"
echo "[*] plan for this window:"
printf "      duration   : %ss (%s)\n" "$DURATION" "$(fmt_dur "$DURATION")"
printf "      interval   : every %ss   (deep CLI capture every %s cycles)\n" "$INT" "$DEEP_EVERY"
printf "      namespace  : %s   brokers: %s\n" "$NS" "$NBROKER"
printf "      run folder : %s\n" "$OUTDIR"
# leave a fingerprint beside the data
{ echo "script_version=$PK_VERSION"
  echo "started_ist=$(TZ=Asia/Kolkata date '+%Y-%m-%d %H:%M:%S')"
  echo "namespace=$NS"; echo "interval_s=$INT"; echo "duration_s=$DURATION"
  echo "tag=$TAG"; echo "brokers=$NBROKER"
  echo "latency_kind=$LAT_KIND"
  # The report labels the saturation figure by its provenance: a windowed rate
  # off the counter is a real measurement; the MeanRate gauge is not.
  if [ -n "$M_HANDLER_C" ]; then echo "idle_source=counter_rate"
  else echo "idle_source=meanrate_gauge"; fi
  if [ -n "$M_IOWAIT" ]; then echo "net_idle_source=per_listener"
  else echo "net_idle_source=aggregate_gauge"; fi
} > "$OUTDIR/run_info.txt" 2>/dev/null
[ "$NOREPORT" -eq 0 ] && printf "      the HTML report is built automatically when the window closes\n"
echo
[ "$NMET" -lt 5 ] && echo "  [!] few/no JMX metrics in Thanos — run --check; collection will be partial"
[ "$CLI_OK" -eq 0 ] && [ "$USE_CLI" -eq 1 ] && echo "  [!] in-pod Kafka CLI unusable — consumer lag/health from CLI will be empty"

i=0
while :; do
  sleep "$INT"
  NOW=$(date +%s)
  if [ "$DEADLINE" -gt 0 ] && [ "$NOW" -ge "$DEADLINE" ]; then
    # The last sample is allowed to finish; a collection cannot be aborted
    # half-way without writing a partial row. So the window can run slightly
    # past the requested duration, and the summary states the real elapsed
    # time rather than the requested one.
    echo "[*] requested ${DURATION}s reached — no further samples will start."
    finish
  fi
  i=$((i+1)); SAMPLES=$i
  W="${INT}s"; [ "$INT" -lt 60 ] && W="2m"   # rate() needs >= 2 scrapes

  # ---------- Thanos: broker throughput + saturation ----------
  : > "$TMP/brk"
  if [ -n "$M_BYTESIN" ]; then
    qj "sum by($POD_LABEL) ($(rate_expr "$M_BYTESIN"  "$W" "$AGG_SEL"))" "$POD_LABEL" > "$TMP/in"  || :
    qj "sum by($POD_LABEL) ($(rate_expr "$M_BYTESOUT" "$W" "$AGG_SEL"))" "$POD_LABEL" > "$TMP/out" || :
    qj "sum by($POD_LABEL) ($(rate_expr "$M_MSGIN"    "$W" "$AGG_SEL"))" "$POD_LABEL" > "$TMP/msg" || :
  else : > "$TMP/in"; : > "$TMP/out"; : > "$TMP/msg"; fi
  for m in HANDLER NETPROC REQQ RESPQ URP PARTS LEADERS FETCHLAG ATMINISR; do
    eval "nm=\$M_$m"
    f="$TMP/$(echo $m | tr 'A-Z' 'a-z')"
    if [ -n "$nm" ]; then qj "sum by($POD_LABEL) ($nm{namespace=\"$NS\"})" "$POD_LABEL" > "$f" || :; else : > "$f"; fi
  done

  # ---------- handler idle, measured over THIS window ----------
  # rate() of the cumulative idle-nanoseconds counter / 1e9 = idle fraction now.
  # The gauge beside it is a since-broker-start mean and is kept only as a
  # fallback, flagged, for exporters that publish no counter.
  : > "$TMP/handler_w"
  if [ -n "$M_HANDLER_C" ]; then
    qj "clamp_max(sum by($POD_LABEL) (rate($M_HANDLER_C{namespace=\"$NS\"}[${W}])) * 100 / 1e9, 100)" \
       "$POD_LABEL" > "$TMP/handler_w" 2>/dev/null || :
  fi
  # A live reading wins over the frozen mean; an empty result must not clobber it.
  if [ -s "$TMP/handler_w" ]; then cp "$TMP/handler_w" "$TMP/handler"; fi

  # ---------- purgatory, split by what is actually waiting ----------
  # Summed across every delayedOperation, Fetch long-polls dominate and the
  # total is large on a completely idle cluster, which says nothing. Produce
  # purgatory is the one that indicates trouble.
  : > "$TMP/purg"; : > "$TMP/purg_prod"
  if [ -n "$M_PURG" ]; then
    qj "sum by($POD_LABEL) ($M_PURG{namespace=\"$NS\",delayedOperation=\"Fetch\"})" \
       "$POD_LABEL" > "$TMP/purg" 2>/dev/null || :
    qj "sum by($POD_LABEL) ($M_PURG{namespace=\"$NS\",delayedOperation=\"Produce\"})" \
       "$POD_LABEL" > "$TMP/purg_prod" 2>/dev/null || :
    # no delayedOperation label on this exporter: fall back to the plain sum
    [ -s "$TMP/purg" ] || qj "sum by($POD_LABEL) ($M_PURG{namespace=\"$NS\"})" \
       "$POD_LABEL" > "$TMP/purg" 2>/dev/null || :
  fi

  # ---------- network idle, per listener ----------
  # `> 0` drops listeners whose processors never recorded a ratio; min() then
  # gives the busiest listener that is actually carrying connections.
  if [ -n "$M_IOWAIT" ]; then
    qj "clamp_max(min by($POD_LABEL) ($M_IOWAIT{namespace=\"$NS\"} > 0) * 100, 100)" \
       "$POD_LABEL" > "$TMP/netproc_w" 2>/dev/null || :
    [ -s "$TMP/netproc_w" ] && cp "$TMP/netproc_w" "$TMP/netproc"
  fi
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
    qj "sum by($TOPIC_LABEL) ($(rate_expr "$M_BYTESIN"  "$W" 'topic!=""'))" "$TOPIC_LABEL" > "$TMP/t_in"  || :
    qj "sum by($TOPIC_LABEL) ($(rate_expr "$M_BYTESOUT" "$W" 'topic!=""'))" "$TOPIC_LABEL" > "$TMP/t_out" || :
    qj "sum by($TOPIC_LABEL) ($(rate_expr "$M_MSGIN"    "$W" 'topic!=""'))" "$TOPIC_LABEL" > "$TMP/t_msg" || :
  else : > "$TMP/t_in"; : > "$TMP/t_out"; : > "$TMP/t_msg"; fi
  # per-topic produce / fetch request outcomes
  for _pair in "t_ptot:$M_PRODTOT" "t_pfail:$M_PRODFAIL" "t_ftot:$M_FETCHTOT" "t_ffail:$M_FETCHFAIL"; do
    _f="$TMP/${_pair%%:*}"; _m="${_pair#*:}"
    if [ -n "$_m" ] && [ -n "$TOPIC_LABEL" ]; then
      qj "sum by($TOPIC_LABEL) ($(rate_expr "$_m" "$W" 'topic!=""'))" "$TOPIC_LABEL" > "$_f" 2>/dev/null || :
    else : > "$_f"; fi
  done

  # ---------- Thanos: latency by request type ----------
  : > "$TMP/lat"
  if [ -n "$M_TOTTIME" ]; then
    _ql=""; [ "$LAT_Q" -eq 1 ] && _ql=',quantile="0.99"'
    # Grouped by (request, broker), NOT by request alone. Three separate
    # `max by(request)` queries take each column's maximum independently, so the
    # total can come from one broker while the queue time comes from another —
    # which produced queue times LARGER than the total they are part of. Keeping
    # the broker label lets the renderer take all three from the same broker, so
    # the breakdown belongs to one real request path.
    qj "max by(request,$POD_LABEL) ($M_TOTTIME{namespace=\"$NS\"$_ql})" \
       "request,$POD_LABEL" > "$TMP/l_tot" 2>/dev/null
    [ -s "$TMP/l_tot" ] || : > "$TMP/l_tot"
    if [ -n "$M_QTIME" ]; then
      qj "max by(request,$POD_LABEL) ($M_QTIME{namespace=\"$NS\"$_ql})" \
         "request,$POD_LABEL" > "$TMP/l_q" 2>/dev/null
    fi; [ -s "$TMP/l_q" ] || : > "$TMP/l_q"
    if [ -n "$M_LTIME" ]; then
      qj "max by(request,$POD_LABEL) ($M_LTIME{namespace=\"$NS\"$_ql})" \
         "request,$POD_LABEL" > "$TMP/l_l" 2>/dev/null
    fi; [ -s "$TMP/l_l" ] || : > "$TMP/l_l"
    [ -n "$M_REQRATE" ] && qj "sum by(request) ($(rate_expr "$M_REQRATE" "$W"))" "request" > "$TMP/l_r" || : > "$TMP/l_r"
  else : > "$TMP/l_tot"; : > "$TMP/l_q"; : > "$TMP/l_l"; : > "$TMP/l_r"; fi

  # ---------- Thanos: JVM ----------
  if [ -n "$M_HEAP" ]; then
    # `a || b` only runs b when a FAILS. An empty result is a success, so the
    # area="heap" filter returning nothing silently left heap blank. Test the file.
    qj "sum by($POD_LABEL) ($M_HEAP{namespace=\"$NS\",area=\"heap\"})" "$POD_LABEL" > "$TMP/heap" 2>/dev/null
    [ -s "$TMP/heap" ] || qj "sum by($POD_LABEL) ($M_HEAP{namespace=\"$NS\"})" "$POD_LABEL" > "$TMP/heap" 2>/dev/null
    [ -s "$TMP/heap" ] || : > "$TMP/heap"
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
  OUT=$(TS="$TS" EL="$EL" TMPD="$TMP" NS="$NS" HAVE="$HAVE" LAT_KIND="$LAT_KIND" \
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

  # ---------- countdown, computed HERE and not at the top of the loop ----------
  # Two separate bugs lived in the old placement and arithmetic:
  #
  #   * REMAIN/ETA were computed before collection, then printed after it. On a
  #     cycle that took 11s, the screen said "0m10s remaining, ends ~15:57:45"
  #     at 15:57:45 — already past its own ETA.
  #   * TOTSAMP was DURATION/INT, i.e. it assumed a cycle costs only the sleep.
  #     A cycle is the sleep PLUS the Thanos and oc round trips, which here is
  #     20-43s against a 10s interval, so "sample 6/18" counted towards a total
  #     the run could never reach.
  #
  # Both are now derived from the measured cycle time.
  NOW=$(date +%s)
  CYC=$(( NOW - T0 )); [ "$i" -gt 0 ] && CYC=$(( CYC / i )); [ "$CYC" -lt 1 ] && CYC=1
  if [ "$DEADLINE" -gt 0 ]; then
    REMAIN=$(( DEADLINE - NOW )); [ "$REMAIN" -lt 0 ] && REMAIN=0
    TOTSAMP=$(( i + (REMAIN + CYC - 1) / CYC ))
    ETA=$(TZ=Asia/Kolkata date -d "@$DEADLINE" '+%H:%M:%S' 2>/dev/null \
          || TZ=Asia/Kolkata date -r "$DEADLINE" '+%H:%M:%S' 2>/dev/null || echo "?")
    PROG=$(awk -v d="$DURATION" -v r="$REMAIN" 'BEGIN{p=(d>0)?(d-r)*100/d:0; if(p>100)p=100; printf "%.0f", p}')
  else
    REMAIN=""; TOTSAMP=""; ETA=""; PROG=""
  fi

  if [ "$QUIET" -eq 1 ]; then
    if [ -n "$REMAIN" ]; then
      printf "[kafka] %s  sample %s/%s  %s%%  %s left  in %s MB/s  hnd-idle %s%%  urp %s\n" \
        "$TS" "$i" "$TOTSAMP" "$PROG" "$(fmt_hms "$REMAIN")" \
        "${CUR_IN:-?}" "${CUR_IDLE:-?}" "${CUR_URP:-?}"
    else
      printf "[kafka] %s  sample %s  elapsed %ss  in %s MB/s  hnd-idle %s%%  urp %s\n" \
        "$TS" "$i" "$EL" "${CUR_IN:-?}" "${CUR_IDLE:-?}" "${CUR_URP:-?}"
    fi
    continue
  fi
  clear
  echo "=================================================================================="
  echo "   PERF-KAFKA — $TS IST    cycle ${INT}s set / ${CYC}s actual    ns=$NS    tag=$TAG"
  if [ -n "$REMAIN" ]; then
    _bar=$(awk -v p="$PROG" 'BEGIN{n=int(p/2.5); for(i=0;i<40;i++) printf (i<n)?"#":"."}')
    printf "   elapsed %-7s  REMAINING %-8s  ends ~%s IST   sample %s/%s\n" \
      "$(fmt_hms "$EL")" "$(fmt_hms "$REMAIN")" "$ETA" "$i" "$TOTSAMP"
    printf "   [%s] %s%%\n" "$_bar" "$PROG"
  else
    printf "   elapsed %-7s  no duration limit — runs until Ctrl-C   sample %s\n" \
      "$(fmt_hms "$EL")" "$i"
  fi
  echo "   window start $START_IST IST"
  echo "=================================================================================="
  printf '%s\n' "$OUT" | grep -v '^##STAT '
  [ -n "$LAGLINE" ] && { echo; echo "$LAGLINE"; }
  echo "=================================================================================="
  if [ -n "$REMAIN" ]; then
    printf "   %s remaining (ends ~%s IST)   |   Ctrl-C to stop early and still get the report\n" \
      "$(fmt_hms "$REMAIN")" "$ETA"
  else
    printf "   Ctrl-C for the window summary and the HTML report\n"
  fi
  printf "   CSVs: %s/kafka_*_%s.csv\n" "$OUTDIR" "$TAG"
done
