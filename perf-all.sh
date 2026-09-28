#!/usr/bin/env sh
# perf-all.sh — ONE script, ONE window: gateway ingress + node-level traffic.
#
#   ./perf-all.sh                        # refresh every 10s, stop with Ctrl-C
#   ./perf-all.sh 10 run1                # 10s refresh -> traffic_run1.csv + nodenet_run1.csv
#   ./perf-all.sh 10 run1 -o DIR         # write both CSVs into DIR
#   ./perf-all.sh 10 run1 -d 1800        # stop itself after 1800s (no Ctrl-C needed)
#   ./perf-all.sh --check                # preflight: test every dependency, name the failure
#
#   SECTION 1  gateway ingress   (= what AVLB delivered to OCP)
#   SECTION 2  per-node network + requests, grouped
#
# Both sections share the SAME baseline and the SAME window, so the totals
# reconcile. Ctrl-C (or -d) prints one summary for the whole window.
#
# Use 10s or higher: section 2 execs into every perf pod each cycle.
set -u

CHECK=0
[ "${1:-}" = "--check" ] && { CHECK=1; shift; }

# positional INTERVAL and TAG stay first for backward compatibility
INT=10; TAG=""
case "${1:-}" in ''|-*) ;; *) INT="$1"; shift;; esac
case "${1:-}" in ''|-*) ;; *) TAG="$1"; shift;; esac
OUTDIR="."; DURATION=0
while [ $# -gt 0 ]; do
  case "$1" in
    -o) OUTDIR="${2:-.}"; shift 2;;
    -d) DURATION="${2:-0}"; shift 2;;
    *)  shift;;
  esac
done
[ -z "$TAG" ] && TAG=$(date +%H%M)
mkdir -p "$OUTDIR" 2>/dev/null
GWNS="perf-istio-system"
GWSEL="istio=perf-istio-gateway"
MPATH="/stats/prometheus"
GCSV="$OUTDIR/traffic_${TAG}.csv"
NCSV="$OUTDIR/nodenet_${TAG}.csv"
REQTMP="/tmp/.pa_req"

# Redrawing the screen only makes sense on a terminal. Launched from perf-run.sh
# its output is piped, so print one progress line per cycle instead.
QUIET=0; [ -t 1 ] || QUIET=1

# In --check mode nothing exits early: the point is to reach every step and show
# which one fails, so a missing token does not hide a broken pod list.
die() { [ "$CHECK" -eq 1 ] && return 0; echo "$1"; exit 1; }

command -v oc >/dev/null 2>&1 || { echo "ERROR: oc not found in PATH"; exit 1; }
TOKEN=$(oc whoami -t 2>/dev/null)
[ -z "$TOKEN" ] && die "ERROR: no oc token (run: oc login)"
THANOS=$(oc -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}' 2>/dev/null)
[ -z "$THANOS" ] && die "ERROR: thanos-querier route not found"

GWPODS=$(oc -n "$GWNS" get pods -l "$GWSEL" --no-headers 2>/dev/null | awk '$3=="Running"{print $1}')
[ -z "$GWPODS" ] && die "ERROR: no running gateway pods in $GWNS"
NGW=$(echo "$GWPODS" | grep -c . )

# ---------- gateway reader ----------
gwp() {
  oc -n "$GWNS" exec "$1" -c istio-proxy -- pilot-agent request GET "$MPATH" 2>/dev/null \
  | awk '/^istio_requests_total/ {s+=$NF} END{print s+0}'
}

q() {  # run a promQL instant query, print "label value" lines
  curl -sk --max-time 15 -H "Authorization: Bearer $TOKEN" \
    --data-urlencode "query=$1" "https://$THANOS/api/v1/query" 2>/dev/null \
  | python3 -c '
import sys,json
try: d=json.load(sys.stdin)
except Exception: sys.exit(0)
if d.get("status")!="success": sys.exit(0)
for r in d["data"]["result"]:
    m=r["metric"]
    k=m.get("instance") or m.get("node") or m.get("exported_instance") or "?"
    k=k.split(":")[0].split(".")[0]
    try: v=float(r["value"][1])
    except Exception: v=0.0
    print(k, v)
'
}

# Millisecond wall clock. `date +%s%3N` is GNU (present on RHEL); anything else
# falls back to whole seconds. Rates divide by this, so a +/-1s rounding at each
# end of a 30s gap stops costing ~3% accuracy.
now_ms() {
  _t=$(date +%s%3N 2>/dev/null)
  case "$_t" in
    ''|*[!0-9]*) echo "$(date +%s)000" ;;
    *)           echo "$_t" ;;
  esac
}

# ---------- pod -> node map ----------
# custom-columns, NOT -o wide. A RESTARTS cell renders as "1 (51m ago)", which splits
# into three whitespace fields and shifts every later column: $8 then held the pod's
# AGE ("51m") instead of its node, and that pod's requests were filed under a phantom
# node whose name ticked over every minute. custom-columns has fixed field positions.
CCOLS='NS:.metadata.namespace,POD:.metadata.name,NODE:.spec.nodeName,PHASE:.status.phase,CTRS:.spec.containers[*].name'
podlist() { oc get pods -A --no-headers -o custom-columns="$CCOLS"; }

# ---------- requests per NODE (istio sidecars, grouped by node) ----------
REQTMP="/tmp/.nn_req"
collect_requests() {
  rm -rf "$REQTMP"; mkdir -p "$REQTMP"
  # gateway pods go in the SAME parallel batch so both snapshots are simultaneous
  for gp in $GWPODS; do
    ( v=$(oc -n "$GWNS" exec "$gp" -c istio-proxy -- \
            pilot-agent request GET "$MPATH" 2>/dev/null \
          | awk '/^istio_requests_total/ {s+=$NF} END{print s+0}')
      echo "${v:-0}" > "$REQTMP/GW__$gp" ) &
  done
  # custom-columns, NOT -o wide. A RESTARTS cell renders as "1 (51m ago)", which
  # splits into three whitespace fields and shifts every later column: $8 then held
  # the pod's AGE ("51m") instead of its node, and that pod's requests were filed
  # under a phantom node whose name ticked over every minute.
  podlist > "$REQTMP/raw" 2>"$REQTMP/err"
  awk '$1 ~ /^perf-/ && $4=="Running" && $5 ~ /istio-proxy/ {print $1"|"$2"|"$3}' \
      "$REQTMP/raw" > "$REQTMP/map"
  if [ ! -s "$REQTMP/map" ]; then
    # stdout is redirected into a temp file here, so every word of this goes to stderr
    {
      echo
      echo "ERROR: could not map any perf pod to a node."
      _rows=$(grep -c . "$REQTMP/raw" 2>/dev/null); echo "  rows returned by oc : ${_rows:-0}"
      [ -s "$REQTMP/err" ] && { echo "  oc said:"; sed 's/^/    /' "$REQTMP/err"; }
      [ -s "$REQTMP/raw" ] && { echo "  first perf row seen:"
        grep '^perf-' "$REQTMP/raw" | head -1 | sed 's/^/    /'; }
      echo "  expected columns: 1=namespace 2=pod 3=node 4=Running 5=...,istio-proxy"
      echo "  run  ./perf-all.sh --check  for a step-by-step diagnosis"
    } >&2
    exit 1
  fi
  while IFS='|' read -r ns pod node; do
    [ -z "$node" ] && continue
    node=${node%%.*}
    ( oc -n "$ns" exec "$pod" -c istio-proxy -- \
        pilot-agent request GET /stats/prometheus 2>/dev/null \
      | awk '/^istio_requests_total\{/ && /reporter="destination"/ {
               all += $NF
               if ($0 ~ /source_workload="[^"]*gateway/) gw += $NF
             }
             END { printf "%d %d\n", gw+0, all+0 }' > "$REQTMP/${node}__${pod}" ) &
  done < "$REQTMP/map"
  wait
  for f in "$REQTMP"/*__*; do
    [ -e "$f" ] || continue
    b=$(basename "$f"); n=${b%%__*}
    [ "$n" = "GW" ] && continue
    g=$(awk '{print $1+0; exit}' "$f" 2>/dev/null); g=${g:-0}
    a=$(awk '{print $2+0; exit}' "$f" 2>/dev/null); a=${a:-0}
    echo "$n ${g:-0} ${a:-0}"
  done | awk '{g[$1]+=$2; a[$1]+=$3} END{for(n in g) print n, g[n], a[n]}'
}

# ---------- preflight ----------
# ./perf-all.sh --check  runs every dependency once and shows the real error for
# whichever one fails, instead of the script dying with a one-line message.
selfcheck() {
  fail=0
  echo "PERF-ALL PREFLIGHT"
  echo "--------------------------------------------------------------------------"
  printf "1. oc client            : "
  ocv=$(oc version --client 2>&1 | head -1)
  echo "${ocv:-<no output>}"

  printf "2. logged in            : "
  if [ -n "$TOKEN" ]; then echo "yes  ($(oc whoami 2>/dev/null))"; else echo "NO"; fail=1; fi

  printf "3. thanos route         : "
  if [ -n "$THANOS" ]; then echo "$THANOS"; else echo "NOT FOUND"; fail=1; fi

  printf "4. gateway pods         : "
  if [ -n "$GWPODS" ]; then echo "$NGW found"; else echo "NONE"; fail=1; fi

  printf "5. gateway counter      : "
  gp1=$(echo "$GWPODS" | head -1)
  c=$([ -n "$gp1" ] && gwp "$gp1" 2>&1)
  case "$c" in
    ''|0) echo "UNREADABLE"
          echo "     raw output of the exec:"
          [ -n "$gp1" ] && oc -n "$GWNS" exec "$gp1" -c istio-proxy -- \
             pilot-agent request GET "$MPATH" 2>&1 | head -3 | sed 's/^/       /'
          fail=1 ;;
    *) echo "$c" ;;
  esac

  printf "6. thanos query         : "
  n=$(q 'up{job="node-exporter"}' | wc -l)
  if [ "$n" -gt 0 ]; then echo "$n nodes reporting"; else
    echo "NO DATA"
    echo "     raw response:"
    curl -sk --max-time 15 -H "Authorization: Bearer $TOKEN" \
      --data-urlencode 'query=up{job="node-exporter"}' \
      "https://$THANOS/api/v1/query" 2>&1 | head -c 300 | sed 's/^/       /'
    echo; fail=1
  fi

  printf "7. pod list (columns)   : "
  raw=$(podlist 2>&1)
  rows=$(printf '%s\n' "$raw" | grep -c . )
  if [ "$rows" -lt 2 ]; then
    echo "FAILED"
    echo "     oc said:"; printf '%s\n' "$raw" | head -4 | sed 's/^/       /'
    fail=1
  else
    echo "$rows rows"
    printf "8. perf pods with mesh  : "
    m=$(printf '%s\n' "$raw" \
        | awk '$1 ~ /^perf-/ && $4=="Running" && $5 ~ /istio-proxy/ {print $1"|"$2"|"$3}')
    mc=$(printf '%s\n' "$m" | grep -c . )
    if [ "$mc" -eq 0 ]; then
      echo "0  <-- THIS IS THE PROBLEM"
      echo "     a perf row looks like:"
      printf '%s\n' "$raw" | grep '^perf-' | head -1 | sed 's/^/       /'
      echo "     expected: col1=namespace col2=pod col3=node col4=Running col5=...,istio-proxy"
      fail=1
    else
      echo "$mc pods"
      echo "     sample: $(printf '%s\n' "$m" | head -1)"
      printf "9. sidecar metrics      : "
      p=$(printf '%s\n' "$m" | head -1)
      pns=$(echo "$p" | cut -d'|' -f1); ppod=$(echo "$p" | cut -d'|' -f2)
      v=$(oc -n "$pns" exec "$ppod" -c istio-proxy -- \
            pilot-agent request GET /stats/prometheus 2>&1 \
          | awk '/^istio_requests_total\{/ && /reporter="destination"/ {s+=$NF} END{print s+0}')
      if [ "${v:-0}" -gt 0 ] 2>/dev/null; then echo "$v requests on that pod"
      else echo "0 or unreadable (fine if no load is running)"; fi
    fi
  fi
  echo "--------------------------------------------------------------------------"
  [ "$fail" -eq 0 ] && echo "ALL CHECKS PASSED — safe to run." || echo "FIX THE FAILING STEP ABOVE."
  exit "$fail"
}
[ "$CHECK" -eq 1 ] && selfcheck

# ---------- sanity checks ----------
CHK=$(gwp "$(echo "$GWPODS" | head -1)")
[ -z "$CHK" ] || [ "$CHK" = "0" ] && {
  echo "ERROR: cannot read gateway counter — run: ./perf-all.sh --check"; exit 1; }
T=$(q 'up{job="node-exporter"}' | wc -l)
[ "$T" -eq 0 ] && {
  echo "ERROR: thanos returned no node-exporter data — run: ./perf-all.sh --check"; exit 1; }
echo "gateway pods: $NGW    nodes reporting: $T"

# ---------- ONE shared baseline ----------
echo "capturing baseline (gateway + all app pods)..."
collect_requests > /tmp/.pa_req_base
i=0; GW_BASE=0
for p in $GWPODS; do
  i=$((i+1)); v=$(cat "$REQTMP/GW__$p" 2>/dev/null); [ -z "$v" ] && v=0
  eval "GWPREV_$i=$v; GWBASE_$i=$v"
  GW_BASE=$((GW_BASE + v))
done

GW_PREV=$GW_BASE
GW_SINCE=0
T0=$(date +%s)
T0_MS=$(now_ms)
PREV_READ_MS=$T0_MS   # ms clock of the last counter read (baseline counts as read 0)
LAST_READ_MS=$T0_MS
DEADLINE=0
[ "$DURATION" -gt 0 ] 2>/dev/null && DEADLINE=$(( T0 + DURATION ))
START_IST=$(TZ=Asia/Kolkata date '+%Y-%m-%d %H:%M:%S')

echo "ts_ist,elapsed_s,rate_rps,total_since_start" > "$GCSV"
echo "ts_ist,node,rx_mbps,tx_mbps,rx_pps,tx_pps,conntrack,from_gateway,all_received" > "$NCSV"

finish() {
  END_IST=$(TZ=Asia/Kolkata date '+%Y-%m-%d %H:%M:%S')
  # average over the span the counters actually cover (baseline read -> last read),
  # so this figure matches the CSV and the HTML report instead of adding the
  # trailing partial cycle to the denominator.
  EL_MS=$(( LAST_READ_MS - T0_MS ))
  [ "$EL_MS" -lt 1 ] && EL_MS=$(( $(now_ms) - T0_MS ))
  [ "$EL_MS" -lt 1 ] && EL_MS=1000
  EL=$(( (EL_MS + 500) / 1000 )); [ "$EL" -lt 1 ] && EL=1
  echo
  echo "=================================================================================="
  echo "   LOAD WINDOW SUMMARY   (tag: $TAG)"
  echo "=================================================================================="
  printf "   START (IST)  : %s\n" "$START_IST"
  printf "   END   (IST)  : %s\n" "$END_IST"
  printf "   DURATION     : %s s (%s min)\n" "$EL" "$(awk -v e=$EL 'BEGIN{printf "%.1f", e/60}')"
  printf "   OCP RECEIVED : %s requests\n" "$GW_SINCE"
  printf "   AVERAGE RATE : %s req/s\n" \
     "$(awk -v t="$GW_SINCE" -v e="$EL_MS" 'BEGIN{ if(e>0) printf "%.1f", t*1000/e; else print 0}')"
  echo "=================================================================================="
  printf "   CSV: %s  and  %s\n" "$GCSV" "$NCSV"
  rm -rf "$REQTMP"
  exit 0
}
trap finish INT

# ---------- single loop: both sections, same window ----------
while :; do
  sleep "$INT"
  NOW=$(date +%s)
  if [ "$DEADLINE" -gt 0 ] && [ "$NOW" -ge "$DEADLINE" ]; then
    echo "[*] duration limit reached (${DURATION}s) — closing the window on schedule."
    finish
  fi

  # ===== section 2 data: nodes =====
  NETDEV='device!~"lo|veth.*|br.*|ovn.*|genev.*|tun.*|docker.*"'
  q "sum by(instance) (rate(node_network_receive_bytes_total{$NETDEV}[1m]))"  > /tmp/.nn_rx
  q "sum by(instance) (rate(node_network_transmit_bytes_total{$NETDEV}[1m]))" > /tmp/.nn_tx
  q "sum by(instance) (rate(node_network_receive_packets_total{$NETDEV}[1m]))"  > /tmp/.nn_rp
  q "sum by(instance) (rate(node_network_transmit_packets_total{$NETDEV}[1m]))" > /tmp/.nn_tp
  q 'node_nf_conntrack_entries' > /tmp/.nn_ct
  collect_requests > /tmp/.pa_req_now

  # The counters for BOTH sections were just read. Time the READ, not the start of
  # the cycle: a cycle is `sleep INT` plus ~20s of pod execs and Thanos queries, so
  # dividing the counter delta by INT overstated the rate by the ratio between the
  # two (~3x at INT=10). Rate must divide by the real gap between reads.
  READ_MS=$(now_ms)
  EL=$(( (READ_MS - T0_MS + 500) / 1000 ))
  GAP_MS=$(( READ_MS - PREV_READ_MS )); [ "$GAP_MS" -lt 1 ] && GAP_MS=1
  GAP=$(( (GAP_MS + 500) / 1000 )); [ "$GAP" -lt 1 ] && GAP=1
  PREV_READ_MS=$READ_MS
  LAST_READ_MS=$READ_MS
  TS=$(TZ=Asia/Kolkata date '+%Y-%m-%d %H:%M:%S')

  # ===== section 1 data: gateway (values captured in the same batch as section 2) =====
  # prev/base are filled by the eval inside the loop; initialise them so the
  # script is safe under `set -u` even if GWPODS is somehow empty.
  GW_NOW=0; GWLINES=""; k=0; prev=0; base=0
  for p in $GWPODS; do
    k=$((k+1)); v=$(cat "$REQTMP/GW__$p" 2>/dev/null); [ -z "$v" ] && v=0
    eval "prev=\$GWPREV_$k; base=\$GWBASE_$k"
    d=$((v - prev)); [ "$d" -lt 0 ] && d=0
    tp=$((v - base)); [ "$tp" -lt 0 ] && tp=0
    r=$(awk -v d="$d" -v g="$GAP_MS" 'BEGIN{printf "%9.1f", d*1000/g}')
    GWLINES="${GWLINES}$(printf '     %s /s  %s total   %s' "$r" "$tp" "$p")\n"
    eval "GWPREV_$k=$v"
    GW_NOW=$((GW_NOW + v))
  done
  DG=$((GW_NOW - GW_PREV)); [ "$DG" -lt 0 ] && DG=0
  GW_SINCE=$((GW_NOW - GW_BASE)); [ "$GW_SINCE" -lt 0 ] && GW_SINCE=0
  GW_PREV=$GW_NOW
  RPS=$(awk -v d="$DG" -v g="$GAP_MS" 'BEGIN{printf "%.1f", d*1000/g}')
  AVG=$(awk -v t="$GW_SINCE" -v e="$READ_MS" -v z="$T0_MS" \
        'BEGIN{ e=e-z; if(e>0) printf "%.1f", t*1000/e; else print "0.0"}')
  echo "$TS,$EL,$RPS,$GW_SINCE" >> "$GCSV"

  OUT=$(python3 - "$TS" "$NCSV" <<'PY'
import sys
ts, csv = sys.argv[1], sys.argv[2]
def load2(f):          # "node v1 v2" -> {node:(v1,v2)}
    d={}
    try:
        for l in open(f):
            p=l.split()
            if len(p)==3:
                try: d[p[0]]=(float(p[1]), float(p[2]))
                except: pass
    except FileNotFoundError: pass
    return d
def load(f):
    d={}
    try:
        for l in open(f):
            p=l.split()
            if len(p)==2:
                try: d[p[0]]=float(p[1])
                except: pass
    except FileNotFoundError: pass
    return d
rx,tx,rp,tp,ct = (load(f) for f in
    ("/tmp/.nn_rx","/tmp/.nn_tx","/tmp/.nn_rp","/tmp/.nn_tp","/tmp/.nn_ct"))
B = load2("/tmp/.pa_req_base"); N = load2("/tmp/.pa_req_now")
def dlt(n,idx):
    a = N.get(n,(0,0))[idx]; b = B.get(n,(0,0))[idx]
    return a-b if a>b else 0

def grp(n):
    if n.startswith(("master","infra","zookeeper")): return 0
    if n.startswith("kafbroker"):                    return 2
    return 1
GROUPS=[("MASTER & INFRA NODES  (master / infra / zookeeper)",0),
        ("WORKER NODES          (worker / gemfirecache)  <- APP TRAFFIC",1),
        ("KAFKA NODES           (kafbroker)",2)]

allnodes=set(rx)|set(tx)|set(N)
rows_by_g={0:[],1:[],2:[]}
with open(csv,"a") as f:
    for n in allnodes:
        a=rx.get(n,0)/1e6*8; b=tx.get(n,0)/1e6*8
        pr=rp.get(n,0); pt=tp.get(n,0); c=ct.get(n,0)
        fg=dlt(n,0); al=dlt(n,1)
        rows_by_g[grp(n)].append((n,a,b,pr,pt,c,fg,al))
        f.write(f"{ts},{n},{a:.2f},{b:.2f},{pr:.0f},{pt:.0f},{c:.0f},{fg:.0f},{al:.0f}\n")

HDR=(f"{'NODE':<16}{'RX Mbps':>9}{'TX Mbps':>9}{'RX pkt/s':>9}{'TX pkt/s':>9}"
     f"{'conntrk':>8}{'FROM GW':>11}{'ALL RCVD':>11}")
W=len(HDR)
G_rx=G_tx=G_fg=G_al=0.0
for title,gi in GROUPS:
    rws=sorted(rows_by_g[gi], key=lambda r:-(r[6]*1e6 + r[1]+r[2]))
    if not rws: continue
    srx=sum(r[1] for r in rws); stx=sum(r[2] for r in rws)
    sfg=sum(r[6] for r in rws); sal=sum(r[7] for r in rws)
    G_rx+=srx; G_tx+=stx; G_fg+=sfg; G_al+=sal
    print(f"\n  {title}")
    print("  "+HDR)
    print("  "+"-"*W)
    for n,a,b,pr,pt,ck,fg,al in rws:
        flag=" !" if ck>100000 else ""
        c1 = f"{fg:11.0f}" if fg>0 else f"{'-':>11}"
        c2 = f"{al:11.0f}" if al>0 else f"{'-':>11}"
        print(f"  {n:<16}{a:9.1f}{b:9.1f}{pr:9.0f}{pt:9.0f}{ck:8.0f}{c1}{c2}{flag}")
    print("  "+"-"*W)
    print(f"  {'GROUP SUBTOTAL':<16}{srx:9.1f}{stx:9.1f}{'':9}{'':9}{'':8}{sfg:11.0f}{sal:11.0f}   ({len(rws)} nodes)")
print("\n  "+"="*W)
print(f"  {'CLUSTER TOTAL':<16}{G_rx:9.1f}{G_tx:9.1f}{'':9}{'':9}{'':8}{G_fg:11.0f}{G_al:11.0f}")
print(f"  {'':16}{'Mbps in':>9}{'Mbps out':>9}{'':9}{'':9}{'':8}{'= gateway':>11}{'incl. e-w':>11}")
PY
)
  if [ "$QUIET" -eq 1 ]; then
    printf "%s  elapsed %5ss  cycle %3ss  %9s req/s  total %s\n" \
      "$TS" "$EL" "$GAP" "$RPS" "$GW_SINCE"
    continue
  fi
  clear
  echo "=================================================================================="
  echo "   PERF TRAFFIC — $TS IST    elapsed ${EL}s    cycle ${GAP}s (asked ${INT}s)    tag=$TAG"
  echo "   window start $START_IST IST"
  echo "=================================================================================="
  echo
  echo "  [1] GATEWAY INGRESS  —  AVLB -> OCP   ($NGW gateway pods)"
  echo "  ----------------------------------------------------------------------------------"
  printf "        CURRENT : %s req/s        AVERAGE : %s req/s\n" "$RPS" "$AVG"
  printf "        TOTAL   : %s requests into the cluster\n\n" "$GW_SINCE"
  printf "$GWLINES"
  echo
  echo "  [2] NODE-LEVEL NETWORK + REQUESTS"
  echo "      RX = data IN   TX = data OUT   conntrk = tracked conns (! if >100k)"
  echo "      FROM GW = first-hop requests (reconciles with [1])   ALL RCVD = incl. east-west"
  echo "$OUT"
  echo "=================================================================================="
  printf "   Ctrl-C for window summary   |   CSV: %s , %s\n" "$GCSV" "$NCSV"
done
