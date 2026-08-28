#!/usr/bin/env sh
# ============================================================================
# perf-capacity.sh — cluster capacity snapshot (run BEFORE and AFTER load)
# SBI ePay 2.0 PERF — case 04437672
# ----------------------------------------------------------------------------
# Read-only. POSIX sh. Requires: oc (logged in), python3, zip.
# Produces a timestamped dir + zip with per-node reserved/used/allocatable,
# node classification (app-worker vs gemfire vs infra/kafka/zk/master),
# Pending pods, live HPA state, and per-namespace request totals.
#
# Usage:
#   ./perf-capacity.sh --phase before        # before load
#   ./perf-capacity.sh --phase after         # after load
#   ./perf-capacity.sh --phase before -o /path/to/output
#
# The two phases produce diffable output the report builder pairs up.
# ============================================================================
set -u

PHASE="snapshot"
OUTBASE="."
NAMESPACES="perf-transaction perf-admin perf-kms perf-merchant perf-simulators perf-notification perf-epay-common"

while [ $# -gt 0 ]; do
  case "$1" in
    --phase) PHASE="$2"; shift 2 ;;
    -o) OUTBASE="$2"; shift 2 ;;
    -n) NAMESPACES="$2"; shift 2 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown arg: $1"; exit 1 ;;
  esac
done

command -v oc >/dev/null 2>&1 || { echo "FATAL: oc not found"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "FATAL: python3 not found"; exit 1; }
oc whoami >/dev/null 2>&1 || { echo "FATAL: not logged in to oc"; exit 1; }

TS=$(date +%Y%m%d_%H%M%S)
OUTDIR="$OUTBASE/perf_capacity_${PHASE}_${TS}"
mkdir -p "$OUTDIR/raw"
echo "[*] perf-capacity.sh  phase=$PHASE  out=$OUTDIR"

# ---- 1. Raw captures (the micro-detail, kept verbatim) ----
echo "[*] capturing raw node + pod data..."
oc get nodes -o json > "$OUTDIR/raw/nodes.json" 2>/dev/null
oc get nodes --show-labels > "$OUTDIR/raw/nodes_labels.txt" 2>/dev/null
oc adm top nodes > "$OUTDIR/raw/top_nodes.txt" 2>/dev/null
oc describe nodes > "$OUTDIR/raw/describe_nodes.txt" 2>/dev/null
oc get pods -A -o wide > "$OUTDIR/raw/pods_all.txt" 2>/dev/null
oc get hpa -A > "$OUTDIR/raw/hpa_all.txt" 2>/dev/null
oc get hpa -A -o json > "$OUTDIR/raw/hpa_all.json" 2>/dev/null

# per-pod resource requests/limits for the perf namespaces (for allocation section)
for ns in $NAMESPACES; do
  oc -n "$ns" get pods -o json > "$OUTDIR/raw/pods_${ns}.json" 2>/dev/null
done

echo "[*] raw capture done. (parser + HTML in next build step)"
echo "[*] OUTDIR=$OUTDIR"

# ---- 2. Parse: node classification + capacity, HPA drift, pending ----
SCRIPT_DIR=$(dirname "$0")
echo "[*] parsing capacity..."
python3 "$SCRIPT_DIR/capacity_parse.py" "$OUTDIR" 2>&1 | sed 's/^/    /'
python3 "$SCRIPT_DIR/cap_extras.py" "$OUTDIR" 2>&1 | sed 's/^/    /'
python3 "$SCRIPT_DIR/alloc_parse.py" "$OUTDIR" 2>&1 | sed 's/^/    /'

# ---- 3. Write a phase manifest (so report builder can pair before/after) ----
cat > "$OUTDIR/manifest.txt" << MANIFEST
phase=$PHASE
timestamp=$TS
captured_by=perf-capacity.sh
oc_user=$(oc whoami 2>/dev/null)
cluster=$(oc whoami --show-server 2>/dev/null)
MANIFEST

# ---- 4. Zip ----
echo "[*] packaging..."
ZIPNAME="perf_capacity_${PHASE}_${TS}.zip"
( cd "$OUTBASE" && zip -qr "$ZIPNAME" "perf_capacity_${PHASE}_${TS}" )
echo "[✓] DONE"
echo "    dir: $OUTDIR"
echo "    zip: $OUTBASE/$ZIPNAME"
echo ""
echo "    Key outputs:"
echo "      node_capacity.csv      — per-node alloc/reserved/free, classified"
echo "      capacity_by_class.csv  — app-worker vs gemfire vs infra (the 112c truth)"
echo "      fit_check.txt          — can a 3000m txn pod be placed?"
echo "      hpa_state.csv          — live min/max/current (repo-drift check)"
echo "      pending_pods.csv       — anything Pending at snapshot"
