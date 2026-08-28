#!/usr/bin/env sh
# ============================================================================
# perf-run.sh — one entry point for the whole PERF load-test capture
# SBI ePay 2.0 PERF · Saadi M Muzzammil
# ----------------------------------------------------------------------------
# All artifacts for one test cycle land in a single run folder:
#     perf-test-<date_time>/
#        perf_capacity_before_.../   perf_infra_.../   perf_app_.../
#        perf_capacity_after_.../     PERF_Report_....html
#
#   ./perf-run.sh before      # creates the run folder + capacity snapshot
#   ./perf-run.sh during      # infra + app collectors (into the same folder)
#   ./perf-run.sh after       # capacity snapshot after load
#   ./perf-run.sh report      # HTML from this run folder
#   ./perf-run.sh new         # force a brand-new run folder (start fresh)
#
# The active run folder is remembered in .perf_current_run so the 4 commands
# (run minutes apart) all share one folder. Options: -i INTERVAL, -d DURATION.
# ============================================================================
set -u
CMD="${1:-help}"; shift 2>/dev/null || true
INTERVAL=60; DURATION=3600
BASE="$(pwd)"
MARKER="$BASE/.perf_current_run"
while [ $# -gt 0 ]; do case "$1" in
  -i) INTERVAL="$2"; shift 2;; -d) DURATION="$2"; shift 2;; *) shift;; esac; done
D=$(dirname "$0")

new_run() {
  RUN="$BASE/perf-test-$(date +%Y%m%d_%H%M%S)"
  mkdir -p "$RUN"
  echo "$RUN" > "$MARKER"
  echo "$RUN"
}
current_run() {
  if [ -f "$MARKER" ]; then cat "$MARKER"; else new_run; fi
}
newest() { for d in $(ls -dt "$1"/$2 2>/dev/null); do [ -d "$d" ] && { echo "$d"; return; }; done; }

case "$CMD" in
  before)
    RUN=$(new_run)                       # 'before' always starts a fresh cycle
    echo "[*] NEW run folder: $RUN"
    sh "$D/perf-capacity.sh" --phase before -o "$RUN"
    ;;
  new)
    RUN=$(new_run); echo "[*] NEW run folder ready: $RUN"
    ;;
  during)
    RUN=$(current_run); echo "[*] run folder: $RUN"
    echo "[*] launching infra + app collectors in parallel for $((DURATION/60)) min..."
    sh "$D/perf-infra.sh" -i "$INTERVAL" -d "$DURATION" -o "$RUN" & IP=$!
    sh "$D/perf-app.sh"   -i "$INTERVAL" -d "$DURATION" -o "$RUN" & AP=$!
    wait "$IP"; wait "$AP"
    echo "[✓] both collectors finished"
    ;;
  after)
    RUN=$(current_run); echo "[*] run folder: $RUN"
    sh "$D/perf-capacity.sh" --phase after -o "$RUN"
    ;;
  report)
    RUN=$(current_run); echo "[*] run folder: $RUN"
    B=$(newest "$RUN" 'perf_capacity_before_*'); A=$(newest "$RUN" 'perf_capacity_after_*')
    I=$(newest "$RUN" 'perf_infra_*'); AP=$(newest "$RUN" 'perf_app_*')
    echo "    before=$B"; echo "    after=$A"; echo "    infra=$I"; echo "    app=$AP"
    OUT="$RUN/PERF_Report_$(date +%Y%m%d_%H%M%S).html"
    python3 "$D/perf_report.py" ${B:+--before "$B"} ${A:+--after "$A"} ${I:+--infra "$I"} ${AP:+--app "$AP"} -o "$OUT"
    echo "[✓] report: $OUT"
    ;;
  *) grep '^#' "$0" | sed 's/^# \{0,1\}//' ;;
esac
