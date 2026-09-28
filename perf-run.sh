#!/usr/bin/env sh
# ============================================================================
# perf-run.sh — one entry point for the whole PERF load-test capture
# SBI ePay 2.0 PERF · Saadi M Muzzammil
# ----------------------------------------------------------------------------
# All artifacts for one test cycle land in a single run folder:
#     perf-test-<date_time>/
#        perf_capacity_before_.../   perf_infra_.../   perf_app_.../
#        traffic_<tag>.csv           nodenet_<tag>.csv
#        perf_capacity_after_.../    PERF_Report_....html   NET_Report_....html
#
#   ./perf-run.sh before      # creates the run folder + capacity snapshot
#   ./perf-run.sh during      # asks for interval + duration, then runs all three collectors
#   ./perf-run.sh after       # capacity snapshot after load
#   ./perf-run.sh report      # BOTH HTML reports from this run folder
#   ./perf-run.sh new         # force a brand-new run folder (start fresh)
#
# The active run folder is remembered in .perf_current_run so the 4 commands
# (run minutes apart) all share one folder.
#
# Options:  -n SECONDS   network sampling interval (10-30, default 10)
#           -d SECONDS   load duration, 60 .. 86400 (24 h)   default 3600
#           -n SECONDS   network sampling interval, 10 .. 30   default 10
#           -i SECONDS   infra/app pod sampling interval, 30 .. 600   default 60
# `during` prompts for each of these when it was not supplied and a terminal is
# present; supplying a flag skips its prompt, so a scripted run never blocks.
# A duration over one hour asks for confirmation before it starts.
# ============================================================================
set -u
CMD="${1:-help}"; shift 2>/dev/null || true
INTERVAL=60          # infra + app sampling cadence
NETINT=10            # network sampling cadence (perf-all.sh)
DURATION=3600
GOT_NETINT=0; GOT_DUR=0; GOT_INT=0
BASE="$(pwd)"
NETTAG="$(date +%H%M)"
MARKER="$BASE/.perf_current_run"
while [ $# -gt 0 ]; do case "$1" in
  -i) INTERVAL="$2"; GOT_INT=1;    shift 2;;
  -n) NETINT="$2";   GOT_NETINT=1; shift 2;;
  -d) DURATION="$2"; GOT_DUR=1;    shift 2;;
  *) shift;; esac; done
D=$(dirname "$0")

# ---------------------------------------------------------------- prompts ---
# `during` asks for each setting it was not given, when there is a terminal to
# ask on. Any flag supplied on the command line skips its prompt, so a scripted
# or scheduled run never blocks.
MAXDUR=86400      # 24 hours

fmt_dur() {   # seconds -> "45 min" / "2.5 hours"
  awk -v d="$1" 'BEGIN{ if (d<3600) printf "%.0f min", d/60; else printf "%.1f hours", d/3600 }'
}

ask_yn() {    # $1 question -> 0 = yes, 1 = no.  Empty answer means yes.
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

ask_int() {   # $1 label  $2 min  $3 max  $4 default  -> echoes the answer
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

new_run() {
  RUN="$BASE/perf-test-$(date +%Y%m%d_%H%M%S)"
  mkdir -p "$RUN"
  echo "$RUN" > "$MARKER"
  echo "$RUN"
}
current_run() {
  if [ -f "$MARKER" ]; then cat "$MARKER"; else new_run; fi
}
# newest matching directory, by name (the names are timestamped, so name order ==
# time order). Globs, not `ls` output, so a path with a space can't split.
newest() {
  # names are timestamped, so lexical order == chronological order.
  # sort|tail avoids both `ls` word-splitting and the non-POSIX `\>` test.
  for d in "$1"/$2; do [ -d "$d" ] && echo "$d"; done | sort | tail -1
}

# A value given on the command line skips its prompt, so it has to be range-checked
# here or a typo like `-d 999999` would quietly run for eleven days.
chk() {   # $1 flag  $2 value  $3 min  $4 max
  if ! echo "$2" | grep -qE '^[0-9]+$' || [ "$2" -lt "$3" ] || [ "$2" -gt "$4" ]; then
    echo "ERROR: $1 must be a whole number between $3 and $4 (got '$2')"; exit 1
  fi
}
[ "$GOT_NETINT" -eq 1 ] && chk -n "$NETINT"   10 30
[ "$GOT_INT"    -eq 1 ] && chk -i "$INTERVAL" 30 600
[ "$GOT_DUR"    -eq 1 ] && chk -d "$DURATION" 60 "$MAXDUR"

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

    if [ -t 0 ]; then
      echo
      echo "   --------------------------------------------------------------"
      echo "   CAPTURE SETTINGS"
      echo "   --------------------------------------------------------------"
      [ "$GOT_NETINT" -eq 0 ] && NETINT=$(ask_int \
        "1. Network sampling interval, seconds (10-30, recommended 10)" 10 30 10)
      if [ "$GOT_INT" -eq 0 ]; then
        INTERVAL=$(ask_int \
          "2. Pod (infra + app) sampling interval, seconds (30-600, recommended 60)" 30 600 60)
        if [ "$INTERVAL" -lt 45 ]; then
          echo "      note: one full pod sample takes 30-40s across 11 namespaces, so a"
          echo "            cycle under 45s will overrun and samples will land further"
          echo "            apart than you asked for. 60s is the safe cadence."
        fi
      fi
      if [ "$GOT_DUR" -eq 0 ]; then
        while :; do
          DURATION=$(ask_int \
            "3. Load duration, seconds (60-86400 = up to 24h; 300=5m, 1800=30m, 7200=2h)" \
            60 "$MAXDUR" 3600)
          [ "$DURATION" -le 3600 ] && break
          printf "      that is %s — longer than an hour.\n" "$(fmt_dur "$DURATION")" >&2
          ask_yn "Run the capture for that long?" && break
        done
      fi
      echo "   --------------------------------------------------------------"
    elif [ "$GOT_NETINT" -eq 0 ] || [ "$GOT_DUR" -eq 0 ] || [ "$GOT_INT" -eq 0 ]; then
      echo "[*] no terminal to prompt on — using defaults (-n $NETINT, -i $INTERVAL, -d $DURATION)"
    fi

    # The infra collector execs `oc adm top` across every namespace each cycle and
    # a full sample takes 30-40s, so it keeps its own slower cadence. Forcing it to
    # the network interval would make every cycle overrun. Shown, never hidden.
    echo
    echo "[*] plan for this window:"
    printf "      duration        : %ss (%s)\n" "$DURATION" "$(fmt_dur "$DURATION")"
    printf "      network capture : every %ss   -> traffic_%s.csv + nodenet_%s.csv\n" \
      "$NETINT" "$NETTAG" "$NETTAG"
    printf "      pods infra+app  : every %ss\n" "$INTERVAL"
    printf "      output folder   : %s\n" "$RUN"
    echo "      start your JMeter load now."
    echo

    sh "$D/perf-infra.sh" -i "$INTERVAL" -d "$DURATION" -o "$RUN" 2>&1 | sed 's/^/[infra] /' & IP=$!
    sh "$D/perf-app.sh"   -i "$INTERVAL" -d "$DURATION" -o "$RUN" 2>&1 | sed 's/^/[app]   /' & AP=$!
    # network capture writes straight into the run folder and stops itself, so the
    # CSVs can never end up in the wrong directory or need a Ctrl-C to close.
    NP=""
    if [ -f "$D/perf-all.sh" ]; then
      sh "$D/perf-all.sh" "$NETINT" "$NETTAG" -o "$RUN" -d "$DURATION" 2>&1 | sed 's/^/[net]   /' & NP=$!
    else
      echo "[!] perf-all.sh not found next to perf-run.sh — network capture skipped"
    fi
    wait "$IP"; wait "$AP"; [ -n "$NP" ] && wait "$NP"
    echo "[✓] all collectors finished"
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
    STAMP=$(date +%Y%m%d_%H%M%S)
    OUT="$RUN/PERF_Report_$STAMP.html"
    python3 "$D/perf_report.py" ${B:+--before "$B"} ${A:+--after "$A"} ${I:+--infra "$I"} ${AP:+--app "$AP"} -o "$OUT"
    echo "[✓] PERF report   : $OUT"

    # ---- network report from the traffic/nodenet CSVs ----
    # adopt any CSVs left in the toolkit directory by a manual perf-all.sh run
    for f in "$BASE"/traffic_*.csv; do
      [ -e "$f" ] || continue
      t=$(basename "$f" .csv); t=${t#traffic_}
      [ -e "$BASE/nodenet_$t.csv" ] || continue
      [ -e "$RUN/traffic_$t.csv" ] && continue
      echo "[*] adopting loose window '$t' from $BASE into the run folder"
      mv "$f" "$BASE/nodenet_$t.csv" "$RUN"/ 2>/dev/null
    done
    if ls "$RUN"/traffic_*.csv >/dev/null 2>&1; then
      NOUT="$RUN/NET_Report_$STAMP.html"
      if python3 "$D/net_report.py" --dir "$RUN" -o "$NOUT"; then
        echo "[✓] NETWORK report: $NOUT"
      else
        rc=$?
        if [ "$rc" -eq 2 ]; then
          echo "[!] network report skipped — the capture held no usable window (see above)."
        else
          echo "[!] network report FAILED (exit $rc) — the PERF report above is still valid."
        fi
      fi
    else
      echo "[!] no traffic_*.csv found — network report skipped."
      echo "    Capture one with:  ./perf-run.sh during   (it now runs perf-all.sh too)"
      echo "    or manually:       ./perf-all.sh 10 w1 -o $RUN"
    fi
    ;;
  *) grep '^#' "$0" | sed 's/^# \{0,1\}//' ;;
esac
