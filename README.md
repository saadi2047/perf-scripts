# PERF Test Toolkit — SBI ePay 2.0
**Prepared by Saadi M Muzzammil**

A read-only toolkit to capture OpenShift capacity, infra, and app behaviour during a
PERF load test, and render one plain-language HTML report.

---

## Files (10 total — keep all in one directory)

**Scripts (make executable):**
| File | Role |
|---|---|
| `perf-run.sh` | Orchestrator — the only script you call directly |
| `perf-capacity.sh` | Node + pod capacity snapshot (before / after) |
| `perf-infra.sh` | Continuous infra collector (per-pod + per-node usage) |
| `perf-app.sh` | Continuous filtered-log collector |

**Parsers (no chmod needed — called by the scripts):**
`capacity_parse.py`, `cap_extras.py`, `alloc_parse.py`,
`infra_parse.py`, `app_parse.py`, `perf_report.py`

> All 10 must sit in the **same directory**. The scripts call the parsers by path.

---

## One-time setup

```bash
cd /home/osuser/saadi/M/Muzzammil/perf-testing
chmod +x perf-run.sh perf-capacity.sh perf-infra.sh perf-app.sh
oc whoami          # must be logged in
```

---

## Running a test — the 4 commands

Everything for one test lands in a single folder: `perf-test-<date_time>/`

```bash
./perf-run.sh before      # 1) snapshot BEFORE load (creates the run folder)
./perf-run.sh during      # 2) infra + app collectors — RUN AT LOAD START
./perf-run.sh after       # 3) snapshot AFTER load
./perf-run.sh report      # 4) build the HTML report
```

`during` runs infra + app in parallel and **blocks** until the duration ends.
Start it right when the load test begins.

---

## Duration & interval — match the collector to the load length

`during` defaults to **60 min at 60s interval**. Override with `-d` (duration, seconds)
and `-i` (interval, seconds).

| Load length | Command | Samples |
|---|---|---|
| **5 min** (smoke test) | `./perf-run.sh during -i 30 -d 300` | 10 |
| **30 min** | `./perf-run.sh during -i 60 -d 1800` | 30 |
| **1 hour** (default) | `./perf-run.sh during` | 60 |
| **1 hour** (explicit) | `./perf-run.sh during -i 60 -d 3600` | 60 |
| **2 hours** | `./perf-run.sh during -i 60 -d 7200` | 120 |
| **2 hours (lighter)** | `./perf-run.sh during -i 120 -d 7200` | 60 |

`-d` is in **seconds**: 300=5min, 1800=30min, 3600=1hr, 7200=2hr, 10800=3hr.
Interval 60s is a good default; 120s halves the sample count and zip size for long runs.

### Full 2-hour run — copy/paste
```bash
cd /home/osuser/saadi/M/Muzzammil/perf-testing
./perf-run.sh before
./perf-run.sh during -i 60 -d 7200      # 2 hours, 120 samples — run at load start
./perf-run.sh after
./perf-run.sh report
```

> Tip for long runs: `during` blocks the terminal for the whole duration. Run it in
> the background so a dropped SSH session doesn't kill it:
> ```bash
> nohup ./perf-run.sh during -i 60 -d 7200 > during.log 2>&1 &
> tail -f during.log      # watch progress
> ```

---

## Output

```
perf-test-<date_time>/
├── perf_capacity_before_.../   + .zip
├── perf_infra_.../             + .zip   (per-sample data, trends, node usage)
├── perf_app_.../               + .zip   (filtered logs)
├── perf_capacity_after_.../    + .zip
└── PERF_Report_<time>.html     ← open this
```

The HTML has 6 sections: **verdict, capacity deltas, hour trends, node utilization,
HPA per namespace, resource allocation** — plain language, human units (cores / GB),
with Dynatrace + OCP Console links at the top.

---

## Report sections

1. **What changed** — reserved CPU, Pending, fit-check before vs after
2. **What happened during the hour** — per-service CPU/mem swings, restarts, scheduling
   (real CPU pressure separated from affinity/PVC/taint noise)
3. **Node utilization** — per app-worker node: before / during-avg / during-peak / after
4. **HPA per namespace** — min/max/current, with repo-drift flags
5. **Resource allocation** — request & limit per pod, × current and × max replicas,
   with fleet totals vs the 186-core app-worker pool

---

## Notes

- **All read-only.** Nothing is changed on the cluster.
- Requires `oc` (logged in), `python3`, `zip` — all present on the bastion.
- **App metrics** (Hikari pool, GC, controller latency) come from **Dynatrace**, not these
  scripts. `perf-app.sh` captures logs (errors, exceptions, 503, GC lines) only.
- `before` always starts a fresh run folder; `during`/`after`/`report` reuse it (tracked
  in `.perf_current_run`). To force a new folder without a snapshot: `./perf-run.sh new`.
- The **resource allocation** section needs the pod specs captured by `perf-capacity.sh`,
  so run a full fresh cycle (not just `report` on an old run) to populate it.
- App-worker pool = **12 nodes / 186 allocatable cores** (7 worker + 5 gemfire, gemfire
  used as regular workers).

---
© Saadi M Muzzammil
