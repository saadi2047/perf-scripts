# 5-Minute Test Guide — PERF Toolkit
**Saadi M Muzzammil**

Quick smoke test to confirm all scripts work before a real load run.

---

## Step 0 — Setup (once)

```bash
cd /home/osuser/saadi/M/Muzzammil/perf-testing
chmod +x perf-run.sh perf-capacity.sh perf-infra.sh perf-app.sh
oc whoami          # must be logged in
```

**Confirm all 10 files are the latest versions** (the last test failed because an old
`perf-app.sh` was still there):
```bash
ls -lrt *.sh *.py
```
Check these 4 are present and recent: `perf-app.sh`, `perf-capacity.sh`,
`perf_report.py`, and the new `alloc_parse.py`.

---

## Step 1 — Run the 5-minute test

```bash
./perf-run.sh before
./perf-run.sh during -i 30 -d 300      # 5 min, 10 samples
./perf-run.sh after
./perf-run.sh report
```

- `during` blocks for ~5 min — that's normal. You'll see `sample 1/10 ... done` lines.
- **Expect zero syntax errors** this time. If you see any, the old files weren't replaced.

---

## Step 2 — Verify the output

```bash
cd perf-test-*/                          # enter the run folder

# all pieces present
ls -d perf_capacity_* perf_infra_* perf_app_* PERF_Report_*.html

# key CSVs populated (not just headers)
wc -l perf_infra_*/node_usage.csv                # ~120+ rows (12 nodes x 10 samples)
cat  perf_infra_*/node_trends.csv                # per-node avg/peak
cat  perf_infra_*/findings.txt                   # plain-language findings
cat  perf_capacity_before_*/resource_alloc.csv   # NEW: request/limit x replicas
head perf_capacity_before_*/hpa_state.csv        # HPA min/max/current
```

Then **open** `PERF_Report_*.html` in a browser.

---

## Step 3 — Check the report has all 6 sections

1. **Summary verdict** (top)
2. **What changed** — capacity before/after
3. **What happened during** — per-service CPU/mem trends
4. **Node utilization** — per app-worker node (before/avg/peak/after)
5. **HPA per namespace** — min/max/current (no Desired column)
6. **Resource allocation** — request/limit x current & max replicas, human units (cores/GB)

Masthead should show **Dynatrace** + **OCP Console** links and title **"PERF Test Report"**.

---

## What's normal at idle (no load running)

- Findings say "no significant CPU swings / no scheduling pressure" — correct, not a failure.
- App log section sparse or empty — normal without load (few errors/503s).
- CPU numbers low (single-digit %) — the cluster is idle.

The point of the 5-min test is to confirm **the pipeline runs and the HTML builds**,
not to see load behaviour.

---

## If something looks wrong

Zip the run folder and share it:
```bash
cd /home/osuser/saadi/M/Muzzammil/perf-testing
zip -r run5min.zip perf-test-*/
```

---
© Saadi M Muzzammil
