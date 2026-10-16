#!/usr/bin/env python3
# ============================================================================
# kafka_summary.py — one flat CSV per run, for Excel
# SBI ePay 2.0 PERF · Saadi M Muzzammil
# ----------------------------------------------------------------------------
#   python3 kafka_summary.py --dir <run folder> [--tag TAG]
#
# The run already leaves nine per-sample CSVs. Answering "how did broker-2
# behave?" or "did any topic reject writes?" from those means joining several
# files by pod or topic. This collapses the window into one row per entity so
# the question is answerable by opening a single file.
#
# Every figure is recomputed from the per-sample CSVs. A metric the exporter
# never published stays empty — never zero.
# ============================================================================
import os, csv, sys, argparse
from collections import defaultdict


def read(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="") as f:
        return [r for r in csv.DictReader(f) if r]


def nums(rows, col):
    out = []
    for r in rows:
        v = (r.get(col) or "").strip()
        if not v:
            continue
        try:
            out.append(float(v))
        except ValueError:
            pass
    return out


def agg(rows, col, how):
    v = nums(rows, col)
    if not v:
        return ""
    if how == "max":
        return f"{max(v):.3f}".rstrip("0").rstrip(".")
    if how == "min":
        return f"{min(v):.3f}".rstrip("0").rstrip(".")
    if how == "avg":
        return f"{sum(v)/len(v):.3f}".rstrip("0").rstrip(".")
    if how == "last":
        return f"{v[-1]:.3f}".rstrip("0").rstrip(".")
    return ""


def group(rows, key):
    g = defaultdict(list)
    for r in rows:
        g[r.get(key, "")].append(r)
    return g


def main():
    ap = argparse.ArgumentParser(description="flat per-run summary CSV")
    ap.add_argument("--dir", required=True)
    ap.add_argument("--tag", default="")
    ap.add_argument("-o", default="")
    a = ap.parse_args()

    tag = a.tag
    if not tag:
        for f in sorted(os.listdir(a.dir)):
            if f.startswith("kafka_cluster_") and f.endswith(".csv"):
                tag = f[len("kafka_cluster_"):-4]
                break
    if not tag:
        print("kafka_summary: no kafka_cluster_<tag>.csv found", file=sys.stderr)
        return 2

    g = lambda n: read(os.path.join(a.dir, f"kafka_{n}_{tag}.csv"))
    clu, brk, top = g("cluster"), g("broker"), g("topic")
    pod, jvm, dsk, lag = g("pod"), g("jvm"), g("disk"), g("lag")
    off = g("offsets")

    # messages actually written, from the brokers' own partition offsets
    produced = {}
    byt = defaultdict(lambda: defaultdict(float))
    for r in off:
        t = r.get("topic", "")
        try:
            e, v = float(r["elapsed_s"]), float(r["end_offset"])
        except (KeyError, ValueError, TypeError):
            continue
        if t:
            byt[t][e] += v
    for t, m in byt.items():
        els = sorted(m)
        if len(els) >= 2 and m[els[-1]] - m[els[0]] >= 0:
            produced[t] = (m[els[-1]] - m[els[0]], els[-1] - els[0])

    info = {}
    ri = os.path.join(a.dir, "run_info.txt")
    if os.path.exists(ri):
        with open(ri) as fh:
            for ln in fh:
                if "=" in ln:
                    k, v = ln.split("=", 1)
                    info[k.strip()] = v.strip()

    start = clu[0]["ts_ist"] if clu else info.get("started_ist", "")
    end = clu[-1]["ts_ist"] if clu else ""
    dur = (clu[-1].get("elapsed_s") or "") if clu else ""
    nsmp = len(clu)

    out = a.o or os.path.join(a.dir, f"kafka_summary_{tag}.csv")
    rows = []

    def row(section, entity, **kw):
        d = {"section": section, "entity": entity,
             "window_start_ist": start, "window_end_ist": end,
             "window_seconds": dur, "samples": nsmp}
        d.update(kw)
        rows.append(d)

    # ---- cluster -----------------------------------------------------------
    if clu:
        row("cluster", "ALL",
            mb_in_peak=agg(clu, "bytes_in_mb_s", "max"),
            mb_in_avg=agg(clu, "bytes_in_mb_s", "avg"),
            mb_out_peak=agg(clu, "bytes_out_mb_s", "max"),
            msg_s_peak=agg(clu, "messages_in_s", "max"),
            msg_s_avg=agg(clu, "messages_in_s", "avg"),
            under_replicated_peak=agg(clu, "under_replicated", "max"),
            offline_partitions_peak=agg(clu, "offline_partitions", "max"),
            active_controllers=agg(clu, "active_controllers", "last"),
            partitions_replicas=agg(clu, "partitions_total", "last"),
            partition_leaders=agg(clu, "leaders_total", "last"))

    # ---- per broker --------------------------------------------------------
    for b, rs in sorted(group(brk, "pod").items()):
        if not b:
            continue
        row("broker", b,
            mb_in_peak=agg(rs, "bytes_in_mb_s", "max"),
            mb_out_peak=agg(rs, "bytes_out_mb_s", "max"),
            msg_s_peak=agg(rs, "messages_in_s", "max"),
            handler_idle_min=agg(rs, "handler_idle_pct", "min"),
            handler_idle_avg=agg(rs, "handler_idle_pct", "avg"),
            net_idle_min=agg(rs, "network_idle_pct", "min"),
            request_queue_peak=agg(rs, "request_queue", "max"),
            purgatory_fetch_peak=agg(rs, "purgatory", "max"),
            purgatory_produce_peak=agg(rs, "purgatory_produce", "max"),
            under_replicated_peak=agg(rs, "under_replicated", "max"),
            at_min_isr_peak=agg(rs, "at_min_isr", "max"),
            partitions_replicas=agg(rs, "partitions", "last"),
            partition_leaders=agg(rs, "leaders", "last"))

    # ---- per pod: resources (what the window actually did to them) --------
    jv = group(jvm, "pod")
    for q, rs in sorted(group(pod, "pod").items()):
        if not q:
            continue
        row("pod", q,
            role=rs[-1].get("role", ""), node=rs[-1].get("node", ""),
            ready=rs[-1].get("ready", ""),
            cpu_m_avg=agg(rs, "cpu_m", "avg"), cpu_m_peak=agg(rs, "cpu_m", "max"),
            mem_mi_avg=agg(rs, "mem_mi", "avg"), mem_mi_peak=agg(rs, "mem_mi", "max"),
            heap_mi_peak=agg(jv.get(q, []), "heap_used_mb", "max"),
            gc_per_s_peak=agg(jv.get(q, []), "gc_count_s", "max"),
            restarts_at_start=agg(rs, "restarts", "min"),
            restarts_at_end=agg(rs, "restarts", "last"))

    # ---- per topic: throughput AND request outcomes -----------------------
    for t, rs in sorted(group(top, "topic").items()):
        if not t:
            continue
        row("topic", t,
            mb_in_peak=agg(rs, "bytes_in_mb_s", "max"),
            mb_in_avg=agg(rs, "bytes_in_mb_s", "avg"),
            mb_out_peak=agg(rs, "bytes_out_mb_s", "max"),
            msg_s_peak=agg(rs, "messages_in_s", "max"),
            msg_s_avg=agg(rs, "messages_in_s", "avg"),
            messages_from_offsets=(f"{produced[t][0]:.0f}" if t in produced else ""),
            offset_span_s=(f"{produced[t][1]:.0f}" if t in produced else ""),
            produce_ok_s_avg=agg(rs, "produce_ok_s", "avg"),
            produce_fail_s_peak=agg(rs, "produce_fail_s", "max"),
            fetch_ok_s_avg=agg(rs, "fetch_ok_s", "avg"),
            fetch_fail_s_peak=agg(rs, "fetch_fail_s", "max"))

    # ---- per consumer group / topic ---------------------------------------
    pairs = defaultdict(list)
    for r in lag:
        pairs[(r.get("group", ""), r.get("topic", ""))].append(r)
    for (grp, t), rs in sorted(pairs.items()):
        if not grp:
            continue
        row("consumer_group", f"{grp} / {t}",
            lag_peak=agg(rs, "lag", "max"), lag_final=agg(rs, "lag", "last"),
            members=rs[-1].get("members", ""))

    # ---- per PVC -----------------------------------------------------------
    for v, rs in sorted(group(dsk, "pvc").items()):
        if not v:
            continue
        u = nums(rs, "used_gb")
        row("storage", v,
            pod=rs[-1].get("pod", ""),
            used_gb_peak=agg(rs, "used_gb", "max"),
            capacity_gb=agg(rs, "capacity_gb", "last"),
            used_pct_peak=agg(rs, "used_pct", "max"),
            grew_gb=(f"{u[-1]-u[0]:.3f}".rstrip("0").rstrip(".") if len(u) > 1 else ""))

    # stable column order: the shared keys first, then whatever the sections added
    lead = ["section", "entity", "window_start_ist", "window_end_ist",
            "window_seconds", "samples"]
    rest = []
    for r in rows:
        for k in r:
            if k not in lead and k not in rest:
                rest.append(k)
    cols = lead + rest
    with open(out, "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        wr.writeheader()
        for r in rows:
            wr.writerow(r)
    print(f"[kafka-summary] wrote {out}  ({len(rows)} rows, {len(cols)} columns)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
