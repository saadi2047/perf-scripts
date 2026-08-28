#!/usr/bin/env python3
# perf_report.py — ONE plain-language HTML report from capacity(before/after)+infra+app.
# Design: verdict up top, before/after deltas in words, hour-trends as findings, tables secondary.
# Usage: python3 perf_report.py --before DIR --after DIR --infra DIR --app DIR -o report.html
import sys, os, csv, argparse, datetime

def read_csv(p):
    return list(csv.DictReader(open(p))) if p and os.path.exists(p) else []
def read_kv(p):
    d={}
    if p and os.path.exists(p):
        for line in open(p):
            if "=" in line: k,v=line.strip().split("=",1); d[k]=v
    return d

def read_lines(p):
    return [l.strip() for l in open(p) if l.strip()] if p and os.path.exists(p) else []


def fmt_cpu(m):
    """millicores -> human: 20000 -> '20 cores', 500 -> '500m'"""
    try: m=float(m)
    except: return str(m)
    if m>=1000: return f"{m/1000:.1f}".rstrip("0").rstrip(".")+" cores"
    return f"{m:.0f}m"

def fmt_mem(mi):
    """Mi -> human: 30720 -> '30 GB', 512 -> '512 MB'"""
    try: mi=float(mi)
    except: return str(mi)
    if mi>=1024: return f"{mi/1024:.1f}".rstrip("0").rstrip(".")+" GB"
    return f"{mi:.0f} MB"

CSS = """<style>
:root{--bg:#f4f6f9;--card:#fff;--ink:#1a2733;--mut:#5b6b7b;--line:#d6dee6;--band:#eef3f8;
--head:#1f3a52;--accent:#2563a8;--good:#1a7f4b;--warn:#b7791f;--bad:#c0392b;}
*{box-sizing:border-box}body{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;
margin:0;background:var(--bg);color:var(--ink);font-size:14px;line-height:1.6}
.wrap{max-width:1000px;margin:0 auto;padding:24px}
h1{font-size:22px;margin:0 0 4px}h2{font-size:17px;color:var(--head);border-bottom:2px solid var(--accent);
padding-bottom:6px;margin:32px 0 14px}
.masthead{background:linear-gradient(135deg,#1f3a52,#2563a8);color:#fff;padding:22px 26px;border-radius:10px;margin-bottom:8px}
.masthead .sub{opacity:.9;font-size:12.5px;margin-top:6px}
.verdict{background:var(--card);border:1px solid var(--line);border-left:5px solid var(--accent);
border-radius:8px;padding:16px 20px;margin:16px 0;font-size:15px}
.verdict.good{border-left-color:var(--good)}.verdict.warn{border-left-color:var(--warn)}.verdict.bad{border-left-color:var(--bad)}
.findings{list-style:none;padding:0;margin:10px 0}
.findings li{background:var(--card);border:1px solid var(--line);border-radius:7px;padding:11px 15px;margin:7px 0;
border-left:3px solid var(--accent)}
.findings li.warn{border-left-color:var(--warn)}.findings li.bad{border-left-color:var(--bad)}.findings li.good{border-left-color:var(--good)}
.findings li.noise{border-left-color:var(--mut);color:var(--mut)}
details{margin:12px 0}summary{cursor:pointer;color:var(--accent);font-size:13px;font-weight:600;padding:6px 0}
table{width:100%;border-collapse:collapse;background:var(--card);border:1px solid var(--line);border-radius:8px;overflow:hidden;margin:8px 0;font-size:13px}
th{background:var(--head);color:#fff;text-align:left;padding:7px 11px;font-size:12px}
td{padding:6px 11px;border-top:1px solid var(--line)}tr:nth-child(even) td{background:var(--band)}
.delta{font-weight:600}.up{color:var(--bad)}.dn{color:var(--good)}.flat{color:var(--mut)}
.muted{color:var(--mut);font-size:12.5px}
</style>"""

def li(text, cls=""):
    return f'<li class="{cls}">{text}</li>'

def classify_finding(txt):
    t=txt.lower()
    if "not cpu-related" in t or "excluded" in t: return "noise"
    if "503" in t or "exception" in t or "restart" in t or "exhaustion" in t or "unable to acquire" in t: return "bad"
    if "insufficient cpu" in t or "swing" in t or "throttl" in t: return "warn"
    return ""

def section_deltas(before, after):
    h=['<h2>1 · What changed from the load</h2>']
    bc=read_csv(os.path.join(before,"capacity_by_class.csv")) if before else []
    ac=read_csv(os.path.join(after,"capacity_by_class.csv")) if after else []
    bf=read_kv(os.path.join(before,"fit_check.txt")) if before else {}
    af=read_kv(os.path.join(after,"fit_check.txt")) if after else {}
    bp=read_csv(os.path.join(before,"pending_pods.csv")) if before else []
    ap=read_csv(os.path.join(after,"pending_pods.csv")) if after else []
    if not bc and not ac:
        return "".join(h)+'<p class="muted">No capacity snapshots provided.</p>'
    def approw(rows):
        for r in rows:
            if r["class"]=="app-worker": return r
        return {}
    ba,aa=approw(bc),approw(ac)
    facts=[]
    # reserved cpu delta
    try:
        rb,ra=float(ba.get("reserved_cores",0)),float(aa.get("reserved_cores",0))
        d=ra-rb; arrow="up" if d>0 else ("dn" if d<0 else "flat")
        facts.append(f'Reserved CPU on the 12 app-worker nodes went <span class="delta {arrow}">{rb:.0f}c → {ra:.0f}c</span> ({"+" if d>=0 else ""}{d:.0f}c) between before and after load.')
    except: pass
    # pending delta
    facts.append(f'Pending pods: <span class="delta {"up" if len(ap)>len(bp) else "flat"}">{len(bp)} → {len(ap)}</span>.')
    # fit
    if bf.get("can_fit_3000m_txn_pod"):
        facts.append(f'A new 3000m transaction pod could be placed: <b>before</b> {bf.get("can_fit_3000m_txn_pod","?")}, <b>after</b> {af.get("can_fit_3000m_txn_pod","?")} (largest free slot {bf.get("largest_single_node_free_cpu_m","?")}m → {af.get("largest_single_node_free_cpu_m","?")}m).')
    h.append('<ul class="findings">'+"".join(li(f) for f in facts)+'</ul>')
    # secondary table
    if bc:
        h.append('<details><summary>Capacity by node class (detail)</summary><table><tr><th>Class</th><th>Nodes</th><th>Alloc (c)</th><th>Reserved before</th><th>Reserved after</th></tr>')
        am={r["class"]:r for r in ac}
        for r in bc:
            a=am.get(r["class"],{})
            h.append(f'<tr><td>{r["class"]}</td><td>{r["nodes"]}</td><td>{r["alloc_cores"]}</td><td>{r["reserved_cores"]}</td><td>{a.get("reserved_cores","–")}</td></tr>')
        h.append('</table></details>')
    return "".join(h)

def _fmt_ts(iso):
    # 2026-08-28T07:46:04Z -> "13:16:04 IST / 07:46:04 UTC (28 Aug)"
    try:
        import datetime
        d=datetime.datetime.strptime(iso,"%Y-%m-%dT%H:%M:%SZ")
        ist=d+datetime.timedelta(hours=5,minutes=30)
        return ist.strftime("%H:%M:%S IST") + " / " + d.strftime("%H:%M:%S UTC (%d %b)")
    except: return iso

def section_trends(infra):
    win=read_kv(os.path.join(infra,"window.txt")) if infra else {}
    hdr="2 · What happened during the run"
    if win.get("start") and win.get("end"):
        hdr += f' <span style="font-size:12px;font-weight:400;color:var(--mut)">({_fmt_ts(win["start"])} → {_fmt_ts(win["end"])}, {win.get("samples","?")} samples)</span>'
    h=[f'<h2>{hdr}</h2>']
    finds=read_lines(os.path.join(infra,"findings.txt")) if infra else []
    trends=read_csv(os.path.join(infra,"service_trends.csv")) if infra else []
    if not finds and not trends:
        return "".join(h)+'<p class="muted">No infra run provided.</p>'
    if finds:
        h.append('<ul class="findings">'+"".join(li(f, classify_finding(f)) for f in finds)+'</ul>')
    if trends:
        h.append('<details><summary>Per-service CPU / memory / scaling (detail)</summary>'
                 '<table><tr><th>Service</th><th>CPU peak</th><th>CPU settled</th><th>Mem peak (Mi)</th><th>Pods</th><th>Restarts</th></tr>')
        for r in trends:
            h.append(f'<tr><td>{r["service"]}</td><td>{fmt_cpu(r["cpu_peak_m"])}</td><td>{fmt_cpu(r["cpu_settled_m"])}</td>'
                     f'<td>{fmt_mem(r["mem_peak_mi"])}</td><td>{r["pods_min"]}→{r["pods_max"]}</td><td>{r["restart_delta"]}</td></tr>')
        h.append('</table></details>')
    return "".join(h)

def section_app(app):
    finds=read_lines(os.path.join(app,"findings.txt")) if app else []
    summ=read_csv(os.path.join(app,"log_summary.csv")) if app else []
    if not finds and not summ: return ""
    h=['<h2>3 · Application log signals</h2>']
    if finds:
        h.append('<ul class="findings">'+"".join(li(f, classify_finding(f)) for f in finds)+'</ul>')
    if summ:
        h.append('<details><summary>Log signal counts by service (detail)</summary>'
                 '<table><tr><th>Service</th><th>Errors</th><th>Exceptions</th><th>Conn-pool</th><th>503</th><th>GC</th></tr>')
        for r in summ:
            h.append(f'<tr><td>{r["service"]}</td><td>{r["errors"]}</td><td>{r["exceptions"]}</td><td>{r["conn_pool"]}</td><td>{r["http503"]}</td><td>{r["gc"]}</td></tr>')
        h.append('</table></details>')
    h.append('<p class="muted">Application performance metrics (Hikari pool, GC, controller latency) are read from Dynatrace, not captured here.</p>')
    return "".join(h)


def section_nodes(before, after, infra):
    bu={r["node"]:r for r in read_csv(os.path.join(before,"node_usage.csv"))} if before else {}
    au={r["node"]:r for r in read_csv(os.path.join(after,"node_usage.csv"))} if after else {}
    du={r["node"]:r for r in read_csv(os.path.join(infra,"node_trends.csv"))} if infra else {}
    nodes=sorted(set(list(bu)+list(au)+list(du)))
    # app-workers only (worker* and gemfire*)
    nodes=[n for n in nodes if "worker" in n or "gemfire" in n]
    if not nodes: return ""
    h=['<h2>3 · Node utilization (app-worker nodes)</h2>']
    # findings: hottest node during load, and before vs after shift
    du_app={n:r for n,r in du.items() if "worker" in n or "gemfire" in n}
    if du_app:
        hot=max(du_app.values(), key=lambda r: float(r.get("cpu_pct_peak") or 0))
        h.append('<ul class="findings">')
        h.append(li(f'Busiest app-worker node during the run: <b>{hot["node"].split(".")[0]}</b> peaked at {hot["cpu_pct_peak"]}% CPU (avg {hot["cpu_pct_avg"]}%).',
                    "warn" if float(hot.get("cpu_pct_peak") or 0)>70 else ""))
        avgall=sum(float(r["cpu_pct_avg"] or 0) for r in du_app.values())/len(du_app)
        h.append(li(f'Average CPU across app-worker nodes during load: {avgall:.1f}%.'))
        h.append('</ul>')
    # table: node | before% | during avg% | during peak% | after%  (cpu then mem)
    h.append('<table><tr><th>Node</th><th>CPU before</th><th>CPU avg (load)</th><th>CPU peak (load)</th><th>CPU after</th><th>Mem avg (load)</th></tr>')
    for n in nodes:
        short=n.split(".")[0]
        b=bu.get(n,{}); a=au.get(n,{}); d=du.get(n,{})
        h.append(f'<tr><td>{short}</td><td>{b.get("cpu_pct","–")}%</td>'
                 f'<td>{d.get("cpu_pct_avg","–")}%</td><td>{d.get("cpu_pct_peak","–")}%</td>'
                 f'<td>{a.get("cpu_pct","–")}%</td><td>{d.get("mem_pct_avg","–")}%</td></tr>')
    h.append('</table>')
    h.append('<p class="muted">Before/after are point-in-time snapshots; load columns are averaged/peaked across the run.</p>')
    return "".join(h)


def section_hpa(before):
    rows=read_csv(os.path.join(before,"hpa_state.csv")) if before else []
    # perf namespaces only (exclude pre-prod, quay, stackrox, istio infra)
    perf=[r for r in rows if r.get("namespace","").startswith("perf-") and "istio" not in r.get("namespace","")]
    if not perf: return ""
    h=['<h2>5 · HPA configuration (per perf namespace)</h2>']
    # findings: any HPA at/near max (current==max) = maxed out; txn drift note
    findings=[]
    for r in perf:
        try:
            cur=int(r["current"] or 0); mx=int(r["max"] or 0)
            if mx>0 and cur>=mx:
                findings.append(li(f'{r["namespace"]}/{r["hpa"]}: at max replicas ({cur}/{mx}) — cannot scale further.',"warn"))
        except: pass
        if "txn-transactionservice" in r.get("hpa",""):
            findings.append(li(f'{r["namespace"]}/{r["hpa"]}: live max={r["max"]} (repo defines max=15 — drift if different).',
                               "bad" if r.get("max")!="15" else ""))
    if findings:
        h.append('<ul class="findings">'+"".join(findings)+'</ul>')
    # table
    h.append('<table><tr><th>Namespace</th><th>HPA</th><th>Min</th><th>Max</th><th>Current</th></tr>')
    for r in sorted(perf,key=lambda x:(x["namespace"],x["hpa"])):
        h.append(f'<tr><td>{r["namespace"]}</td><td>{r["hpa"]}</td><td>{r["min"]}</td>'
                 f'<td>{r["max"]}</td><td>{r.get("current","–")}</td></tr>')
    h.append('</table>')
    return "".join(h)


def section_alloc(before):
    rows=read_csv(os.path.join(before,"resource_alloc.csv")) if before else []
    if not rows: return ""
    # cluster app-worker capacity for context
    cls=read_csv(os.path.join(before,"capacity_by_class.csv")) if before else []
    appc=next((c for c in cls if c["class"]=="app-worker"), {})
    h=['<h2>6 · Resource allocation (request / limit \u00d7 replicas)</h2>']
    if appc:
        h.append('<div class="grid">')
        h.append(f'<div class="kpi"><div class="lbl">App-worker nodes</div><div class="val">{appc.get("nodes","–")}</div></div>')
        h.append(f'<div class="kpi"><div class="lbl">Total CPU (allocatable)</div><div class="val">{appc.get("alloc_cores","–")} cores</div></div>')
        h.append(f'<div class="kpi"><div class="lbl">Reserved now</div><div class="val">{appc.get("reserved_cores","–")} cores</div></div>')
        h.append(f'<div class="kpi"><div class="lbl">Free</div><div class="val good">{appc.get("free_cores","–")} cores</div></div>')
        h.append(f'<div class="kpi"><div class="lbl">Total memory (allocatable)</div><div class="val">{appc.get("alloc_mem_gb","–")} GB</div></div>')
        h.append('</div>')
    h.append('<p class="muted">Per-service pod request &amp; limit, then multiplied by current and max HPA replicas '
             'to show total provisioned CPU/memory. Bottom row = fleet total.</p>')
    # CPU table
    h.append('<h3 style="margin-top:14px">CPU (millicores)</h3>')
    h.append('<table><tr><th>Service</th><th>Req/pod</th><th>Lim/pod</th><th>Repl (cur/max)</th>'
             '<th>Req\u00d7cur</th><th>Lim\u00d7cur</th><th>Req\u00d7max</th><th>Lim\u00d7max</th></tr>')
    for r in rows:
        tot = r["service"]=="TOTAL"
        style=' style="font-weight:700;background:var(--band)"' if tot else ''
        repl = "" if tot else f'{r["replicas_cur"]}/{r["replicas_max"]}'
        h.append(f'<tr{style}><td>{r["service"]}</td><td>{fmt_cpu(r["cpu_req_m"])}</td><td>{fmt_cpu(r["cpu_lim_m"])}</td><td>{repl}</td>'
                 f'<td>{fmt_cpu(r["cpu_req_cur_m"])}</td><td>{fmt_cpu(r["cpu_lim_cur_m"])}</td><td>{fmt_cpu(r["cpu_req_max_m"])}</td><td>{fmt_cpu(r["cpu_lim_max_m"])}</td></tr>')
    h.append('</table>')
    # Memory table
    h.append('<h3 style="margin-top:14px">Memory (Mi)</h3>')
    h.append('<table><tr><th>Service</th><th>Req/pod</th><th>Lim/pod</th><th>Repl (cur/max)</th>'
             '<th>Req\u00d7cur</th><th>Lim\u00d7cur</th><th>Req\u00d7max</th><th>Lim\u00d7max</th></tr>')
    for r in rows:
        tot = r["service"]=="TOTAL"
        style=' style="font-weight:700;background:var(--band)"' if tot else ''
        repl = "" if tot else f'{r["replicas_cur"]}/{r["replicas_max"]}'
        h.append(f'<tr{style}><td>{r["service"]}</td><td>{fmt_mem(r["mem_req_mi"])}</td><td>{fmt_mem(r["mem_lim_mi"])}</td><td>{repl}</td>'
                 f'<td>{fmt_mem(r["mem_req_cur_mi"])}</td><td>{fmt_mem(r["mem_lim_cur_mi"])}</td><td>{fmt_mem(r["mem_req_max_mi"])}</td><td>{fmt_mem(r["mem_lim_max_mi"])}</td></tr>')
    h.append('</table>')
    # a plain finding: total reserved at max vs the 186c pool
    tr=[r for r in rows if r["service"]=="TOTAL"]
    if tr:
        t=tr[0]
        try:
            crm=int(t["cpu_req_max_m"])/1000; mrm=float(t["mem_req_max_mi"])/1024
            h.insert(2, li(f'At max replicas, services would reserve <b>{crm:.1f} CPU cores</b> and <b>{mrm:.0f} GB</b> memory (requests). App-worker pool = 186 cores.',
                           "warn" if crm>186 else ""))
            h.insert(2,'<ul class="findings">'); h.insert(4,'</ul>')
        except: pass
    return "".join(h)

def build_verdict(before, after, infra, app):
    bits=[]; cls="good"
    inf=read_lines(os.path.join(infra,"findings.txt")) if infra else []
    real_cpu = any("Insufficient cpu" in x and "real" in x.lower() for x in inf)
    restarts = any("restart" in x.lower() for x in inf)
    ap=read_lines(os.path.join(app,"findings.txt")) if app else []
    pool = any("connection-pool" in x.lower() or "503" in x for x in ap)
    if real_cpu: bits.append("real CPU scheduling pressure appeared on perf-transaction"); cls="warn"
    if restarts: bits.append("some pods restarted mid-run"); cls="warn"
    if pool: bits.append("connection-pool / 503 signals showed in the logs"); cls="bad"
    if not bits:
        return ("good","The run completed without CPU scheduling pressure, pod restarts, or connection-pool errors in the captured window.")
    return (cls,"During this run, "+"; ".join(bits)+". See the sections below for the per-service detail.")

def build(a):
    _utc=datetime.datetime.now(datetime.timezone.utc)
    _ist=_utc+datetime.timedelta(hours=5,minutes=30)
    now=_ist.strftime("%Y-%m-%d %H:%M IST")+" / "+_utc.strftime("%H:%M UTC")
    man=read_kv(os.path.join(a.before,"manifest.txt")) if a.before else {}
    vcls,vtext=build_verdict(a.before,a.after,a.infra,a.app)
    H=['<!doctype html><html><head><meta charset="utf-8"><title>PERF Report</title>',CSS,'</head><body><div class="wrap">']
    H.append('<div class="masthead"><h1>SBI ePay 2.0 — PERF Test Report</h1>'
             f'<div class="sub">Prepared by Saadi M Muzzammil · generated {now}</div>'
             '<div class="sub" style="margin-top:8px">'
             '<a href="https://oneviewuat.sbi/" style="color:#cfe3ff;margin-right:16px">Dynatrace ↗</a>'
             '<a href="https://console-openshift-console.apps.preprod.epay.sbi/" style="color:#cfe3ff;margin-right:16px">OCP Console ↗</a>'
             '<a href="http://10.177.139.68:3000/" style="color:#cfe3ff">Grafana ↗</a>'
             '</div></div>')
    H.append(f'<div class="verdict {vcls}"><b>Summary.</b> {vtext}</div>')
    H.append(section_deltas(a.before,a.after))
    H.append(section_trends(a.infra))
    H.append(section_nodes(a.before,a.after,a.infra))
    H.append(section_app(a.app))
    H.append(section_hpa(a.before))
    H.append(section_alloc(a.before))
    H.append('<p class="muted" style="margin-top:40px;border-top:1px solid var(--line);padding-top:14px">'
             '© Saadi M Muzzammil · read-only · raw CSVs retained in each source zip.</p>')
    H.append('</div></body></html>')
    return "".join(H)

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    for k in ("before","after","infra","app"): ap.add_argument("--"+k)
    ap.add_argument("-o",default="PERF_Report.html")
    a=ap.parse_args()
    open(a.o,"w").write(build(a))
    print(f"[report] wrote {a.o}")
