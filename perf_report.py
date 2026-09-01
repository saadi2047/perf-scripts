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
    """millicores -> human: 20000 -> '20 cores', 1000 -> '1 core', 500 -> '500m'"""
    try: m=float(m)
    except: return str(m)
    if m>=1000:
        v=f"{m/1000:.1f}".rstrip("0").rstrip(".")
        return v+(" core" if v=="1" else " cores")
    return f"{m:.0f}m"

def fmt_mem(mi):
    """Mi -> human: 30720 -> '30 GB', 512 -> '512 MB'"""
    try: mi=float(mi)
    except: return str(mi)
    if mi>=1024: return f"{mi/1024:.1f}".rstrip("0").rstrip(".")+" GB"
    return f"{mi:.0f} MB"

CSS = """<style>
:root{--bg:#eef2f7;--card:#fff;--ink:#16212e;--mut:#5b6b7b;--line:#dce4ec;--band:#f2f6fa;
--head:#16324a;--accent:#2563a8;--good:#1a7f4b;--warn:#b7791f;--bad:#c0392b;--shadow:0 2px 8px rgba(20,40,60,.08);}
*{box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;
margin:0;background:var(--bg);color:var(--ink);font-size:14px;line-height:1.6;-webkit-font-smoothing:antialiased}
.wrap{max-width:1600px;margin:0 auto;padding:24px 40px}
@media(max-width:640px){.wrap{padding:12px}}
h1{font-size:22px;margin:0 0 4px}
h2{font-size:17px;color:var(--head);margin:34px 0 14px;scroll-margin-top:60px;padding-bottom:8px;border-bottom:2px solid var(--accent);
display:flex;align-items:center;gap:8px}
h3{font-size:13px;color:var(--mut);margin:16px 0 8px;text-transform:uppercase;letter-spacing:.04em}
.masthead{background:linear-gradient(135deg,#16324a,#2563a8);color:#fff;padding:24px 26px;border-radius:12px;
box-shadow:var(--shadow);animation:fadeDown .5s ease}
.masthead .sub{opacity:.92;font-size:12.5px;margin-top:6px}
.masthead a{color:#d0e4ff;text-decoration:none;transition:opacity .2s}.masthead a:hover{opacity:.7}
@keyframes fadeDown{from{opacity:0;transform:translateY(-10px)}to{opacity:1;transform:none}}
@keyframes fadeUp{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:none}}
.verdict,.banner,section,table,.kpi,.findings li{animation:fadeUp .4s ease both}
.verdict{background:var(--card);border-left:5px solid var(--accent);border-radius:10px;padding:16px 20px;
margin:16px 0;font-size:15px;box-shadow:var(--shadow)}
.verdict.good{border-left-color:var(--good)}.verdict.warn{border-left-color:var(--warn)}.verdict.bad{border-left-color:var(--bad)}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin:14px 0}
.kpi{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px;box-shadow:var(--shadow);
transition:transform .2s,box-shadow .2s}
.kpi:hover{transform:translateY(-2px);box-shadow:0 6px 16px rgba(20,40,60,.12)}
.kpi .lbl{font-size:11px;color:var(--mut);text-transform:uppercase;letter-spacing:.04em}
.kpi .val{font-size:21px;font-weight:600;margin-top:4px}
.kpi .val.good{color:var(--good)}.kpi .val.warn{color:var(--warn)}.kpi .val.bad{color:var(--bad)}
.findings{list-style:none;padding:0;margin:10px 0}
.findings li{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:11px 15px;margin:7px 0;
border-left:3px solid var(--accent);box-shadow:var(--shadow)}
.findings li.warn{border-left-color:var(--warn)}.findings li.bad{border-left-color:var(--bad)}
.findings li.good{border-left-color:var(--good)}.findings li.noise{border-left-color:var(--mut);color:var(--mut)}
details{margin:12px 0;background:var(--card);border:1px solid var(--line);border-radius:8px;padding:4px 14px;box-shadow:var(--shadow)}
summary{cursor:pointer;color:var(--accent);font-size:13px;font-weight:600;padding:8px 0;user-select:none}
summary:hover{color:var(--head)}
.tbl-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;border-radius:8px}
table{width:100%;border-collapse:collapse;background:var(--card);border:1px solid var(--line);
border-radius:8px;overflow:hidden;margin:8px 0;font-size:13px;box-shadow:var(--shadow)}
th{background:var(--head);color:#fff;text-align:left;padding:8px 11px;font-size:12px;white-space:nowrap}
td{padding:7px 11px;border-top:1px solid var(--line)}tr:nth-child(even) td{background:var(--band)}
tr{transition:background .15s}tbody tr:hover td{background:#e8f0f8}
.delta{font-weight:600}.up{color:var(--bad)}.dn{color:var(--good)}.flat{color:var(--mut)}
.muted{color:var(--mut);font-size:12.5px}
.copybtn{cursor:pointer;border:1px solid var(--line);background:var(--band);border-radius:5px;
padding:2px 8px;font-size:11px;color:var(--accent);margin-left:6px;transition:all .15s}
.copybtn:hover{background:var(--accent);color:#fff}
.copybtn:active{transform:scale(.94)}
.pod-mono{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px}
.nav{position:sticky;top:0;z-index:10;background:rgba(255,255,255,.95);backdrop-filter:blur(8px);
border:1px solid var(--line);border-radius:8px;padding:9px 14px;margin:12px 0 18px;font-size:12px;box-shadow:var(--shadow)}
.nav a{color:var(--accent);text-decoration:none;margin-right:14px;white-space:nowrap;transition:color .15s}
.nav a:hover{color:var(--head)}
@media(max-width:640px){.nav{overflow-x:auto;white-space:nowrap}h2{font-size:15px}.kpi .val{font-size:18px}}
span[id]{scroll-margin-top:60px;display:block}

.groupband{text-align:center;margin:44px 0 10px;position:relative;animation:fadeUp .5s ease both}
.groupband h2.group{display:inline-block;font-size:19px;font-weight:700;color:#fff;
background:linear-gradient(135deg,#16324a,#2563a8);padding:10px 30px;border-radius:30px;
border:none;box-shadow:0 4px 14px rgba(37,99,168,.3);letter-spacing:.03em;margin:0}
.groupband .gsub{display:block;color:var(--mut);font-size:12.5px;margin-top:8px;font-style:italic}
h2{text-align:center;justify-content:center}
.navbtns{display:flex;flex-wrap:wrap;gap:8px;justify-content:center;margin:18px 0 8px}
.navbtns a{display:inline-flex;align-items:center;justify-content:center;min-width:42px;height:38px;
padding:0 14px;background:var(--card);border:1.5px solid var(--accent);border-radius:9px;
color:var(--accent);text-decoration:none;font-size:13px;font-weight:600;
transition:all .2s cubic-bezier(.2,.8,.3,1);box-shadow:0 2px 6px rgba(20,40,60,.08)}
.navbtns a:hover{background:var(--accent);color:#fff;transform:translateY(-3px) scale(1.05);
box-shadow:0 6px 16px rgba(37,99,168,.35)}
.navbtns a:active{transform:translateY(-1px) scale(1.0)}
@media(max-width:640px){.navbtns a{min-width:36px;height:34px;padding:0 10px;font-size:12px}
.groupband h2.group{font-size:15px;padding:8px 18px}}

</style>
<script>
function copyPod(t){navigator.clipboard.writeText(t).then(function(){
var e=event.target;var o=e.textContent;e.textContent='✓';setTimeout(function(){e.textContent=o},1000);});}
</script>"""

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
    h.append('<p class="muted">This compares the cluster <b>before</b> the load started vs <b>after</b> it finished — showing what the load consumed and whether capacity held.</p>')
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
    try:
        if float(ba.get("alloc_cores",0))>0 and float(ba.get("reserved_cores",0))==0:
            h.append('<div class="banner bad">⚠ Capacity capture incomplete this run — reserved/free CPU unavailable '
                     '(oc describe nodes returned empty). Re-run the before-snapshot for accurate reservation data.</div>')
    except: pass
    # reserved cpu delta — spelled out plainly
    try:
        rb,ra=float(ba.get("reserved_cores",0)),float(aa.get("reserved_cores",0))
        fb=float(ba.get("free_cores",0)); fa=float(aa.get("free_cores",0))
        d=ra-rb; arrow="up" if d>0 else ("dn" if d<0 else "flat")
        _alloc_total=float(ba.get("alloc_cores",0) or 0)
        if abs(d) < 1.0:
            chg=f'<span class="delta flat">essentially unchanged</span>'
        elif d>0:
            chg=f'<span class="delta up">{abs(d):.0f} cores more</span> booked by the load'
        else:
            chg=f'<span class="delta dn">{abs(d):.0f} cores released</span> by the end'
        facts.append(f'<b>CPU booked by pods</b> (reserved) went from <b>{rb:.0f} cores</b> before to <b>{ra:.0f} cores</b> after — {chg}, '
                     f'out of {_alloc_total:.0f} cores available on the 12 app-worker nodes.')
        fd=fa-fb
        if abs(fd) < 1.0:
            fw=f'<span class="delta flat">held steady at {fa:.0f} cores</span>'
        elif fd<0:
            fw=f'<span class="delta up">fell from {fb:.0f} to {fa:.0f} cores</span>'
        else:
            fw=f'<span class="delta dn">rose from {fb:.0f} to {fa:.0f} cores</span>'
        facts.append(f'<b>Free CPU left</b> for new pods {fw} — this is the headroom remaining at the end of the run.')
    except: pass
    # pending — plain
    pcls="up" if len(ap)>len(bp) else "flat"
    _INFRA_POD=("grafana","tempo","otel","collector","prometheus","loki","quay","operator",
                "alertmanager","exporter","kafka","zookeeper","istio")
    _appq=[p for p in ap if p.get("namespace","").startswith("perf-")
           and not any(k in p.get("pod","").lower() for k in _INFRA_POD)]
    def _names(lst,limit=3):
        ns=[f'{p.get("namespace","")}/{p.get("pod","")}' for p in lst]
        shown=", ".join(ns[:limit])
        return shown+(f" +{len(ns)-limit} more" if len(ns)>limit else "")
    if len(ap)==0:
        _pnote="None — every pod found a node."
    elif _appq:
        _w="pod" if len(_appq)==1 else "pods"
        _pnote=f'{len(_appq)} app-service {_w} could not get a node: <b>{_names(_appq)}</b>.'
    else:
        _pnote=(f'These are platform pods, not app services — {_names(ap)}. '
                f'App-service pods all scheduled normally.')
    facts.append(f'<b>Pods waiting to schedule</b>: <span class="delta {pcls}">{len(bp)} before → {len(ap)} after</span>. {_pnote}')
    def pending_detail(plist):
        if not plist: return ""
        items=[f'<li>{p.get("namespace","")} / {p.get("pod","")}</li>' for p in plist]
        return '<p class="muted" style="margin-top:6px">Pods awaiting scheduling:</p><ul class="findings">'+"".join(items)+'</ul>'
    pend_html = pending_detail(ap if ap else bp)
    # fit — plain
    if bf.get("can_fit_3000m_txn_pod"):
        b_fit=bf.get("can_fit_3000m_txn_pod","?"); a_fit=af.get("can_fit_3000m_txn_pod","?")
        facts.append(f'<b>Could a new transaction pod still be placed?</b> Before: <b>{b_fit}</b>, after: <b>{a_fit}</b> '
                     f'(largest single-node free slot: {bf.get("largest_single_node_free_cpu_m","?")}m → {af.get("largest_single_node_free_cpu_m","?")}m). '
                     f'{"Room remained." if a_fit=="YES" else "No single node had room for a 3-core pod."}')
    h.append('<ul class="findings">'+"".join(li(f) for f in facts)+'</ul>')
    if pend_html: h.append(pend_html)
    if bc:
        h.append('<details><summary>Capacity by node class (detail)</summary><div class="tbl-wrap"><table><tr><th>Class</th><th>Nodes</th><th>Alloc (c)</th><th>Reserved before</th><th>Reserved after</th></tr>')
        am={r["class"]:r for r in ac}
        for r in bc:
            a=am.get(r["class"],{})
            h.append(f'<tr><td>{r["class"]}</td><td>{r["nodes"]}</td><td>{r["alloc_cores"]}</td><td>{r["reserved_cores"]}</td><td>{a.get("reserved_cores","–")}</td></tr>')
        h.append('</table></div></details>')
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
    hdr="3 · What happened during the run"
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
                 '<table><tr><th>Service</th><th>CPU peak</th><th>CPU settled</th><th>Mem peak</th><th>Pods</th><th>Restarts</th></tr>')
        for r in trends:
            h.append(f'<tr><td>{r["service"]}</td><td>{fmt_cpu(r["cpu_peak_m"])}</td><td>{fmt_cpu(r["cpu_settled_m"])}</td>'
                     f'<td>{fmt_mem(r["mem_peak_mi"])}</td><td>{r["pods_min"]}→{r["pods_max"]}</td><td>{r["restart_delta"]}</td></tr>')
        h.append('</table></div></details>')
    return "".join(h)

def section_app(app):
    finds=read_lines(os.path.join(app,"findings.txt")) if app else []
    summ=read_csv(os.path.join(app,"log_summary.csv")) if app else []
    if not finds and not summ: return ""
    h=['<h2>9 · Application log signals</h2>']
    if finds:
        h.append('<ul class="findings">'+"".join(li(f, classify_finding(f)) for f in finds)+'</ul>')
    if summ:
        h.append('<details><summary>Log signal counts by service (detail)</summary>'
                 '<div class="tbl-wrap"><table><tr><th>Service</th><th>Errors</th><th>Exceptions</th><th>Conn-pool</th><th>503</th></tr>')
        for r in summ:
            h.append(f'<tr><td>{r["service"]}</td><td>{r["errors"]}</td><td>{r["exceptions"]}</td><td>{r["conn_pool"]}</td><td>{r["http503"]}</td></tr>')
        h.append('</table></div></details>')
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
    h=['<h2>5 · Node utilization (app-worker nodes)</h2>']
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


def section_hpa(before, after=None, infra=None):
    rows=read_csv(os.path.join(before,"hpa_state.csv")) if before else []
    perf=[r for r in rows if r.get("namespace","").startswith("perf-") and "istio" not in r.get("namespace","")]
    if not perf: return ""
    # after-load replica counts
    arows={}
    if after:
        for r in read_csv(os.path.join(after,"hpa_state.csv")):
            arows[(r["namespace"],r["hpa"])]=r
    # peak replicas during load from hpa_scaling.csv
    peak={}
    if infra:
        for r in read_csv(os.path.join(infra,"hpa_scaling.csv")):
            peak[r["hpa"]]=r
    h=['<h2>6 · HPA configuration</h2>']
    findings=[]
    for r in perf:
        try:
            cur=int(r["current"] or 0); mx=int(r["max"] or 0)
            if mx>0 and cur>=mx:
                findings.append(li(f'{r["namespace"]} / {r["hpa"]}: at max replicas ({cur}/{mx}) — cannot scale further.',"warn"))
        except: pass
    if findings:
        h.append('<ul class="findings">'+"".join(findings)+'</ul>')
    h.append('<div class="tbl-wrap"><table><tr><th>Namespace</th><th>HPA</th><th>Min</th><th>Max</th>'
             '<th>Replicas before</th><th>Peak during load</th><th>Replicas after</th></tr>')
    for r in sorted(perf,key=lambda x:(x["namespace"],x["hpa"])):
        key=(r["namespace"],r["hpa"])
        bef=r.get("current","–")
        aft=arows.get(key,{}).get("current","–")
        pk=peak.get(r["hpa"],{}).get("peak_replicas","–")
        # highlight if it scaled up during load
        pkcell=pk
        try:
            if int(pk)>int(bef): pkcell=f'<b style="color:var(--warn)">{pk}</b>'
        except: pass
        h.append(f'<tr><td>{r["namespace"]}</td><td>{r["hpa"]}</td><td>{r["min"]}</td>'
                 f'<td>{r["max"]}</td><td>{bef}</td><td>{pkcell}</td><td>{aft}</td></tr>')
    h.append('</table></div>')
    h.append('<p class="muted">Replicas before → peak during load → after. Peak comes from per-sample HPA capture during the run.</p>')
    return "".join(h)


def section_alloc(before):
    rows=read_csv(os.path.join(before,"resource_alloc.csv")) if before else []
    if not rows: return ""
    # cluster app-worker capacity for context
    cls=read_csv(os.path.join(before,"capacity_by_class.csv")) if before else []
    appc=next((c for c in cls if c["class"]=="app-worker"), {})
    h=['<h2>2 · Resource allocation (request / limit \u00d7 replicas)</h2>']
    # warn if HPA replicas all defaulted to 1 (HPA capture failed)
    nonhdr=[r for r in rows if r["service"]!="TOTAL"]
    if nonhdr and all(r.get("replicas_cur")=="1" and r.get("replicas_max")=="1" for r in nonhdr):
        h.append('<div class="banner bad">⚠ HPA data was not captured this run — replica counts default to 1, '
                 'so the ×replicas totals are understated. Re-run the before-snapshot for accurate HPA scaling.</div>')
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
    h.append('<h3 style="margin-top:14px">CPU</h3>')
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
    h.append('<h3 style="margin-top:14px">Memory</h3>')
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
            _pool=0.0
            try: _pool=float(appc.get("alloc_cores",0) or 0)
            except: _pool=0.0
            _poolt=f"{_pool:.0f}" if _pool else "?"
            h.insert(2, li(f'At max replicas, services would reserve <b>{crm:.1f} CPU cores</b> and <b>{mrm:.0f} GB</b> memory (requests). App-worker pool = {_poolt} cores.',
                           "warn" if (_pool and crm>_pool) else ""))
            h.insert(2,'<ul class="findings">'); h.insert(4,'</ul>')
        except: pass
    return "".join(h)


def section_utilization(infra):
    rows=read_csv(os.path.join(infra,"utilization_summary.csv")) if infra else []
    if not rows: return ""
    def _is_infra(name):
        n=(name or "").lower()
        return any(x in n for x in ("kafka","istio","tempo","zookeeper","otel","generate-traces"))
    app=[r for r in rows if not _is_infra(r["service"])]
    inf=[r for r in rows if _is_infra(r["service"])]
    h=['<h2>4 · Actual resource utilization during load</h2>']
    aw=read_kv(os.path.join(infra,"active_window.txt")) if infra else {}
    _scopetxt="across the load window"
    try:
        _a=int(aw.get("active_samples",0)); _t=int(aw.get("total_samples",0))
        if _a and _t and _a<_t:
            _s=_fmt_ts(aw.get("active_start","")); _e=_fmt_ts(aw.get("active_end",""))
            _scopetxt=(f'over the <b>active load period only</b> ({_s} → {_e}, {_a} of {_t} samples) — '
                       f'idle samples after the load stopped are excluded so the averages are not diluted')
    except: pass
    h.append(f'<p class="muted">Real CPU/memory used (not provisioned) {_scopetxt}. Peak values show the IST timestamp they occurred. Figures are <b>whole-pod</b> (app container + istio-proxy sidecar); Dynatrace per-container views show the app container only.</p>')
    def _tbl(rws):
        o=['<div class="tbl-wrap"><table><tr><th>Service</th><th>CPU avg</th><th>CPU peak</th><th>CPU peak time</th><th>Mem avg</th><th>Mem peak</th><th>Mem peak time</th></tr>']
        for r in sorted(rws,key=lambda x:-float(x.get("cpu_peak_m") or 0)):
            o.append(f'<tr><td>{r["service"]}</td><td>{fmt_cpu(r["cpu_avg_m"])}</td><td>{fmt_cpu(r["cpu_peak_m"])}</td>'
                     f'<td class="muted">{r["cpu_peak_ts"]}</td><td>{fmt_mem(r["mem_avg_mi"])}</td><td>{fmt_mem(r["mem_peak_mi"])}</td>'
                     f'<td class="muted">{r["mem_peak_ts"]}</td></tr>')
        o.append('</table></div>')
        return "".join(o)
    h.append(_tbl(app))
    if inf:
        h.append(f'<details><summary>Platform infra (kafka / istio / tempo) — {len(inf)} workloads</summary>'+_tbl(inf)+'</details>')
    return "".join(h)

def section_container_split(infra):
    rows=read_csv(os.path.join(infra,"container_split.csv")) if infra else []
    if not rows: return ""
    INF=("kafka","istio","tempo","zookeeper","otel","istiod","generate-traces")
    rows=[r for r in rows if not any(k in r["service"].lower() for k in INF)]
    rows=[r for r in rows if float(r.get("app_cpu_avg_m") or 0)>0]
    if not rows: return ""
    h=['<h3>App container vs istio-proxy sidecar</h3>']
    h.append('<p class="muted">Pod CPU includes the istio-proxy sidecar. Dynatrace\'s per-container view shows the '
             'app container only — this table reconciles the two.</p>')
    h.append('<div class="tbl-wrap"><table><tr><th>Service</th><th>App CPU avg</th><th>App CPU peak</th>'
             '<th>Sidecar avg</th><th>Sidecar peak</th><th>Sidecar share</th></tr>')
    for r in sorted(rows,key=lambda x:-float(x["app_cpu_avg_m"])):
        h.append(f'<tr><td>{r["service"]}</td><td>{fmt_cpu(r["app_cpu_avg_m"])}</td><td>{fmt_cpu(r["app_cpu_peak_m"])}</td>'
                 f'<td>{fmt_cpu(r["sidecar_cpu_avg_m"])}</td><td>{fmt_cpu(r["sidecar_cpu_peak_m"])}</td>'
                 f'<td>{r["sidecar_pct"]}%</td></tr>')
    h.append('</table></div>')
    return "".join(h)

def section_load_alloc(infra, before):
    # actual usage (peak/avg) alongside provisioned request — the window below allocation
    util={r["service"]:r for r in read_csv(os.path.join(infra,"utilization_summary.csv"))} if infra else {}
    alloc=read_csv(os.path.join(before,"resource_alloc.csv")) if before else []
    if not util or not alloc: return ""
    h=['<h3>Provisioned vs actually used (during load)</h3>']
    h.append('<p class="muted">How much of each service\'s CPU request was actually used at peak — shows over/under-provisioning.</p>')
    h.append('<div class="tbl-wrap"><table><tr><th>Service</th><th>CPU req/pod</th><th>CPU peak used (total)</th><th>CPU avg used (total)</th><th>Mem peak used (total)</th></tr>')
    for a in alloc:
        svc=a["service"]
        if svc=="TOTAL": continue
        u=util.get(svc)
        if not u: continue
        h.append(f'<tr><td>{svc}</td><td>{fmt_cpu(a["cpu_req_m"])}</td><td>{fmt_cpu(u["cpu_peak_m"])}</td>'
                 f'<td>{fmt_cpu(u["cpu_avg_m"])}</td><td>{fmt_mem(u["mem_peak_mi"])}</td></tr>')
    h.append('</table></div>')
    return "".join(h)

def section_probes(infra):
    rows=read_csv(os.path.join(infra,"probe_summary.csv")) if infra else []
    if not rows: return ""
    h=['<h3>Liveness / readiness probe failures</h3>']
    h.append('<p class="muted">Pods whose health checks failed during the run. Repeated liveness failures are what trigger a SIGTERM restart (exit 143).</p>')
    h.append('<div class="tbl-wrap"><table><tr><th>Namespace</th><th>Pod</th><th>Liveness</th><th>Readiness</th><th>Startup</th><th>Total</th></tr>')
    for r in rows[:15]:
        h.append(f'<tr><td>{r["namespace"]}</td><td class="pod-mono">{r["pod"]}</td>'
                 f'<td>{r["liveness_failures"]}</td><td>{r["readiness_failures"]}</td>'
                 f'<td>{r["startup_failures"]}</td><td><b>{r["total"]}</b></td></tr>')
    h.append('</table></div>')
    return "".join(h)

def section_restarts(infra):
    rows=read_csv(os.path.join(infra,"restart_summary.csv")) if infra else []
    if not rows: return ""
    # split app-service restarts (what matters) from infra noise (tempo/kafka/istio/otel)
    INFRA=("tempo","kafka","istio","otel","collector","grafana","prometheus","loki","zookeeper")
    app=[r for r in rows if not any(x in r["pod"].lower() or x in r["namespace"].lower() for x in INFRA)]
    infra_noise=[r for r in rows if any(x in r["pod"].lower() or x in r["namespace"].lower() for x in INFRA)]
    h=['<h2>10 · Pod restarts &amp; reasons</h2>']
    if app:
        h.append('<p class="muted">App-service pods that restarted during the run, worst first. Exit 143 = SIGTERM (usually liveness-probe kill); 137 = OOMKilled.</p>')
        h.append('<ul class="findings">')
        for r in app[:10]:
            cls="bad" if str(r["exit_code"]) in ("137","143") or r["reason"]=="OOMKilled" else "warn"
            h.append(f'<li class="{cls}">{r["namespace"]} / {r["pod"]} — <b>{r["restarts"]} restart(s)</b>, {r["reason"]} (exit {r["exit_code"]}) — {r["interpretation"]}</li>')
        h.append('</ul>')
    else:
        h.append('<p class="muted">No app-service pod restarts during the run.</p>')
    # infra noise collapsed
    if infra_noise:
        h.append(f'<details><summary>Infrastructure pod restarts — {len(infra_noise)} pods (tempo / kafka / istio — not app services)</summary><div class="tbl-wrap">')
        h.append('<table><tr><th>Namespace</th><th>Pod</th><th>Restarts</th><th>Reason</th><th>Exit</th></tr>')
        for r in infra_noise[:20]:
            h.append(f'<tr><td>{r["namespace"]}</td><td>{r["pod"]}</td><td>{r["restarts"]}</td><td>{r["reason"]}</td><td>{r["exit_code"]}</td></tr>')
        h.append('</table></div></details>')
    return "".join(h)

def section_distribution(infra):
    rows=read_csv(os.path.join(infra,"node_distribution.csv")) if infra else []
    if not rows: return ""
    nsrows=read_csv(os.path.join(infra,"node_distribution_ns.csv")) if infra else []
    h=['<h2>8 · Pod distribution across nodes</h2>']
    h.append('<p class="muted">App pods on each of the 12 app-worker nodes (7 workers + 5 gemfire), broken down by namespace — before load, at peak, and after. Node name is merged across its namespace rows (like a spreadsheet).</p>')
    # group namespace rows by node
    from collections import OrderedDict
    bynode=OrderedDict()
    for r in nsrows:
        bynode.setdefault(r["node"],[]).append(r)
    # node totals lookup
    tot={r["node"]:r for r in rows}
    # order nodes: workers first (1-7), then gemfire (1-5)
    def _nodekey(n):
        import re
        m=re.match(r'(worker|gemfirecache)(\d+)',n)
        if m: return (0 if m.group(1)=="worker" else 1, int(m.group(2)))
        return (2,0)
    ordered=sorted(bynode.keys(),key=_nodekey)
    h.append('<div class="tbl-wrap"><table><tr><th>Node</th><th>Namespace</th><th>Pods before</th><th>Pods peak</th><th>Pods after</th></tr>')
    for node in ordered:
        items=sorted(bynode[node],key=lambda x:x["namespace"])
        span=len(items)+1  # +1 for the node total row
        t=tot.get(node,{})
        first=True
        def _infra_ns(nsn):
            return any(x in (nsn or "").lower() for x in ("kafka","istio","tempo","zookeeper"))
        for r in items:
            muted=' style="color:var(--mut)"' if _infra_ns(r["namespace"]) else ''
            tag=' <span class="muted">(infra)</span>' if _infra_ns(r["namespace"]) else ''
            if first:
                h.append(f'<tr><td rowspan="{span}" style="vertical-align:middle;font-weight:600;background:var(--band)">{node}</td>'
                         f'<td{muted}>{r["namespace"]}{tag}</td><td{muted}>{r["pods_before"]}</td><td{muted}>{r["pods_peak"]}</td><td{muted}>{r["pods_after"]}</td></tr>')
                first=False
            else:
                h.append(f'<tr><td{muted}>{r["namespace"]}{tag}</td><td{muted}>{r["pods_before"]}</td><td{muted}>{r["pods_peak"]}</td><td{muted}>{r["pods_after"]}</td></tr>')
        # node total row
        h.append(f'<tr style="border-top:2px solid var(--accent)"><td style="font-weight:600">TOTAL</td>'
                 f'<td style="font-weight:600">{t.get("pods_before","–")}</td>'
                 f'<td style="font-weight:600">{t.get("pods_peak","–")}</td>'
                 f'<td style="font-weight:600">{t.get("pods_after","–")}</td></tr>')
    h.append('</table></div>')
    return "".join(h)

def section_spinup(infra):
    rows=read_csv(os.path.join(infra,"spinup_summary.csv")) if infra else []
    if not rows: return ""
    def _is_infra(name):
        n=(name or "").lower()
        return any(x in n for x in ("kafka","istio","tempo","zookeeper","otel","generate-traces"))
    app=[r for r in rows if not _is_infra(r["service"])]
    inf=[r for r in rows if _is_infra(r["service"])]
    h=['<h2>7 · Pod spin-up time (per namespace)</h2>']
    h.append('<p class="muted">How long pods took from created → ready during the run, by namespace/service. Slow spin-up delays HPA response under load.</p>')
    def _tbl(rws):
        o=['<div class="tbl-wrap"><table><tr><th>Namespace</th><th>Service</th><th>Pods</th><th>Avg spin-up</th><th>Max spin-up</th></tr>']
        for r in rws:
            o.append(f'<tr><td>{r.get("namespace","")}</td><td>{r["service"]}</td><td>{r["pods"]}</td>'
                     f'<td>{r["spinup_avg_s"]}s</td><td>{r["spinup_max_s"]}s</td></tr>')
        o.append('</table></div>')
        return "".join(o)
    h.append(_tbl(app))
    if inf:
        h.append(f'<details><summary>Platform infra (kafka / istio / tempo) — {len(inf)} workloads</summary>'+_tbl(inf)+'</details>')
    return "".join(h)

def section_hpa_scaling(infra):
    rows=read_csv(os.path.join(infra,"hpa_scaling.csv")) if infra else []
    if not rows: return ""
    h=['<h3>HPA scaling during load</h3>']
    h.append('<div class="tbl-wrap"><table><tr><th>HPA</th><th>Start</th><th>Peak</th><th>Max</th><th>Hit max?</th></tr>')
    for r in rows:
        if r.get("scaled_up")!="YES" and r.get("hit_max")!="YES": continue
        hitcls=' style="color:var(--bad);font-weight:600"' if r["hit_max"]=="YES" else ''
        h.append(f'<tr><td>{r["hpa"]}</td><td>{r["start_replicas"]}</td><td>{r["peak_replicas"]}</td>'
                 f'<td>{r["max_replicas"]}</td><td{hitcls}>{r["hit_max"]}</td></tr>')
    h.append('</table></div>')
    return "".join(h)

def section_app_pods(app):
    rows=read_csv(os.path.join(app,"pod_signal_detail.csv")) if app else []
    if not rows: return ""
    rows=sorted(rows,key=lambda x:-int(x.get("total") or 0))
    n=len(rows)
    h=['<details><summary>Pods with log signals — click to expand ('+str(n)+' pods)</summary>']
    h.append('<div class="tbl-wrap"><table><tr><th>Namespace</th><th>Pod</th><th>Errors</th><th>Exceptions</th><th>Conn-pool</th><th>503</th><th>Total</th></tr>')
    for r in rows:
        pod=r["pod"]
        h.append(f'<tr><td>{r["namespace"]}</td><td class="pod-mono">{pod}'
                 f'<button class="copybtn" onclick="copyPod(\'{pod}\')">copy</button></td>'
                 f'<td>{r["errors"]}</td><td>{r["exceptions"]}</td><td>{r["conn_pool"]}</td><td>{r["http503"]}</td>'
                 f'<td><b>{r["total"]}</b></td></tr>')
    h.append('</table></div></details>')
    return "".join(h)

def build_verdict(before, after, infra, app):
    """Headline verdict with real numbers, so a reader gets the result without scrolling."""
    cls="good"; bits=[]
    # --- capacity headline ---
    cap=""
    try:
        bc=read_csv(os.path.join(before,"capacity_by_class.csv")) if before else []
        ac=read_csv(os.path.join(after,"capacity_by_class.csv")) if after else []
        b=next((r for r in bc if r["class"]=="app-worker"),{})
        a2=next((r for r in ac if r["class"]=="app-worker"),{})
        if b:
            alloc=float(b.get("alloc_cores",0)); free_b=float(b.get("free_cores",0))
            free_a=float(a2.get("free_cores",free_b) or free_b)
            pct=100.0*(alloc-free_a)/alloc if alloc else 0
            cap=(f"The 12 app-worker nodes hold <b>{alloc:.0f} CPU cores</b>; at the end of the run "
                 f"<b>{free_a:.0f} cores</b> were still free ({pct:.0f}% booked).")
            if free_a < alloc*0.08: cls="bad"
            elif free_a < alloc*0.15 and cls!="bad": cls="warn"
    except: pass
    # --- what was actually used during the load window (real usage, not projections) ---
    risk=""
    try:
        urows=read_csv(os.path.join(infra,"utilization_summary.csv")) if infra else []
        INFRA_K=("kafka","istio","tempo","zookeeper","otel","generate-traces")
        appu=[r for r in urows if not any(k in r["service"].lower() for k in INFRA_K)]
        if appu:
            cpu_avg=sum(float(r.get("cpu_avg_m") or 0) for r in appu)/1000.0
            cpu_pk =sum(float(r.get("cpu_peak_m") or 0) for r in appu)/1000.0
            mem_pk =sum(float(r.get("mem_peak_mi") or 0) for r in appu)/1024.0
            _res=0.0
            try:
                _b=next((r for r in read_csv(os.path.join(before,"capacity_by_class.csv")) if r["class"]=="app-worker"),{})
                _res=float(_b.get("reserved_cores",0) or 0)
            except: pass
            aw=read_kv(os.path.join(infra,"active_window.txt")) if infra else {}
            _scope="the load window"
            _tail=""
            try:
                _act=int(aw.get("active_samples",0)); _tot=int(aw.get("total_samples",0))
                if _act and _tot and _act<_tot:
                    _tail=(f" The capture ran for {_tot} samples but load was only active for {_act} of them; "
                           f"averages below cover the active load period only.")
            except: pass
            risk=(f" Across {_scope} the app services actually used <b>{cpu_avg:.1f} cores</b> CPU on average "
                  f"(<b>{cpu_pk:.1f} cores</b> at peak) and <b>{mem_pk:.0f} GB</b> memory at peak."+_tail)
            # be explicit when the window was idle — otherwise the numbers look wrong
            if _res and cpu_pk < _res*0.10:
                risk+=(" CPU use is far below the reserved figure, so this window was near-idle — "
                       "run the capture during real load for representative numbers.")
    except: pass
    # --- failures ---
    ap_l=read_lines(os.path.join(app,"findings.txt")) if app else []
    inf_l=read_lines(os.path.join(infra,"findings.txt")) if infra else []
    n503=sum(1 for x in ap_l if "503" in x)
    npool=sum(1 for x in ap_l if "connection-pool" in x.lower())
    if npool: bits.append(f"connection-pool exhaustion on {npool} service(s)"); cls="bad"
    if n503: bits.append(f"HTTP 503s on {n503} service(s)")
    try:
        rs=read_csv(os.path.join(infra,"restart_summary.csv")) if infra else []
        appr=[r for r in rs if not any(k in (r["pod"]+r["namespace"]).lower()
              for k in ("kafka","istio","tempo","zookeeper","otel"))]
        if appr:
            worst=appr[0]
            bits.append(f"{len(appr)} app pod(s) restarted (worst: {worst['pod'].split('-')[0]}-… "
                        f"{worst['restarts']}\u00d7, {worst['interpretation'] or worst['reason']})")
            if cls!="bad": cls="warn"
    except: pass
    if any("Insufficient cpu" in x for x in inf_l):
        bits.append("real CPU scheduling pressure (Insufficient cpu events)"); cls="bad"
    fail = (" Failures seen: "+"; ".join(bits)+".") if bits else " No pod restarts, 503s, or scheduling failures were seen in this window."
    return (cls, (cap+risk+fail).strip())

def build(a):
    _utc=datetime.datetime.now(datetime.timezone.utc)
    _ist=_utc+datetime.timedelta(hours=5,minutes=30)
    now=_ist.strftime("%Y-%m-%d %H:%M IST")+" / "+_utc.strftime("%H:%M UTC")
    man=read_kv(os.path.join(a.before,"manifest.txt")) if a.before else {}
    # load window (IST) from infra window.txt
    win=read_kv(os.path.join(a.infra,"window.txt")) if a.infra else {}
    loadwin=""
    if win.get("start") and win.get("end"):
        loadwin=f'<div class="sub" style="margin-top:4px">Load window: {_fmt_ts(win["start"])} → {_fmt_ts(win["end"])} ({win.get("samples","?")} samples)</div>'
    vcls,vtext=build_verdict(a.before,a.after,a.infra,a.app)
    H=['<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>PERF Report</title>',CSS,'</head><body><div class="wrap">']
    H.append('<div class="masthead"><h1>SBI ePay 2.0 — PERF Test Report</h1>'
             f'<div class="sub">Prepared by Saadi M Muzzammil · generated {now}</div>'
             f'{loadwin}'
             '<div class="sub" style="margin-top:8px">'
             '<a href="https://oneviewuat.sbi/" target="_blank" rel="noopener" style="color:#cfe3ff;margin-right:16px">Dynatrace ↗</a>'
             '<a href="https://console-openshift-console.apps.preprod.epay.sbi/" target="_blank" rel="noopener" style="color:#cfe3ff;margin-right:16px">OCP Console ↗</a>'
             '<a href="http://10.177.139.68:3000/" target="_blank" rel="noopener" style="color:#cfe3ff">Grafana ↗</a>'
             '</div></div>')
    H.append('<div class="navbtns">'
             '<a href="#s1">1 · Changed</a><a href="#s2">2 · Allocation</a>'
             '<a href="#s3">3 · During run</a><a href="#s4">4 · Utilization</a>'
             '<a href="#s5">5 · Nodes</a><a href="#s6">6 · HPA</a>'
             '<a href="#s7">7 · Spin-up</a><a href="#s8">8 · Distribution</a>'
             '<a href="#s9">9 · Log signals</a><a href="#s10">10 · Restarts</a></div>')
    H.append(f'<div class="verdict {vcls}"><b>Summary.</b> {vtext}</div>')

    def band(title, sub):
        return f'<div class="groupband"><h2 class="group">{title}</h2><span class="gsub">{sub}</span></div>'

    # GROUP A — Capacity & Headroom
    H.append(band("A · Capacity &amp; Headroom","Did we have room, and what did the load consume?"))
    H.append('<span id="s1"></span>'+section_deltas(a.before,a.after))
    H.append('<span id="s2"></span>'+section_alloc(a.before))

    # GROUP B — Resource Usage During Load
    H.append(band("B · Resource Usage During Load","How hard did everything actually work?"))
    H.append('<span id="s3"></span>'+section_trends(a.infra))
    H.append('<span id="s4"></span>'+section_utilization(a.infra))
    H.append(section_container_split(a.infra))
    H.append(section_load_alloc(a.infra,a.before))
    H.append('<span id="s5"></span>'+section_nodes(a.before,a.after,a.infra))

    # GROUP C — Scaling Behaviour
    H.append(band("C · Scaling Behaviour","Did autoscaling keep up with the load?"))
    H.append('<span id="s6"></span>'+section_hpa(a.before,a.after,a.infra))
    H.append(section_hpa_scaling(a.infra))
    H.append('<span id="s7"></span>'+section_spinup(a.infra))

    # GROUP D — Placement & Scheduling
    H.append(band("D · Placement &amp; Scheduling","Where did pods land across the nodes?"))
    H.append('<span id="s8"></span>'+section_distribution(a.infra))

    # GROUP E — Health & Failures
    H.append(band("E · Health &amp; Failures","What broke during the run, and why?"))
    H.append('<span id="s9"></span>'+section_app(a.app))
    H.append(section_app_pods(a.app))
    H.append('<span id="s10"></span>'+section_restarts(a.infra))
    H.append(section_probes(a.infra))

    H.append('<p class="muted" style="text-align:center;margin-top:40px;border-top:1px solid var(--line);padding-top:14px">'
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
