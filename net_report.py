#!/usr/bin/env python3
# ============================================================================
# net_report.py — HTML report from the perf-all.sh network capture
# SBI ePay 2.0 PERF · Saadi M Muzzammil
# ----------------------------------------------------------------------------
# Consumes the two CSVs perf-all.sh writes per load window:
#     traffic_<tag>.csv   ts_ist,elapsed_s,rate_rps,total_since_start
#     nodenet_<tag>.csv   ts_ist,node,rx_mbps,tx_mbps,rx_pps,tx_pps,conntrack,
#                         from_gateway,all_received
#
#   python3 net_report.py --tag run1 -o NET_Report.html
#   python3 net_report.py --tag w1 --tag w2 --tag w3 -o NET_Report_3windows.html
#   python3 net_report.py --traffic traffic_1430.csv --nodenet nodenet_1430.csv
#
# Optional LB funnel numbers (repeat once per --tag, same order):
#   --jmeter 400000 --avlb-recv 399500 --avlb-fwd 399100
#
# Read-only. No cluster access — it only parses the CSVs.
# ============================================================================
import os, re, csv, argparse, datetime

# ---------------------------------------------------------------- helpers ---
def read_csv(p):
    if not p or not os.path.exists(p):
        return []
    with open(p, newline="") as f:
        return [r for r in csv.DictReader(f) if r]

def F(v, d=0.0):
    try:
        return float(v)
    except Exception:
        return d

def n0(v):
    try:
        return f"{int(round(float(v))):,}"
    except Exception:
        return "–"

def f1(v, suf=""):
    try:
        return f"{float(v):,.1f}{suf}"
    except Exception:
        return "–"

def pct(a, b):
    """a as a percentage of b"""
    try:
        b = float(b)
        return (float(a) / b * 100.0) if b else 0.0
    except Exception:
        return 0.0

def short_ts(t):
    """'2026-09-27 14:31:02' -> '14:31:02'"""
    return t.split(" ")[-1] if t else "–"

# A node name that is really an AGE string ("51m", "3h", "10d") or "<none>" means
# the capture mis-parsed its pod->node mapping. Never treat it as a node.
NOT_A_NODE = re.compile(r'^(\d+(\.\d+)?[smhdwy]|<none>|none|)$', re.I)


def esc(t):
    return (str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def grp(n):
    n = (n or "").lower()
    if n.startswith(("master", "infra", "zookeeper")):
        return 0
    if n.startswith("kafbroker"):
        return 2
    return 1

GROUPS = [
    (0, "Master &amp; Infra nodes", "master / infra / zookeeper — control plane, ingress gateway, monitoring"),
    (1, "Worker nodes", "worker / gemfirecache — where the application pods run"),
    (2, "Kafka nodes", "kafbroker — brokers and controllers"),
]

# -------------------------------------------------------------------- CSS ---
CSS = """<style>
/* Theme: "telemetry" — deep teal chrome, light table headers, left-aligned rails.
   Deliberately distinct from the navy PERF capacity report so the two are never
   mistaken for each other at a glance.
   Series colours are the validated categorical order (slots 1-5). */
:root{
  --bg:#f1f3f2; --card:#fff; --ink:#0f1f1e; --mut:#55655f;
  --line:#dbe3e0; --band:#f4f7f6; --band2:#e8efed;
  --head:#0f3b39; --head2:#14514c; --accent:#0d7a6f; --accent-soft:#e2f1ee;
  --good-ink:#146c14; --warn-ink:#8a5d00; --bad-ink:#a92f2f;
  --good-bg:#e4f5e4; --warn-bg:#fdf1d8; --bad-bg:#fbe6e6;
  --s1:#2a78d6; --s2:#eb6834; --s3:#1baf7a; --s4:#eda100; --s5:#e87ba4;
  --grid:#e2e8e6; --axis:#c2ccc9;
  --shadow:0 1px 2px rgba(15,59,57,.06),0 3px 12px rgba(15,59,57,.06);
}
*{box-sizing:border-box}
body{font-family:system-ui,-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;
margin:0;background:var(--bg);color:var(--ink);font-size:14px;line-height:1.6;-webkit-font-smoothing:antialiased}
.wrap{max-width:1600px;margin:0 auto;padding:26px 40px 60px}
@media(max-width:640px){.wrap{padding:14px}}
h1{font-size:23px;margin:0;letter-spacing:-.01em}

/* ---- masthead: solid deep teal, left rail, monospace kicker ---- */
.masthead{background:var(--head);color:#fff;padding:24px 28px;border-radius:4px;
border-left:6px solid var(--accent);position:relative;overflow:hidden}
.masthead::after{content:"";position:absolute;inset:0;pointer-events:none;
background:repeating-linear-gradient(135deg,rgba(255,255,255,.04) 0 1px,transparent 1px 11px)}
.masthead .kick{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;
letter-spacing:.18em;text-transform:uppercase;color:#7fd4c7;margin-bottom:9px}
.masthead .sub{opacity:.85;font-size:12.5px;margin-top:7px}
.masthead a{color:#9fe0d5;text-decoration:none;border-bottom:1px solid rgba(159,224,213,.35)}
.masthead a:hover{color:#fff;border-bottom-color:#fff}

/* ---- nav: monospace ticks, no boxes ---- */
.navbtns{display:flex;flex-wrap:wrap;gap:0;margin:0 0 6px;border-bottom:1px solid var(--line)}
.navbtns a{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px;padding:11px 15px;
color:var(--mut);text-decoration:none;border-bottom:2px solid transparent;margin-bottom:-1px;
transition:color .15s,border-color .15s}
.navbtns a:hover{color:var(--accent);border-bottom-color:var(--accent)}

/* ---- headings: left rail, no centring ---- */
h2{font-size:16px;color:var(--head);margin:30px 0 12px;scroll-margin-top:20px;
padding:0 0 0 13px;border-left:4px solid var(--accent);text-align:left;font-weight:700;
letter-spacing:-.005em}
h3{font-size:11px;color:var(--mut);margin:20px 0 7px;text-transform:uppercase;
letter-spacing:.12em;font-weight:700;font-family:ui-monospace,Menlo,Consolas,monospace}

/* ---- group band: rule + left label, not a centred pill ---- */
.groupband{margin:46px 0 4px;border-top:2px solid var(--head);padding-top:11px}
.groupband h2.group{display:block;font-size:19px;font-weight:700;color:var(--head);
background:none;padding:0;border:none;margin:0;letter-spacing:-.01em}
.groupband .gsub{display:block;color:var(--mut);font-size:12.5px;margin-top:3px}

/* ---- verdict ---- */
.verdict{background:var(--card);border:1px solid var(--line);border-left:4px solid var(--accent);
border-radius:3px;padding:15px 19px;margin:16px 0;font-size:14.5px;box-shadow:var(--shadow)}
.verdict.good{border-left-color:var(--good-ink)}
.verdict.warn{border-left-color:var(--warn-ink)}
.verdict.bad{border-left-color:var(--bad-ink)}

/* ---- KPI tiles: top accent bar, tabular figures ---- */
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(172px,1fr));gap:10px;margin:14px 0}
.kpi{background:var(--card);border:1px solid var(--line);border-top:3px solid var(--accent);
border-radius:3px;padding:13px 15px 14px;box-shadow:var(--shadow)}
.kpi .lbl{font-size:10.5px;color:var(--mut);text-transform:uppercase;letter-spacing:.1em;
font-family:ui-monospace,Menlo,Consolas,monospace}
.kpi .val{font-size:22px;font-weight:650;margin-top:5px;letter-spacing:-.02em}
.kpi .val.good{color:var(--good-ink)}.kpi .val.warn{color:var(--warn-ink)}
.kpi .val.bad{color:var(--bad-ink)}
.kpi .val.sm{font-size:16px;letter-spacing:0;line-height:1.35}
.kpi .sub{font-size:11.5px;color:var(--mut);margin-top:3px;line-height:1.4}

/* ---- findings ---- */
.findings{list-style:none;padding:0;margin:10px 0}
.findings li{background:var(--card);border:1px solid var(--line);border-radius:3px;
padding:11px 15px;margin:6px 0;border-left:3px solid var(--accent);box-shadow:var(--shadow)}
.findings li.warn{border-left-color:var(--warn-ink)}
.findings li.bad{border-left-color:var(--bad-ink)}
.findings li.good{border-left-color:var(--good-ink)}
.findings li.noise{border-left-color:var(--axis);color:var(--mut)}

details{margin:10px 0;background:var(--card);border:1px solid var(--line);border-radius:3px;padding:2px 14px}
summary{cursor:pointer;color:var(--accent);font-size:12.5px;font-weight:600;padding:9px 0;user-select:none;
font-family:ui-monospace,Menlo,Consolas,monospace}
summary:hover{color:var(--head)}

/* ---- tables: LIGHT header with a rule, not a solid dark bar ---- */
.tbl-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch}
table{width:100%;border-collapse:collapse;background:var(--card);border:1px solid var(--line);
border-radius:3px;margin:8px 0;font-size:13px;box-shadow:var(--shadow)}
th{background:var(--band2);color:var(--head);text-align:left;padding:9px 11px;font-size:11px;
white-space:nowrap;text-transform:uppercase;letter-spacing:.07em;font-weight:700;
border-bottom:2px solid var(--head);font-family:ui-monospace,Menlo,Consolas,monospace}
td{padding:7px 11px;border-top:1px solid var(--line);white-space:nowrap}
tr:nth-child(even) td{background:var(--band)}
tbody tr:hover td{background:var(--accent-soft)}
td.num,th.num{text-align:right;font-variant-numeric:tabular-nums}
tr.sub td{font-weight:650;background:var(--band2)!important;border-top:1px solid var(--axis)}
tr.tot td{font-weight:700;color:var(--head);background:var(--accent-soft)!important;
border-top:2px solid var(--accent);border-bottom:2px solid var(--accent)}
.grouphdr td{background:var(--head2)!important;color:#fff;font-weight:700;font-size:11px;
text-transform:uppercase;letter-spacing:.1em;font-family:ui-monospace,Menlo,Consolas,monospace}
.grouphdr td span{font-weight:400;text-transform:none;letter-spacing:0;opacity:.8;margin-left:10px;
font-family:system-ui,sans-serif;font-size:12px}

.muted{color:var(--mut);font-size:12.5px}
.mono{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px}
.pill{display:inline-block;padding:1px 8px;border-radius:2px;font-size:11px;font-weight:700;
font-variant-numeric:tabular-nums}
.pill.good{background:var(--good-bg);color:var(--good-ink)}
.pill.warn{background:var(--warn-bg);color:var(--warn-ink)}
.pill.bad{background:var(--bad-bg);color:var(--bad-ink)}
.pill.mut{background:var(--band2);color:var(--mut)}
.bar{display:inline-block;height:8px;border-radius:2px;background:var(--accent);vertical-align:middle}
.bar.warn{background:#fab219}.bar.bad{background:#d03b3b}.bar.mut{background:var(--axis)}

.chart{background:var(--card);border:1px solid var(--line);border-radius:3px;
padding:12px 8px 6px;box-shadow:var(--shadow);margin:10px 0;overflow-x:auto}
.legend{font-size:12px;color:var(--mut);padding:2px 12px 8px}
.legend i{display:inline-block;width:10px;height:10px;border-radius:2px;margin:0 6px 0 16px;vertical-align:-1px}
.legend i:first-child{margin-left:0}
span[id]{scroll-margin-top:20px;display:block}
@media(max-width:640px){h2{font-size:14px}.kpi .val{font-size:19px}
.groupband h2.group{font-size:16px}.navbtns a{padding:9px 10px;font-size:11px}}
@media print{.navbtns{display:none}body{background:#fff}.wrap{padding:0}
details{border:none}details summary{display:none}details[open]{display:block}}
</style>"""


# ------------------------------------------------------------- svg charts ---
def _nice(v):
    """round a max value up to a readable gridline"""
    if v <= 0:
        return 1.0
    import math
    e = math.floor(math.log10(v))
    base = 10 ** e
    for m in (1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10):
        if v <= m * base:
            return m * base
    return 10 * base


S1, S2, S3, S4, S5 = "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"


def svg_chart(series, width=1140, height=270, ylab="", xfmt=None, ymax=None):
    """series: [{'name','color','pts':[(x,y)],'area':bool}]  x = elapsed seconds."""
    series = [s for s in series if s.get("pts")]
    if not series:
        return '<p class="muted">No samples to plot.</p>'
    PL, PR, PT, PB = 66, 16, 14, 36
    xs = [p[0] for s in series for p in s["pts"]]
    ys = [p[1] for s in series for p in s["pts"]]
    x0, x1 = min(xs), max(xs)
    if x1 <= x0:
        x1 = x0 + 1
    ytop = _nice(ymax if ymax else (max(ys) if ys else 1))
    iw = width - PL - PR
    ih = height - PT - PB

    def X(v):
        return PL + (v - x0) / (x1 - x0) * iw

    def Y(v):
        return PT + ih - (min(v, ytop) / ytop) * ih

    o = [f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" '
         f'preserveAspectRatio="xMidYMid meet" style="display:block">']
    # y gridlines
    for k in range(5):
        v = ytop * k / 4.0
        y = Y(v)
        o.append(f'<line x1="{PL}" y1="{y:.1f}" x2="{width-PR}" y2="{y:.1f}" '
                 f'stroke="#e2e8e6" stroke-width="1"/>')
        lab = f"{v:,.0f}" if ytop >= 10 else f"{v:,.1f}"
        o.append(f'<text x="{PL-8}" y="{y+4:.1f}" font-size="11" fill="#55655f" '
                 f'text-anchor="end">{lab}</text>')
    # x ticks
    nt = 7
    for k in range(nt):
        v = x0 + (x1 - x0) * k / (nt - 1)
        x = X(v)
        o.append(f'<line x1="{x:.1f}" y1="{PT+ih}" x2="{x:.1f}" y2="{PT+ih+4}" '
                 f'stroke="#c2ccc9" stroke-width="1"/>')
        lab = xfmt(v) if xfmt else f"{v:,.0f}s"
        o.append(f'<text x="{x:.1f}" y="{PT+ih+18}" font-size="11" fill="#55655f" '
                 f'text-anchor="middle">{lab}</text>')
    if ylab:
        o.append(f'<text x="4" y="{PT+10}" font-size="11" fill="#55655f">{ylab}</text>')
    # series
    for s in series:
        pts = sorted(s["pts"])
        col = s.get("color", S1)
        d = " ".join(f"{X(a):.1f},{Y(b):.1f}" for a, b in pts)
        if s.get("area"):
            o.append(f'<polygon points="{X(pts[0][0]):.1f},{PT+ih:.1f} {d} '
                     f'{X(pts[-1][0]):.1f},{PT+ih:.1f}" fill="{col}" opacity="0.13"/>')
        o.append(f'<polyline points="{d}" fill="none" stroke="{col}" stroke-width="2.1" '
                 f'stroke-linejoin="round" stroke-linecap="round"/>')
        # peak marker — stagger labels so overlaid series do not collide
        px, py = max(pts, key=lambda p: p[1])
        o.append(f'<circle cx="{X(px):.1f}" cy="{Y(py):.1f}" r="4" fill="{col}" '
                 f'stroke="#fff" stroke-width="2"/>')
        tx = min(max(X(px), PL + 34), width - PR - 34)
        ty = max(Y(py) - 11 - 14 * series.index(s), PT + 10)
        o.append(f'<text x="{tx:.1f}" y="{ty:.1f}" font-size="11" '
                 f'font-weight="700" fill="{col}" text-anchor="middle">{py:,.0f}</text>')
    # hover read-out — one transparent band per sample, native <title>, no JS
    xset = sorted({a for s in series for a, _ in s["pts"]})
    if 1 < len(xset) <= 800:
        bw = iw / len(xset)
        lut = [dict(s["pts"]) for s in series]
        for xv in xset:
            parts = [f'{(s.get("name") or "value")} {m[xv]:,.1f}'
                     for s, m in zip(series, lut) if xv in m]
            lbl = xfmt(xv) if xfmt else f"{xv:,.0f}s"
            t = esc(lbl + "  —  " + ", ".join(parts))
            o.append(f'<rect x="{X(xv)-bw/2:.1f}" y="{PT}" width="{max(bw,1):.2f}" '
                     f'height="{ih:.1f}" fill="transparent"><title>{t}</title></rect>')
    o.append(f'<line x1="{PL}" y1="{PT+ih}" x2="{width-PR}" y2="{PT+ih}" '
             f'stroke="#c2ccc9" stroke-width="1"/>')
    o.append("</svg>")
    return "".join(o)


def legend(items):
    b = ['<div class="legend">']
    for name, col in items:
        b.append(f'<i style="background:{col}"></i>{name}')
    b.append("</div>")
    return "".join(b)


# --------------------------------------------------------------- loading ----
class Window:
    """One load window = one traffic CSV + one nodenet CSV."""

    def __init__(self, tag, tfile, nfile, lb=None):
        self.tag = tag
        self.tfile, self.nfile = tfile, nfile
        self.lb = lb or {}
        self.traffic = read_csv(tfile)
        self.nodenet = read_csv(nfile)
        self._crunch()

    # ---- gateway side ----
    def _crunch(self):
        t = self.traffic
        self.ok_traffic = len(t) > 0
        self.samples = len(t)
        self.start = t[0]["ts_ist"] if t else ""
        self.end = t[-1]["ts_ist"] if t else ""
        self.duration = F(t[-1].get("elapsed_s")) if t else 0.0
        self.total = F(t[-1].get("total_since_start")) if t else 0.0
        self.avg_rps = (self.total / self.duration) if self.duration else 0.0
        self.rate_pts = [(F(r.get("elapsed_s")), F(r.get("rate_rps"))) for r in t]
        self.cum_pts = [(F(r.get("elapsed_s")), F(r.get("total_since_start"))) for r in t]
        self.interval = 0.0
        if len(self.rate_pts) > 1:
            d = [b[0] - a[0] for a, b in zip(self.rate_pts, self.rate_pts[1:]) if b[0] > a[0]]
            if d:
                self.interval = sorted(d)[len(d) // 2]
        self.peak_rps = max((y for _, y in self.rate_pts), default=0.0)
        self.peak_at = ""
        for r in t:
            if abs(F(r.get("rate_rps")) - self.peak_rps) < 1e-9:
                self.peak_at = r["ts_ist"]
                break
        # steady state = samples at >= 70% of peak, first sample excluded (cold)
        thr = 0.70 * self.peak_rps
        st = [(F(r.get("elapsed_s")), F(r.get("rate_rps")), r["ts_ist"])
              for r in t if F(r.get("rate_rps")) >= thr]
        self.steady_n = len(st)
        self.steady_avg = (sum(x[1] for x in st) / len(st)) if st else 0.0
        self.steady_from = st[0][2] if st else ""
        self.steady_to = st[-1][2] if st else ""
        self.steady_secs = (st[-1][0] - st[0][0]) if len(st) > 1 else 0.0
        self.ramp_up_s = st[0][0] if st else 0.0
        self.ramp_dn_s = (self.duration - st[-1][0]) if st else 0.0

        # ---- node side ----
        self.nodes = {}
        self.ts_rx, self.ts_tx = {}, {}   # ts -> cluster Mbps
        self.ts_g = {0: {}, 1: {}, 2: {}} # group -> ts -> [rx, tx] Mbps
        self.ts_el = {}                   # ts -> elapsed (aligned to traffic)
        el_by_ts = {r["ts_ist"]: F(r.get("elapsed_s")) for r in t}
        self.bad_nodes = {}
        for r in self.nodenet:
            n = (r.get("node") or "").strip()
            if NOT_A_NODE.match(n):
                # keep the evidence so the report can say the capture needs fixing
                self.bad_nodes[n] = max(self.bad_nodes.get(n, 0.0),
                                        F(r.get("all_received")))
                continue
            d = self.nodes.setdefault(n, dict(
                rx=[], tx=[], rp=[], tp=[], ct=[], fg=0.0, al=0.0, g=grp(n)))
            rx, tx = F(r.get("rx_mbps")), F(r.get("tx_mbps"))
            d["rx"].append(rx)
            d["tx"].append(tx)
            d["rp"].append(F(r.get("rx_pps")))
            d["tp"].append(F(r.get("tx_pps")))
            d["ct"].append(F(r.get("conntrack")))
            d["fg"] = max(d["fg"], F(r.get("from_gateway")))
            d["al"] = max(d["al"], F(r.get("all_received")))
            ts = r.get("ts_ist", "")
            self.ts_rx[ts] = self.ts_rx.get(ts, 0.0) + rx
            self.ts_tx[ts] = self.ts_tx.get(ts, 0.0) + tx
            gslot = self.ts_g[d["g"]].setdefault(ts, [0.0, 0.0])
            gslot[0] += rx
            gslot[1] += tx
            if ts in el_by_ts:
                self.ts_el[ts] = el_by_ts[ts]
        self.ok_nodes = len(self.nodes) > 0
        for n, d in self.nodes.items():
            d["rx_pk"] = max(d["rx"], default=0.0)
            d["tx_pk"] = max(d["tx"], default=0.0)
            d["rx_av"] = (sum(d["rx"]) / len(d["rx"])) if d["rx"] else 0.0
            d["tx_av"] = (sum(d["tx"]) / len(d["tx"])) if d["tx"] else 0.0
            d["rp_pk"] = max(d["rp"], default=0.0)
            d["tp_pk"] = max(d["tp"], default=0.0)
            d["ct_pk"] = max(d["ct"], default=0.0)
        # cluster and group peaks measured per-timestamp (not sum of independent peaks)
        self.cl_rx_pk = max(self.ts_rx.values(), default=0.0)
        self.cl_tx_pk = max(self.ts_tx.values(), default=0.0)
        self.g_rx_pk = {g: max((v[0] for v in m.values()), default=0.0)
                        for g, m in self.ts_g.items()}
        self.g_tx_pk = {g: max((v[1] for v in m.values()), default=0.0)
                        for g, m in self.ts_g.items()}
        self.node_fg = sum(d["fg"] for d in self.nodes.values())
        self.node_al = sum(d["al"] for d in self.nodes.values())
        self.recon_gap = self.total - self.node_fg
        self.recon_pct = abs(pct(self.recon_gap, self.total)) if self.total else 0.0
        self.ew_ratio = (self.node_al / self.node_fg) if self.node_fg else 0.0
        self.ct_pk = max((d["ct_pk"] for d in self.nodes.values()), default=0.0)

        # Worker balance is measured on ALL RCVD — every request the pods on that
        # node served. FROM GW only counts requests whose FIRST hop was the gateway,
        # so a node hosting only internally-called services reads as idle on it while
        # doing the most work in the cluster. Keep FROM GW for the reconciliation.
        w = [(n, d) for n, d in self.nodes.items() if d["g"] == 1 and d["al"] > 0]
        self.wnodes = sorted(w, key=lambda kv: -kv[1]["al"])
        als = [d["al"] for _, d in w]
        self.w_mean = (sum(als) / len(als)) if als else 0.0
        self.w_max = max(als, default=0.0)
        self.w_min = min(als, default=0.0)
        self.w_skew = (self.w_max / self.w_min) if self.w_min > 0 else 0.0

    def chart_series_net(self):
        pts_rx = sorted((self.ts_el[ts], v) for ts, v in self.ts_rx.items() if ts in self.ts_el)
        pts_tx = sorted((self.ts_el[ts], v) for ts, v in self.ts_tx.items() if ts in self.ts_el)
        return pts_rx, pts_tx


def xfmt_for(win):
    """elapsed seconds -> mm:ss (short windows) or HH:MM clock-ish label"""
    def f(v):
        v = max(0, int(round(v)))
        if win.duration > 5400:
            return f"{v//3600}h{(v%3600)//60:02d}m"
        return f"{v//60}:{v%60:02d}"
    return f


# --------------------------------------------------------------- sections ---
def kpi(lbl, val, cls="", sub=""):
    s = f'<div class="sub">{sub}</div>' if sub else ""
    return f'<div class="kpi"><div class="lbl">{lbl}</div><div class="val {cls}">{val}</div>{s}</div>'


def sec_window(w):
    h = [f'<h2>1 · Load window — what OCP received</h2>']
    if not w.ok_traffic:
        return "".join(h) + f'<p class="muted">traffic CSV missing or empty: {w.tfile}</p>'
    h.append('<p class="muted">The gateway counter is the single authority for "requests that reached OCP". '
             'It is read from every <span class="mono">perf-istio-gateway</span> pod and differenced against a '
             'baseline taken before the window opened, so it counts <b>only</b> this window.</p>')
    h.append('<div class="grid">')
    h.append(kpi("OCP received", n0(w.total), "",
                 "measured at the ingress gateway"))
    h.append(kpi("Average rate", f1(w.avg_rps, " req/s"), "",
                 f"over the whole {f1(w.duration/60)} min"))
    h.append(kpi("Steady-state rate", f1(w.steady_avg, " req/s"), "",
                 f"{w.steady_n} samples ≥70% of peak"))
    h.append(kpi("Peak rate", f1(w.peak_rps, " req/s"), "",
                 f"at {short_ts(w.peak_at)} IST"))
    h.append(kpi("Duration", f"{f1(w.duration/60)} min", "",
                 f"{w.samples} samples @ {f1(w.interval)}s"))
    h.append(kpi("Window (IST)", short_ts(w.start) + " → " + short_ts(w.end), "sm",
                 w.start.split(" ")[0] if w.start else ""))
    h.append("</div>")

    h.append('<h3>Request rate across the window</h3>')
    h.append('<div class="chart">' +
             svg_chart([{"name": "req/s", "color": S1,
                         "pts": w.rate_pts, "area": True}],
                       ylab="req/s", xfmt=xfmt_for(w)) + "</div>")

    # phase table
    h.append('<h3>Shape of the window</h3>')
    rows = [
        ("Ramp-up", f"0 → {f1(w.ramp_up_s)}s",
         f"{f1(w.ramp_up_s/60)} min", "rate climbing to within 30% of peak"),
        ("Steady state", f"{short_ts(w.steady_from)} → {short_ts(w.steady_to)}",
         f"{f1(w.steady_secs/60)} min", f"average {f1(w.steady_avg)} req/s — <b>use this for capacity maths</b>"),
        ("Ramp-down", f"last {f1(w.ramp_dn_s)}s",
         f"{f1(w.ramp_dn_s/60)} min", "rate falling away after JMeter stops"),
    ]
    h.append('<div class="tbl-wrap"><table><tr><th>Phase</th><th>When</th>'
             '<th class="num">Length</th><th>What it means</th></tr>')
    for a, b, c, d in rows:
        h.append(f'<tr><td><b>{a}</b></td><td class="mono">{b}</td>'
                 f'<td class="num">{c}</td><td class="muted">{d}</td></tr>')
    h.append("</table></div>")
    h.append('<p class="muted">The average rate is dragged down by ramp-up and ramp-down. When you compare '
             'against JMeter\'s reported throughput, compare <b>steady-state</b> to steady-state — or compare '
             '<b>totals</b>, which are immune to shape.</p>')
    return "".join(h)


def sec_funnel(w):
    h = ['<h2>2 · JMeter → AVLB → OCP funnel</h2>']
    h.append('<p class="muted">Each stage should be within a fraction of a percent of the one above it. '
             'A gap tells you which hop is dropping requests. Fill the JMeter and AVLB rows from the '
             'JMeter summary and the load-balancer statistics — the OCP rows are measured by this report.</p>')
    lb = w.lb
    stages = [
        ("1 · JMeter sent", lb.get("jmeter"), "JMeter aggregate report — total samples across all 5 injectors"),
        ("2 · AVLB received", lb.get("avlb_recv"), "load balancer — client-side connections/requests on the VIP"),
        ("3 · AVLB forwarded to OCP", lb.get("avlb_fwd"), "load balancer — server-side requests to the OCP pool members"),
        ("4 · OCP gateway received", w.total, "<b>measured here</b> — istio ingress gateway counter"),
        ("5 · Delivered to app pods", w.node_fg, "<b>measured here</b> — sidecar counters, first hop from gateway"),
    ]
    h.append('<div class="tbl-wrap"><table><tr><th>Stage</th><th class="num">Requests</th>'
             '<th class="num">Δ vs above</th><th class="num">Loss</th><th>Source</th></tr>')
    prev = None
    for name, val, src in stages:
        if val in (None, ""):
            h.append(f'<tr><td><b>{name}</b></td>'
                     f'<td class="num"><span class="pill mut">awaiting</span></td>'
                     f'<td class="num">–</td><td class="num">–</td>'
                     f'<td class="muted">{src}</td></tr>')
            continue
        v = F(val)
        if prev is None:
            d, lp, cls = "–", "–", ""
        else:
            gap = prev - v
            p = abs(pct(gap, prev))
            d = ("−" if gap > 0 else "+") + n0(abs(gap)) if abs(gap) >= 1 else "0"
            lp = f"{p:.2f}%"
            cls = "good" if p < 0.5 else ("warn" if p < 2 else "bad")
        pill = f'<span class="pill {cls}">{lp}</span>' if cls else lp
        h.append(f'<tr><td><b>{name}</b></td><td class="num">{n0(v)}</td>'
                 f'<td class="num">{d}</td><td class="num">{pill}</td>'
                 f'<td class="muted">{src}</td></tr>')
        prev = v
    h.append("</table></div>")

    # verdict on the measured hop
    p = w.recon_pct
    if p < 1:
        cls, msg = "good", ("Gateway and pod counters agree — the requests the gateway accepted did reach the "
                            "application pods. Any shortfall against JMeter is upstream of OCP.")
    elif p < 3:
        cls, msg = "warn", ("A small gap between gateway and pods. At these rates that is normally sampling skew "
                            "plus a handful of requests rejected during pod churn — worth noting, not chasing.")
    else:
        cls, msg = "bad", ("A material gap between what the gateway accepted and what the pods served. Look for "
                           "gateway-level 5xx, upstream connection failures, or pods going unready mid-window.")
    h.append(f'<div class="verdict {cls}"><b>Gateway → pods: {n0(w.total)} vs {n0(w.node_fg)} '
             f'({p:.2f}% apart).</b> {msg}</div>')

    h.append('<p class="muted">All timestamps are IST (Asia/Kolkata). Confirm the upstream figures are '
             'reported in the same zone before comparing — a timezone offset is the usual reason two totals '
             'for the same window look nothing alike.</p>')
    return "".join(h)


def sec_nodes(w, nic_gbps):
    h = ['<h2>3 · Node-level network and requests</h2>']
    if not w.ok_nodes:
        return "".join(h) + f'<p class="muted">nodenet CSV missing or empty: {w.nfile}</p>'
    h.append('<p class="muted">'
             '<b>RX</b> = data arriving at the node NIC · <b>TX</b> = data leaving it · '
             '<b>pkt/s</b> = packet rate · <b>conntrack</b> = connections the kernel is tracking · '
             '<b>FROM GW</b> = requests whose first hop was the ingress gateway (reconciles with section 1) · '
             '<b>ALL RCVD</b> = every request the pods on that node served, including service-to-service · '
             '<b>share of work</b> = that node\'s slice of ALL RCVD · '
             '<b>GW-facing</b> = how much of the node\'s work arrived straight from the gateway '
             '(low means the node runs internally-called services).'
             '</p>')
    if w.bad_nodes:
        tot = sum(w.bad_nodes.values())
        names = ", ".join(sorted(w.bad_nodes)[:6])
        h.append('<div class="verdict bad"><b>Capture defect — some pods could not be mapped to a node.</b> '
                 f'{len(w.bad_nodes)} bogus node name(s) appeared in the CSV ({esc(names)}'
                 f'{" …" if len(w.bad_nodes) > 6 else ""}), carrying up to {n0(tot)} requests between them. '
                 'These are AGE strings, not nodes: the collector read the pod list positionally and a '
                 '<span class="mono">RESTARTS</span> value like <span class="mono">1 (51m ago)</span> shifted '
                 'the columns, so a restarted pod\'s requests were filed under its age. They have been excluded '
                 'from every figure below, which means the requests of at least one pod are missing from its '
                 'real node. Update perf-all.sh to read the pod list with '
                 '<span class="mono">-o custom-columns</span> and re-capture for exact per-node figures.</div>')
    cap_mbps = nic_gbps * 1000.0
    hdr = ('<tr><th>Node</th><th class="num">RX peak</th><th class="num">RX avg</th>'
           '<th class="num">TX peak</th><th class="num">TX avg</th>'
           '<th class="num">RX pkt/s pk</th><th class="num">TX pkt/s pk</th>'
           '<th class="num">conntrack pk</th><th class="num">FROM GW</th>'
           '<th class="num">ALL RCVD</th><th class="num">share of work</th>'
           '<th class="num">GW-facing</th></tr>')
    h.append('<div class="tbl-wrap"><table>' + hdr)
    G_fg = G_al = 0.0
    for gi, gname, gdesc in GROUPS:
        rws = sorted([(n, d) for n, d in w.nodes.items() if d["g"] == gi],
                     key=lambda kv: (-kv[1]["al"], -(kv[1]["rx_pk"] + kv[1]["tx_pk"])))
        if not rws:
            continue
        h.append(f'<tr class="grouphdr"><td colspan="12">{gname}<span>{gdesc}</span></td></tr>')
        srxp = w.g_rx_pk.get(gi, 0.0)
        stxp = w.g_tx_pk.get(gi, 0.0)
        sfg = sum(d["fg"] for _, d in rws)
        sal = sum(d["al"] for _, d in rws)
        G_fg += sfg
        G_al += sal
        for n, d in rws:
            ctcls = "bad" if d["ct_pk"] > 100000 else ("warn" if d["ct_pk"] > 60000 else "")
            ct = f'<span class="pill {ctcls}">{n0(d["ct_pk"])}</span>' if ctcls else n0(d["ct_pk"])
            sh = pct(d["al"], w.node_al) if w.node_al else 0.0
            bw = max(1, int(round(sh * 1.6))) if d["al"] else 0
            bcls = ""
            if d["g"] == 1 and w.w_mean:
                if d["al"] > 1.5 * w.w_mean:
                    bcls = "bad"
                elif d["al"] > 1.2 * w.w_mean:
                    bcls = "warn"
                elif d["al"] < 0.6 * w.w_mean:
                    bcls = "mut"
            shcell = (f'<span class="bar {bcls}" style="width:{bw}px"></span> {sh:.1f}%'
                      if d["al"] else '<span class="muted">–</span>')
            gwf = (f'{pct(d["fg"], d["al"]):.0f}%' if d["al"] else '<span class="muted">–</span>')
            nicp = pct(max(d["rx_pk"], d["tx_pk"]), cap_mbps)
            ncls = "bad" if nicp > 70 else ("warn" if nicp > 40 else "")
            rxcell = f'<span class="pill {ncls}">{f1(d["rx_pk"])}</span>' if ncls else f1(d["rx_pk"])
            h.append(f'<tr><td class="mono">{n}</td>'
                     f'<td class="num">{rxcell}</td><td class="num">{f1(d["rx_av"])}</td>'
                     f'<td class="num">{f1(d["tx_pk"])}</td><td class="num">{f1(d["tx_av"])}</td>'
                     f'<td class="num">{n0(d["rp_pk"])}</td><td class="num">{n0(d["tp_pk"])}</td>'
                     f'<td class="num">{ct}</td>'
                     f'<td class="num">{n0(d["fg"]) if d["fg"] else "–"}</td>'
                     f'<td class="num">{n0(d["al"]) if d["al"] else "–"}</td>'
                     f'<td class="num">{shcell}</td>'
                     f'<td class="num">{gwf}</td></tr>')
        h.append(f'<tr class="sub"><td>{gname.replace("&amp;","&")} subtotal '
                 f'<span class="muted">({len(rws)} nodes)</span></td>'
                 f'<td class="num">{f1(srxp)}</td><td class="num"></td>'
                 f'<td class="num">{f1(stxp)}</td><td class="num"></td>'
                 f'<td class="num"></td><td class="num"></td><td class="num"></td>'
                 f'<td class="num">{n0(sfg) if sfg else "–"}</td>'
                 f'<td class="num">{n0(sal) if sal else "–"}</td>'
                 f'<td class="num"></td><td class="num"></td></tr>')
    h.append(f'<tr class="tot"><td>CLUSTER TOTAL <span class="muted">({len(w.nodes)} nodes)</span></td>'
             f'<td class="num">{f1(w.cl_rx_pk)}</td><td class="num"></td>'
             f'<td class="num">{f1(w.cl_tx_pk)}</td><td class="num"></td>'
             f'<td class="num"></td><td class="num"></td><td class="num"></td>'
             f'<td class="num">{n0(G_fg)}</td>'
             f'<td class="num">{n0(G_al)}</td>'
             f'<td class="num">100%</td>'
             f'<td class="num">{pct(G_fg, G_al):.0f}%</td></tr>')
    h.append('</table></div>')
    h.append('<p class="muted">Peak RX/TX in the group and cluster rows is the highest <i>simultaneous</i> '
             'total across those nodes, not the sum of each node\'s own peak — those happen at different '
             'moments, so summing them would overstate the load. For the same reason the three group peaks '
             'need not add up exactly to the cluster peak: each group peaks at its own moment. '
             f'NIC capacity assumed {nic_gbps:.0f} Gbps; a coloured RX peak is above 40% of that.</p>')

    # cluster network chart
    prx, ptx = w.chart_series_net()
    if prx or ptx:
        h.append('<h3>Cluster network throughput across the window</h3>')
        h.append('<div class="chart">' + svg_chart(
            [{"name": "RX", "color": S1, "pts": prx, "area": True},
             {"name": "TX", "color": S2, "pts": ptx}],
            ylab="Mbps", xfmt=xfmt_for(w)) + "</div>")
        h.append(legend([("RX — into the nodes", S1), ("TX — out of the nodes", S2)]))
    return "".join(h)


def sec_balance(w):
    h = ['<h2>4 · Is the load spread evenly across the workers?</h2>']
    if not w.wnodes:
        return "".join(h) + '<p class="muted">No worker-node request data in this window.</p>'
    n = len(w.wnodes)
    wtot = sum(d["al"] for _, d in w.wnodes)
    even = wtot / n if n else 0
    h.append('<p class="muted">Measured on <b>every request the pods on each node served</b>, not just the ones '
             'arriving straight from the gateway — a node running only internally-called services would otherwise '
             'read as idle while doing the most work in the cluster. If replicas were spread evenly, each worker '
             'would take about the same share. A wide spread is a <b>pod placement</b> problem, not a '
             'load-balancer problem.</p>')
    h.append('<div class="grid">')
    h.append(kpi("Worker nodes serving", str(n), "", "nodes that served requests"))
    h.append(kpi("Even share would be", n0(even), "", "requests per node"))
    h.append(kpi("Busiest node", n0(w.w_max), "warn" if w.w_mean and w.w_max > 1.5 * w.w_mean else "",
                 f"{w.w_max/w.w_mean:.2f}× the mean" if w.w_mean else ""))
    h.append(kpi("Quietest node", n0(w.w_min), "",
                 f"{w.w_min/w.w_mean:.2f}× the mean" if w.w_mean else ""))
    skcls = "bad" if w.w_skew > 3 else ("warn" if w.w_skew > 1.6 else "good")
    h.append(kpi("Spread (max ÷ min)", f"{w.w_skew:.2f}×", skcls,
                 "target under 1.6× with maxSkew=1"))
    h.append("</div>")
    h.append('<div class="tbl-wrap"><table><tr><th>Node</th><th class="num">Requests served</th>'
             '<th class="num">Share</th><th class="num">vs mean</th>'
             '<th class="num">from gateway</th><th class="num">GW-facing</th>'
             '<th>Reading</th></tr>')
    for nm, d in w.wnodes:
        r = (d["al"] / w.w_mean) if w.w_mean else 0
        if r > 1.5:
            cls, txt = "bad", "hot — too many busy replicas here"
        elif r > 1.2:
            cls, txt = "warn", "above its share"
        elif r < 0.6:
            cls, txt = "mut", "under-used — room for a replica"
        else:
            cls, txt = "good", "within a fair band"
        bw = max(2, int(round(pct(d["al"], w.w_max) * 0.9)))
        h.append(f'<tr><td class="mono">{nm}</td><td class="num">{n0(d["al"])}</td>'
                 f'<td class="num"><span class="bar {"" if cls=="good" else cls}" '
                 f'style="width:{bw}px"></span> {pct(d["al"], wtot):.1f}%</td>'
                 f'<td class="num"><span class="pill {cls}">{r:.2f}×</span></td>'
                 f'<td class="num">{n0(d["fg"])}</td>'
                 f'<td class="num">{pct(d["fg"], d["al"]):.0f}%</td>'
                 f'<td class="muted">{txt}</td></tr>')
    h.append("</table></div>")
    if w.w_skew > 1.6:
        h.append('<div class="verdict warn"><b>The workers are not sharing the load evenly.</b> '
                 'Gateway distribution is fine — the imbalance comes from where the replicas sit. Check that the '
                 '<span class="mono">topologySpreadConstraints</span> label selectors actually match the pod '
                 'labels (a selector that matches nothing is silently inert), keep '
                 '<span class="mono">maxSkew: 1</span>, and use '
                 '<span class="mono">whenUnsatisfiable: ScheduleAnyway</span> so a skew never becomes a '
                 'Pending pod.</div>')
    else:
        h.append('<div class="verdict good"><b>Load is spread within a fair band across the workers.</b> '
                 'Pod placement is doing its job for this window.</div>')
    h.append('<p class="muted"><b>GW-facing</b> is how much of a node\'s work arrived straight from the ingress '
             'gateway. A low figure is not a problem — it just means that node runs services other services call. '
             f'Cluster-wide each external request turns into {w.ew_ratio:.2f} request-hops inside the mesh, which '
             'is why the totals here are larger than the gateway ingress count.</p>')
    return "".join(h)


def sec_watch(w, nic_gbps):
    h = ['<h2>5 · Saturation watch</h2>']
    cap = nic_gbps * 1000.0
    items = []
    # bandwidth
    worst = max(w.nodes.items(), key=lambda kv: max(kv[1]["rx_pk"], kv[1]["tx_pk"])) if w.nodes else None
    if worst:
        nm, d = worst
        wp = pct(max(d["rx_pk"], d["tx_pk"]), cap)
        cls = "bad" if wp > 70 else ("warn" if wp > 40 else "good")
        items.append((cls, f'<b>NIC headroom.</b> Busiest single node is <span class="mono">{nm}</span> at '
                           f'{f1(max(d["rx_pk"], d["tx_pk"]))} Mbps peak — {wp:.1f}% of an assumed '
                           f'{nic_gbps:.0f} Gbps link. '
                           + ("Bandwidth is not a constraint in this window."
                              if wp <= 40 else
                              "Worth confirming the actual link speed with <span class=\"mono\">ethtool</span> "
                              "before the next scale-up.")))
    # conntrack
    if w.ct_pk:
        cn, cd = max(w.nodes.items(), key=lambda kv: kv[1]["ct_pk"])
        cls = "bad" if cd["ct_pk"] > 100000 else ("warn" if cd["ct_pk"] > 60000 else "good")
        items.append((cls, f'<b>Conntrack.</b> Peak {n0(cd["ct_pk"])} tracked connections on '
                           f'<span class="mono">{cn}</span>. Compare against that node\'s limit '
                           f'(<span class="mono">sysctl net.netfilter.nf_conntrack_max</span>) — when entries '
                           f'approach the limit the kernel drops new connections and you see them as client-side '
                           f'timeouts, not as application errors.'))
    # packet rate
    if w.nodes:
        rn, rd = max(w.nodes.items(), key=lambda kv: kv[1]["rp_pk"])
        items.append(("", f'<b>Packet rate.</b> Highest RX packet rate is {n0(rd["rp_pk"])} pkt/s on '
                          f'<span class="mono">{rn}</span>. Well inside what a modern NIC handles; listed so a '
                          f'later run can be compared against it.'))
    # reconciliation
    cls = "good" if w.recon_pct < 1 else ("warn" if w.recon_pct < 3 else "bad")
    items.append((cls, f'<b>Counter reconciliation.</b> Gateway {n0(w.total)} vs pods {n0(w.node_fg)} — '
                       f'{w.recon_pct:.2f}% apart. Under 1% is normal sampling skew between the two snapshot '
                       f'batches; above 3% means requests really were accepted and not served.'))
    # east-west
    if w.ew_ratio > 1:
        cls = "warn" if w.ew_ratio > 4 else ""
        items.append((cls, f'<b>Internal fan-out.</b> Each external request produces {w.ew_ratio:.2f} '
                           f'request-hops inside the mesh. That multiplier is what turns a modest external rate '
                           f'into heavy sidecar CPU and connection churn — it is the number to watch if you '
                           f'reduce the external rate but CPU does not fall with it.'))
    h.append('<ul class="findings">')
    for cls, txt in items:
        h.append(f'<li class="{cls}">{txt}</li>')
    h.append("</ul>")
    return "".join(h)


def sec_compare(wins):
    h = ['<h2>6 · Windows side by side</h2>']
    h.append('<p class="muted">Same measurement, three windows — this is the table to put in front of the LB '
             'team alongside their own figures.</p>')
    rows = [
        ("Date", lambda w: (w.start.split(" ")[0] if w.start else "–")),
        ("Start (IST)", lambda w: short_ts(w.start)),
        ("End (IST)", lambda w: short_ts(w.end)),
        ("Duration (min)", lambda w: f1(w.duration / 60)),
        ("OCP received", lambda w: n0(w.total)),
        ("Average req/s", lambda w: f1(w.avg_rps)),
        ("Steady-state req/s", lambda w: f1(w.steady_avg)),
        ("Peak req/s", lambda w: f1(w.peak_rps)),
        ("Delivered to pods", lambda w: n0(w.node_fg)),
        ("Gateway↔pods gap", lambda w: f"{w.recon_pct:.2f}%"),
        ("All pod requests (incl. e-w)", lambda w: n0(w.node_al)),
        ("East-west multiplier", lambda w: f"{w.ew_ratio:.2f}×"),
        ("Cluster RX peak (Mbps)", lambda w: f1(w.cl_rx_pk)),
        ("Cluster TX peak (Mbps)", lambda w: f1(w.cl_tx_pk)),
        ("Worker spread (max÷min)", lambda w: f"{w.w_skew:.2f}×"),
        ("Peak conntrack", lambda w: n0(w.ct_pk)),
    ]
    h.append('<div class="tbl-wrap"><table><tr><th>Metric</th>' +
             "".join(f'<th class="num">{w.tag}</th>' for w in wins) + "</tr>")
    for name, fn in rows:
        h.append(f'<tr><td><b>{name}</b></td>' +
                 "".join(f'<td class="num">{fn(w)}</td>' for w in wins) + "</tr>")
    h.append("</table></div>")

    # overlay chart of rate
    cols = [S1, S2, S3, S4, S5]
    series, leg = [], []
    for i, w in enumerate(wins):
        c = cols[i % len(cols)]
        series.append({"name": w.tag, "color": c, "pts": w.rate_pts, "area": (i == 0)})
        leg.append((f"{w.tag} — {n0(w.total)} requests", c))
    longest = max(wins, key=lambda w: w.duration)
    h.append('<h3>Request rate — all windows overlaid on elapsed time</h3>')
    h.append('<div class="chart">' + svg_chart(series, ylab="req/s",
                                               xfmt=xfmt_for(longest)) + "</div>")
    h.append(legend(leg))
    return "".join(h)


def sec_raw(wins):
    h = ['<h2>7 · Raw samples</h2>']
    for w in wins:
        h.append(f'<details><summary>{w.tag} — gateway samples ({w.samples} rows from '
                 f'{os.path.basename(w.tfile or "?")})</summary><div class="tbl-wrap"><table>'
                 '<tr><th>Time (IST)</th><th class="num">Elapsed s</th><th class="num">req/s</th>'
                 '<th class="num">Cumulative</th></tr>')
        for r in w.traffic:
            h.append(f'<tr><td class="mono">{short_ts(r.get("ts_ist",""))}</td>'
                     f'<td class="num">{n0(r.get("elapsed_s"))}</td>'
                     f'<td class="num">{f1(r.get("rate_rps"))}</td>'
                     f'<td class="num">{n0(r.get("total_since_start"))}</td></tr>')
        h.append("</table></div></details>")
        h.append(f'<details><summary>{w.tag} — per-node final values '
                 f'({len(w.nodes)} nodes from {os.path.basename(w.nfile or "?")})</summary>'
                 '<div class="tbl-wrap"><table><tr><th>Node</th><th>Group</th>'
                 '<th class="num">RX pk</th><th class="num">TX pk</th><th class="num">RX pps pk</th>'
                 '<th class="num">TX pps pk</th><th class="num">conntrack pk</th>'
                 '<th class="num">FROM GW</th><th class="num">ALL RCVD</th></tr>')
        gname = {0: "master/infra", 1: "worker", 2: "kafka"}
        for nm, d in sorted(w.nodes.items(), key=lambda kv: (kv[1]["g"], kv[0])):
            h.append(f'<tr><td class="mono">{nm}</td><td class="muted">{gname[d["g"]]}</td>'
                     f'<td class="num">{f1(d["rx_pk"])}</td><td class="num">{f1(d["tx_pk"])}</td>'
                     f'<td class="num">{n0(d["rp_pk"])}</td><td class="num">{n0(d["tp_pk"])}</td>'
                     f'<td class="num">{n0(d["ct_pk"])}</td>'
                     f'<td class="num">{n0(d["fg"])}</td><td class="num">{n0(d["al"])}</td></tr>')
        h.append("</table></div></details>")
    return "".join(h)


# ---------------------------------------------------------------- verdict ---
def verdict(wins):
    w = wins[0]
    bits = []
    if len(wins) == 1:
        bits.append(f"OCP received <b>{n0(w.total)} requests</b> in the "
                    f"{f1(w.duration/60)} min window ({f1(w.avg_rps)} req/s average, "
                    f"{f1(w.peak_rps)} req/s peak).")
    else:
        tot = sum(x.total for x in wins)
        bits.append(f"Across {len(wins)} windows OCP received <b>{n0(tot)} requests</b> "
                    f"(peak {f1(max(x.peak_rps for x in wins))} req/s).")
    cls = "good"
    worst_recon = max(x.recon_pct for x in wins)
    if worst_recon < 1:
        bits.append("Gateway and pod counters reconcile to within 1%, so the ingress total is trustworthy "
                    "as the authoritative count of what reached the cluster.")
    elif worst_recon < 3:
        bits.append(f"Gateway and pod counters differ by up to {worst_recon:.2f}% — small enough to be "
                    "snapshot skew, but check gateway 5xx if it grows.")
        cls = "warn"
    else:
        bits.append(f"Gateway and pod counters differ by {worst_recon:.2f}% — requests were accepted at the "
                    "edge but not served by pods. Investigate before quoting either number.")
        cls = "bad"
    sk = max(x.w_skew for x in wins)
    if sk > 3:
        bits.append(f"Worker load is uneven — the busiest node took {sk:.1f}× the quietest, which is pod "
                    "placement, not load balancing.")
        cls = "bad" if cls != "bad" else cls
    elif sk > 1.6:
        bits.append(f"Worker load is somewhat uneven ({sk:.2f}× spread between busiest and quietest node).")
        if cls == "good":
            cls = "warn"
    else:
        bits.append(f"Worker load is evenly spread ({sk:.2f}× spread).")
    ct = max(x.ct_pk for x in wins)
    if ct > 100000:
        bits.append(f"Conntrack peaked at {n0(ct)} entries — check the kernel limit on that node.")
        if cls == "good":
            cls = "warn"
    if any(x.bad_nodes for x in wins):
        bits.append("Note: the capture could not map every pod to a node (see section 3) — per-node figures "
                    "are short by those pods.")
        cls = "bad"
    nicw = max((max(d["rx_pk"], d["tx_pk"]) for x in wins for d in x.nodes.values()), default=0.0)
    if nicw:
        bits.append(f"Busiest NIC peaked at {f1(nicw)} Mbps, so bandwidth is not the limiting factor.")
    return cls, " ".join(bits)


# ------------------------------------------------------------------ build ---
def build(wins, nic_gbps):
    utc = datetime.datetime.now(datetime.timezone.utc)
    ist = utc + datetime.timedelta(hours=5, minutes=30)
    now = ist.strftime("%Y-%m-%d %H:%M IST") + " / " + utc.strftime("%H:%M UTC")
    vcls, vtxt = verdict(wins)
    tags = ", ".join(x.tag for x in wins)

    H = ['<!doctype html><html><head><meta charset="utf-8">'
         '<meta name="viewport" content="width=device-width, initial-scale=1">'
         '<title>PERF Network Report</title>', CSS, '</head><body><div class="wrap">']
    H.append('<div class="masthead">'
             '<div class="kick">Network &amp; Traffic · OCP Ingress Measurement</div>'
             '<h1>SBI ePay 2.0 — PERF</h1>'
             f'<div class="sub">Prepared by Saadi M Muzzammil · generated {now}</div>'
             f'<div class="sub">Windows: {esc(tags)} · '
             f'source: perf-all.sh gateway counters + node-exporter + istio sidecars</div>'
             '<div class="sub" style="margin-top:11px">'
             '<a href="https://oneviewuat.sbi/" target="_blank" rel="noopener" '
             'style="margin-right:18px">Dynatrace ↗</a>'
             '<a href="https://console-openshift-console.apps.preprod.epay.sbi/" target="_blank" '
             'rel="noopener" style="margin-right:18px">OCP Console ↗</a>'
             '<a href="http://10.177.139.68:3000/" target="_blank" rel="noopener">Grafana ↗</a>'
             '</div></div>')
    multi = len(wins) > 1
    if multi:
        nav = '<div class="navbtns"><a href="#cmp">Compare all</a>'
        nav += "".join(f'<a href="#w-{w.tag}">Window · {w.tag}</a>' for w in wins)
        nav += '<a href="#raw">Raw samples</a></div>'
    else:
        nav = ('<div class="navbtns"><a href="#s1">1 · Window</a><a href="#s2">2 · Funnel</a>'
               '<a href="#s3">3 · Nodes</a><a href="#s4">4 · Balance</a>'
               '<a href="#s5">5 · Saturation</a><a href="#raw">6 · Raw</a></div>')
    H.append(nav)
    H.append(f'<div class="verdict {vcls}"><b>Summary.</b> {vtxt}</div>')

    def band(t, s):
        return f'<div class="groupband"><h2 class="group">{t}</h2><span class="gsub">{s}</span></div>'

    if multi:
        H.append('<span id="cmp"></span>' + band("Comparison", "All windows together"))
        H.append(sec_compare(wins))

    for w in wins:
        if multi:
            H.append(f'<span id="w-{w.tag}"></span>' +
                     band(f"Window · {w.tag}",
                          f'{short_ts(w.start)} → {short_ts(w.end)} IST · {n0(w.total)} requests'))
            pre = ""
        else:
            H.append(band("How much traffic reached OCP",
                          "Gateway ingress, the funnel against the LB, and where it landed"))
            pre = '<span id="s%d"></span>'
        for i, body in enumerate((sec_window(w), sec_funnel(w), sec_nodes(w, nic_gbps),
                                  sec_balance(w), sec_watch(w, nic_gbps)), start=1):
            H.append((pre % i if pre else "") + body)

    H.append('<span id="raw"></span>' + band("Appendix", "Every sample, as captured"))
    H.append(sec_raw(wins))
    H.append('<p class="muted" style="text-align:center;margin-top:40px;'
             'border-top:1px solid var(--line);padding-top:14px">'
             '© Saadi M Muzzammil · read-only · generated from perf-all.sh CSVs.</p>')
    H.append("</div></body></html>")
    return "".join(H)


def main():
    ap = argparse.ArgumentParser(description="HTML report from perf-all.sh network CSVs")
    ap.add_argument("--tag", action="append", default=[],
                    help="window tag; finds traffic_<tag>.csv and nodenet_<tag>.csv (repeatable)")
    ap.add_argument("--dir", default=".", help="directory holding the CSVs (default .)")
    ap.add_argument("--traffic", help="explicit traffic CSV (single window)")
    ap.add_argument("--nodenet", help="explicit nodenet CSV (single window)")
    ap.add_argument("--jmeter", action="append", default=[], help="JMeter total per window")
    ap.add_argument("--avlb-recv", action="append", default=[], help="AVLB received per window")
    ap.add_argument("--avlb-fwd", action="append", default=[], help="AVLB forwarded per window")
    ap.add_argument("--nic-gbps", type=float, default=10.0, help="node NIC speed for headroom maths")
    ap.add_argument("-o", default="NET_Report.html")
    a = ap.parse_args()

    specs = []
    if a.traffic or a.nodenet:
        tag = a.tag[0] if a.tag else "window"
        specs.append((tag, a.traffic, a.nodenet))
    for i, t in enumerate(a.tag):
        if a.traffic or a.nodenet:
            break
        specs.append((t, os.path.join(a.dir, f"traffic_{t}.csv"),
                      os.path.join(a.dir, f"nodenet_{t}.csv")))
    if not specs:
        # auto-discover: every traffic_*.csv with a matching nodenet_*.csv
        found = []
        for f in sorted(os.listdir(a.dir)):
            if f.startswith("traffic_") and f.endswith(".csv"):
                t = f[len("traffic_"):-4]
                nf = os.path.join(a.dir, f"nodenet_{t}.csv")
                if os.path.exists(nf):
                    found.append((t, os.path.join(a.dir, f), nf))
        specs = found
    if not specs:
        print("ERROR: no window found. Pass --tag <tag>, or --traffic/--nodenet, "
              "or run from the directory holding traffic_*.csv + nodenet_*.csv")
        raise SystemExit(1)

    wins = []
    for i, (tag, tf, nf) in enumerate(specs):
        lb = {}
        if i < len(a.jmeter):
            lb["jmeter"] = a.jmeter[i]
        if i < len(a.avlb_recv):
            lb["avlb_recv"] = a.avlb_recv[i]
        if i < len(a.avlb_fwd):
            lb["avlb_fwd"] = a.avlb_fwd[i]
        w = Window(tag, tf, nf, lb)
        if not w.ok_traffic and not w.ok_nodes:
            print(f"  [skip] {tag}: both CSVs missing or empty")
            continue
        wins.append(w)
        print(f"  [window] {tag}: {w.samples} gateway samples, {len(w.nodes)} nodes, "
              f"{int(w.total):,} requests")
    if not wins:
        print("ERROR: nothing to report on.")
        raise SystemExit(1)

    with open(a.o, "w") as f:
        f.write(build(wins, a.nic_gbps))
    print(f"[net-report] wrote {a.o}")


if __name__ == "__main__":
    main()
