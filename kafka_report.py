#!/usr/bin/env python3
# ============================================================================
# kafka_report.py — HTML report from the perf-kafka.sh capture
# SBI ePay 2.0 PERF · Saadi M Muzzammil
# ----------------------------------------------------------------------------
#   python3 kafka_report.py --tag k1 -o KAFKA_k1.html
#   python3 kafka_report.py --dir <run folder>              (auto-discovers tags)
#
# Reads kafka_cluster/broker/topic/latency/jvm/pod/disk/lag/health_<tag>.csv.
# Every rate is taken as the collector measured it; totals and peaks are
# recomputed here from the rows so the report never depends on a summary line.
# A metric the JMX exporter never published stays blank — it is never shown as 0.
# ============================================================================
import os, csv, argparse, datetime, statistics as st

# Bumped whenever the report's judgement changes, so a file can always be traced
# to the build that produced it. Printed in the masthead and by --version.
KR_VERSION = "2026-10-09.7  (frozen-series verdicts + at-min-ISR check)"

def read(p):
    if not p or not os.path.exists(p):
        return []
    with open(p, newline="") as f:
        return [r for r in csv.DictReader(f) if r]

def F(v, d=None):
    try:
        if v is None or str(v).strip() == "":
            return d
        return float(v)
    except Exception:
        return d

def n0(v):
    f = F(v)
    return "–" if f is None else f"{f:,.0f}"

def f1(v, s="", d=1):
    f = F(v)
    return "–" if f is None else f"{f:,.{d}f}{s}"

def esc(t):
    return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def short_ts(t):
    return (t or "").split(" ")[-1] or "–"

S1, S2, S3, S4, S5 = "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"

# ---------------------------------------------------------------- sanity ----
# The report does not trust its input. A collector that resolved a cumulative
# counter where a gauge was wanted will hand over an idle percentage of
# 1,877,290,000,000,000 — and a report that renders that as "healthy headroom"
# is worse than one that renders nothing. Physically impossible values are
# rejected here and listed to the reader, never quietly passed through.
REJECTED = {}


def _reject(kind, val):
    REJECTED[kind] = REJECTED.get(kind, 0) + 1
    return None


def pct_ok(v):
    """a percentage must be 0..100"""
    if v is None:
        return None
    return v if -0.01 <= v <= 100.5 else _reject("idle/percentage", v)


# A Kafka request Max can legitimately reach tens of seconds (JoinGroup waits
# out the rebalance timeout). An hour cannot happen: that is a counter.
LAT_CEILING_MS = 3_600_000

# Below this, the queue-vs-local split is real but not worth acting on: a 73%
# queue share of a 4 ms request is scheduling noise, not a bottleneck.
MATERIAL_MS = 20.0


def ms_ok(v):
    if v is None:
        return None
    return v if 0 <= v <= LAT_CEILING_MS else _reject("request latency", v)


def nonneg(v):
    if v is None:
        return None
    return v if v >= 0 else _reject("negative value", v)


def lat_incoherent(t, q, l):
    """Queue time and local time are disjoint phases of ONE request, and each is
    a PART of total time. That gives three arithmetic facts no real measurement
    can break, and each one catches a different collector fault:

      queue > total        the columns came from different brokers
      local > total        same, or local was resolved to the total series
      queue + local > total the parts do not fit inside the whole
      queue = local = total one series was resolved three times

    A ceiling on magnitude only catches the gross cases; these catch a
    mis-resolved series at any size, which is what matters when the number
    looks plausible enough to quote.
    """
    if t is None or t <= 0:
        return False
    # The slack must cover the exporter's rounding and nothing else. It publishes
    # one decimal place, so 1 ms (or 0.1% on large values) is generous; a flat 1%
    # would quietly absorb a 46 ms violation on an 8-second value, which is
    # exactly the kind of impossible reading this check exists to catch.
    slack = max(1.0, t * 0.001)
    if q is not None and q > t + slack:
        _reject("queue time above total time", q)
        return True
    if l is not None and l > t + slack:
        _reject("local time above total time", l)
        return True
    if q is not None and l is not None:
        if q + l > t * 1.25:
            _reject("queue + local above total", q + l)
            return True
        if abs(q - t) / t < 0.01 and abs(l - t) / t < 0.01:
            _reject("latency queue/local split", t)
            return True
    return False


# per-column validator, applied wherever that column is read
_VALID = {"handler_idle_pct": pct_ok, "network_idle_pct": pct_ok,
          "used_pct": pct_ok, "total_ms": ms_ok, "queue_ms": ms_ok,
          "local_ms": ms_ok}


def _vals(rows, col):
    """every numeric column is read through here, so a value that cannot be
    physically true never reaches a peak, a mean, a chart or a verdict."""
    f = _VALID.get(col)
    out = []
    for r in rows:
        x = F(r.get(col))
        if x is None:
            continue
        if f:
            x = f(x)
            if x is None:
                continue
        out.append(x)
    return out


def one(row, col):
    """single-cell read, validated the same way as the aggregates"""
    x = F(row.get(col))
    if x is None:
        return None
    f = _VALID.get(col)
    return f(x) if f else x


def pk(rows, col):
    v = _vals(rows, col)
    return max(v) if v else None


def mn(rows, col):
    v = _vals(rows, col)
    return min(v) if v else None


def av(rows, col):
    v = _vals(rows, col)
    return st.mean(v) if v else None

CSS = """<style>
/* Theme: "broker" — charcoal chrome, amber accent. Deliberately distinct from the
   navy PERF report and the teal network report so three outputs never blur. */
:root{
  --bg:#f5f3f0; --card:#fff; --ink:#1c1917; --mut:#6b6058;
  --line:#e2dcd4; --band:#faf8f6; --band2:#f0ebe4;
  --head:#2e2a26; --head2:#453f38; --accent:#b45309; --accent-soft:#fdf2e3;
  --good-ink:#146c14; --warn-ink:#8a5d00; --bad-ink:#a92f2f;
  --good-bg:#e6f4e6; --warn-bg:#fdf1d8; --bad-bg:#fbe6e6;
  --grid:#ece6de; --axis:#c9bfb3;
  --shadow:0 1px 2px rgba(28,25,23,.05),0 3px 12px rgba(28,25,23,.06);
}
*{box-sizing:border-box}
body{font-family:system-ui,-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;margin:0;
background:var(--bg);color:var(--ink);font-size:14px;line-height:1.6;-webkit-font-smoothing:antialiased}
.wrap{max-width:1600px;margin:0 auto;padding:26px 40px 60px}
@media(max-width:640px){.wrap{padding:14px}}
h1{font-size:23px;margin:0;letter-spacing:-.01em}
.masthead{background:var(--head);color:#fff;padding:24px 28px;border-radius:2px;
border-top:4px solid var(--accent);position:relative;overflow:hidden}
.masthead::after{content:"";position:absolute;inset:0;pointer-events:none;
background:repeating-linear-gradient(90deg,rgba(255,255,255,.03) 0 2px,transparent 2px 14px)}
.masthead .kick{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;letter-spacing:.2em;
text-transform:uppercase;color:#f0b86e;margin-bottom:9px}
.masthead .sub{opacity:.82;font-size:12.5px;margin-top:7px}
.masthead a{color:#f5c98a;text-decoration:none;border-bottom:1px solid rgba(245,201,138,.35)}
.masthead a:hover{color:#fff;border-bottom-color:#fff}
.navbtns{display:flex;flex-wrap:wrap;gap:0;margin:0 0 6px;border-bottom:1px solid var(--line)}
.navbtns a{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px;padding:11px 14px;
color:var(--mut);text-decoration:none;border-bottom:2px solid transparent;margin-bottom:-1px}
.navbtns a:hover{color:var(--accent);border-bottom-color:var(--accent)}
h2{font-size:16px;color:var(--head);margin:30px 0 12px;scroll-margin-top:20px;padding:0 0 0 0;
border:none;border-bottom:2px solid var(--accent);display:inline-block;font-weight:700}
h3{font-size:11px;color:var(--mut);margin:20px 0 7px;text-transform:uppercase;letter-spacing:.12em;
font-weight:700;font-family:ui-monospace,Menlo,Consolas,monospace}
.groupband{margin:46px 0 4px;border-top:2px solid var(--head);padding-top:11px}
.groupband h2.group{display:block;font-size:19px;font-weight:700;color:var(--head);border:none;
padding:0;margin:0;letter-spacing:-.01em}
.groupband .gsub{display:block;color:var(--mut);font-size:12.5px;margin-top:3px}
.verdict{background:var(--card);border:1px solid var(--line);border-left:4px solid var(--accent);
border-radius:2px;padding:15px 19px;margin:16px 0;font-size:14.5px;box-shadow:var(--shadow)}
.verdict.good{border-left-color:var(--good-ink)}
.verdict.warn{border-left-color:var(--warn-ink)}
.verdict.bad{border-left-color:var(--bad-ink)}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(176px,1fr));gap:10px;margin:14px 0}
.kpi{background:var(--card);border:1px solid var(--line);border-top:3px solid var(--accent);
border-radius:2px;padding:13px 15px 14px;box-shadow:var(--shadow);min-width:0;overflow:hidden}
.kpi .lbl{font-size:10.5px;color:var(--mut);text-transform:uppercase;letter-spacing:.1em;
font-family:ui-monospace,Menlo,Consolas,monospace}
/* min-width:0 + wrapping stop one absurd value from widening the grid track
   and giving the whole page a horizontal scrollbar. */
.kpi .val{font-size:22px;font-weight:650;margin-top:5px;letter-spacing:-.02em;
min-width:0;overflow-wrap:anywhere;word-break:break-word}
.kpi .val.good{color:var(--good-ink)}.kpi .val.warn{color:var(--warn-ink)}.kpi .val.bad{color:var(--bad-ink)}
.kpi .val.sm{font-size:16px;letter-spacing:0;line-height:1.35}
.kpi .sub{font-size:11.5px;color:var(--mut);margin-top:3px;line-height:1.4}
.findings{list-style:none;padding:0;margin:10px 0}
.findings li{background:var(--card);border:1px solid var(--line);border-radius:2px;padding:11px 15px;
margin:6px 0;border-left:3px solid var(--accent);box-shadow:var(--shadow)}
.findings li.warn{border-left-color:var(--warn-ink)}
.findings li.bad{border-left-color:var(--bad-ink)}
.findings li.good{border-left-color:var(--good-ink)}
.findings li.mut{border-left-color:var(--axis);color:var(--mut)}
details{margin:10px 0;background:var(--card);border:1px solid var(--line);border-radius:2px;padding:2px 14px}
summary{cursor:pointer;color:var(--accent);font-size:12.5px;font-weight:600;padding:9px 0;
user-select:none;font-family:ui-monospace,Menlo,Consolas,monospace}
.tbl-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch}
table{width:100%;border-collapse:collapse;background:var(--card);border:1px solid var(--line);
border-radius:2px;margin:8px 0;font-size:13px;box-shadow:var(--shadow)}
th{background:var(--band2);color:var(--head);text-align:left;padding:9px 11px;font-size:11px;
white-space:nowrap;text-transform:uppercase;letter-spacing:.07em;font-weight:700;
border-bottom:2px solid var(--head);font-family:ui-monospace,Menlo,Consolas,monospace}
td{padding:7px 11px;border-top:1px solid var(--line);white-space:nowrap}
tr:nth-child(even) td{background:var(--band)}
tbody tr:hover td{background:var(--accent-soft)}
td.num,th.num{text-align:right;font-variant-numeric:tabular-nums}
tr.tot td{font-weight:700;color:var(--head);background:var(--accent-soft)!important;
border-top:2px solid var(--accent);border-bottom:2px solid var(--accent)}
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
.chart{background:var(--card);border:1px solid var(--line);border-radius:2px;padding:12px 8px 6px;
box-shadow:var(--shadow);margin:10px 0;overflow-x:auto}
.legend{font-size:12px;color:var(--mut);padding:2px 12px 8px}
.legend i{display:inline-block;width:10px;height:10px;border-radius:2px;margin:0 6px 0 16px;vertical-align:-1px}
.legend i:first-child{margin-left:0}
span[id]{scroll-margin-top:20px;display:block}
@media(max-width:640px){h2{font-size:14px}.kpi .val{font-size:19px}.navbtns a{padding:9px 10px;font-size:11px}}
@media print{.navbtns{display:none}body{background:#fff}.wrap{padding:0}details{border:none}
details summary{display:none}}
</style>"""


def _nice(v):
    import math
    if v <= 0:
        return 1.0
    e = math.floor(math.log10(v)); b = 10 ** e
    for m in (1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10):
        if v <= m * b:
            return m * b
    return 10 * b


def svg(series, width=1140, height=250, ylab="", xfmt=None):
    series = [s for s in series if s.get("pts")]
    if not series:
        return '<p class="muted">No samples to plot.</p>'
    PL, PR, PT, PB = 70, 16, 14, 34
    xs = [p[0] for s in series for p in s["pts"]]
    ys = [p[1] for s in series for p in s["pts"]]
    x0, x1 = min(xs), max(xs)
    if x1 <= x0:
        x1 = x0 + 1
    ytop = _nice(max(ys) if ys else 1)
    iw, ih = width - PL - PR, height - PT - PB
    X = lambda v: PL + (v - x0) / (x1 - x0) * iw
    Y = lambda v: PT + ih - (min(v, ytop) / ytop) * ih
    o = [f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" '
         f'preserveAspectRatio="xMidYMid meet" style="display:block">']
    for k in range(5):
        v = ytop * k / 4.0; y = Y(v)
        o.append(f'<line x1="{PL}" y1="{y:.1f}" x2="{width-PR}" y2="{y:.1f}" stroke="#ece6de" stroke-width="1"/>')
        o.append(f'<text x="{PL-8}" y="{y+4:.1f}" font-size="11" fill="#6b6058" text-anchor="end">'
                 f'{v:,.0f}' if ytop >= 10 else f'{v:,.1f}')
        o[-1] += '</text>'
    for k in range(7):
        v = x0 + (x1 - x0) * k / 6.0; x = X(v)
        o.append(f'<line x1="{x:.1f}" y1="{PT+ih}" x2="{x:.1f}" y2="{PT+ih+4}" stroke="#c9bfb3" stroke-width="1"/>')
        lab = xfmt(v) if xfmt else f"{v:,.0f}s"
        o.append(f'<text x="{x:.1f}" y="{PT+ih+18}" font-size="11" fill="#6b6058" text-anchor="middle">{lab}</text>')
    if ylab:
        o.append(f'<text x="4" y="{PT+10}" font-size="11" fill="#6b6058">{ylab}</text>')
    for idx, s in enumerate(series):
        pts = sorted(s["pts"]); col = s.get("color", S1)
        d = " ".join(f"{X(a):.1f},{Y(b):.1f}" for a, b in pts)
        if s.get("area"):
            o.append(f'<polygon points="{X(pts[0][0]):.1f},{PT+ih:.1f} {d} {X(pts[-1][0]):.1f},{PT+ih:.1f}" '
                     f'fill="{col}" opacity="0.12"/>')
        o.append(f'<polyline points="{d}" fill="none" stroke="{col}" stroke-width="2.1" '
                 f'stroke-linejoin="round" stroke-linecap="round"/>')
        px, py = max(pts, key=lambda p: p[1])
        o.append(f'<circle cx="{X(px):.1f}" cy="{Y(py):.1f}" r="4" fill="{col}" stroke="#fff" stroke-width="2"/>')
        tx = min(max(X(px), PL + 34), width - PR - 34)
        o.append(f'<text x="{tx:.1f}" y="{max(Y(py)-11-14*idx, PT+10):.1f}" font-size="11" font-weight="700" '
                 f'fill="{col}" text-anchor="middle">{py:,.0f}</text>')
    xset = sorted({a for s in series for a, _ in s["pts"]})
    if 1 < len(xset) <= 800:
        bw = iw / len(xset)
        lut = [dict(s["pts"]) for s in series]
        for xv in xset:
            parts = [f'{(s.get("name") or "v")} {m[xv]:,.1f}' for s, m in zip(series, lut) if xv in m]
            lbl = xfmt(xv) if xfmt else f"{xv:,.0f}s"
            o.append(f'<rect x="{X(xv)-bw/2:.1f}" y="{PT}" width="{max(bw,1):.2f}" height="{ih:.1f}" '
                     f'fill="transparent"><title>{esc(lbl + "  —  " + ", ".join(parts))}</title></rect>')
    o.append(f'<line x1="{PL}" y1="{PT+ih}" x2="{width-PR}" y2="{PT+ih}" stroke="#c9bfb3" stroke-width="1"/>')
    o.append("</svg>")
    return "".join(o)


def legend(items):
    return '<div class="legend">' + "".join(
        f'<i style="background:{c}"></i>{n}' for n, c in items) + "</div>"


def kpi(lbl, val, cls="", sub=""):
    s = f'<div class="sub">{sub}</div>' if sub else ""
    return f'<div class="kpi"><div class="lbl">{lbl}</div><div class="val {cls}">{val}</div>{s}</div>'


# --------------------------------------------------------------------- data --
class Win:
    def __init__(self, tag, d):
        self.tag = tag
        # run_info.txt is written by the collector. It carries the build identity
        # and — critically — whether the latency series are real percentiles or
        # Max-since-start, so the report never labels a Max as a p99.
        self.info = {}
        ri = os.path.join(d, "run_info.txt")
        if os.path.exists(ri):
            try:
                with open(ri) as fh:
                    for ln in fh:
                        if "=" in ln:
                            k, v = ln.split("=", 1)
                            self.info[k.strip()] = v.strip()
            except OSError:
                pass
        self.lat_kind = self.info.get("latency_kind", "")
        self.lat_label = {"p99": "p99", "max": "max"}.get(self.lat_kind, "peak")
        g = lambda n: read(os.path.join(d, f"kafka_{n}_{tag}.csv"))
        self.clu, self.brk, self.top = g("cluster"), g("broker"), g("topic")
        self.lat, self.jvm, self.pod = g("latency"), g("jvm"), g("pod")
        self.dsk, self.lag, self.hlt = g("disk"), g("lag"), g("health")
        self.ok = bool(self.clu or self.brk or self.pod)
        c = self.clu
        self.samples = len(c)
        self.start = c[0]["ts_ist"] if c else (self.pod[0]["ts_ist"] if self.pod else "")
        self.end = c[-1]["ts_ist"] if c else (self.pod[-1]["ts_ist"] if self.pod else "")
        self.dur = F(c[-1].get("elapsed_s"), 0) if c else 0
        self.brokers = sorted({r["pod"] for r in self.brk}) if self.brk else \
                       sorted({r["pod"] for r in self.pod if r.get("role") == "broker"})
        self.ctrls = sorted({r["pod"] for r in self.pod if r.get("role") == "controller"})
        # cluster peaks
        self.pk_in, self.pk_out = pk(c, "bytes_in_mb_s"), pk(c, "bytes_out_mb_s")
        self.av_in, self.av_out = av(c, "bytes_in_mb_s"), av(c, "bytes_out_mb_s")
        self.pk_msg, self.av_msg = pk(c, "messages_in_s"), av(c, "messages_in_s")
        self.max_urp = pk(c, "under_replicated")
        self.max_off = pk(c, "offline_partitions")
        self.max_uncl = pk(c, "unclean_elections")
        ctl = [F(r.get("active_controllers")) for r in c]
        ctl = [x for x in ctl if x is not None]
        self.ctl_bad = [x for x in ctl if x != 1]
        self.have_ctl = bool(ctl)
        self.min_idle = mn(self.brk, "handler_idle_pct")
        self.min_net = mn(self.brk, "network_idle_pct")
        self.max_atminisr = pk(self.brk, "at_min_isr")
        self.max_reqq = pk(self.brk, "request_queue")
        self.max_purg = pk(self.brk, "purgatory")
        self.max_lag = None
        if self.lag:
            per = {}
            for r in self.lag:
                per[r["ts_ist"]] = per.get(r["ts_ist"], 0) + (F(r.get("lag"), 0) or 0)
            self.max_lag = max(per.values()) if per else None
            self.lag_pts = sorted((F(r["elapsed_s"], 0), v) for r, v in
                                  [(x, per[x["ts_ist"]]) for x in self.lag])
            seen = {}
            for e, v in self.lag_pts:
                seen[e] = v
            self.lag_pts = sorted(seen.items())
        else:
            self.lag_pts = []
        self.max_pvc = pk(self.dsk, "used_pct")
        self.restarts = pk(self.pod, "restarts")
        # Replica count below leader count is arithmetically impossible: every
        # leader IS a replica. 8 partitions / 163 leaders means the partition
        # series was mis-resolved, so neither figure may be quoted.
        self.parts_tot = pk(c, "partitions_total")
        self.leads_tot = pk(c, "leaders_total")
        self.part_bad = (self.parts_tot is not None and self.leads_tot is not None
                         and self.leads_tot > 0 and self.parts_tot < self.leads_tot)
        if self.part_bad:
            _reject("partition / leader counts", self.parts_tot)
            self.parts_tot = None

        # ---- cross-checks between series that must agree with each other ----
        # A single series can look perfectly reasonable on its own and still be
        # the wrong series. These compare independent measurements that describe
        # the same physical thing, which is the only way to catch that.
        self.xchecks = []

        # Request-handler threads are fed by the network threads. If the network
        # threads really were starved, the handlers would be starved too — so
        # abundant handler headroom beside a starved network pool means one of
        # the two series is not what it claims to be.
        if (self.min_idle is not None and self.min_idle >= 50
                and self.min_net is not None and self.min_net < 30):
            self.xchecks.append(
                f"Request handlers report {f1(self.min_idle)}% idle while the network "
                f"processors report {f1(self.min_net)}% idle. Those two pools feed each other, so "
                "one cannot sit nearly idle while the other is starved — one of the two series is "
                "mis-resolved. The handler figure is the one the saturation verdict uses, so treat "
                "the network-idle column as unverified.")

        # Kafka's RequestHandlerAvgIdlePercent is a Meter, and the JMX exporter
        # publishes its mean-since-broker-start alongside the 1/5/15-minute
        # rates. Pick the since-start mean and the number barely moves: on a
        # broker up for days it is dominated by idle time, so it reads ~99% no
        # matter what the load test does. A value that is byte-identical in
        # every sample for every broker is that frozen mean, not a measurement
        # of this window — and it would quietly invalidate the whole saturation
        # verdict, which is the one number this report exists to produce.
        self.idle_frozen = False
        if self.brk and self.samples >= 4:
            bper = {}
            for r in self.brk:
                bper.setdefault(r["pod"], []).append((r.get("handler_idle_pct") or "").strip())
            vals = [v for v in bper.values() if len([x for x in v if x]) >= 4]
            if vals and all(len(set(v)) == 1 and v[0] for v in vals):
                self.idle_frozen = True
                self.xchecks.append(
                    "Request-handler idle is byte-identical in every sample for every broker "
                    f"({', '.join(sorted({v[0] + '%' for v in vals}))}). A live gauge varies; this is "
                    "the exporter's <b>mean since broker start</b>, which on a long-running broker is "
                    "dominated by idle time and barely moves whatever the load does. It does not "
                    "describe this window, so <b>no saturation conclusion can be drawn from it</b>. "
                    "Check for a 1-minute variant with "
                    "<span class=\"mono\">./perf-kafka.sh --metrics idle</span> and prefer that series.")

        # A JVM whose heap falls between samples has just run a collection. If
        # the GC rate is identically zero everywhere, the GC series was never
        # resolved — and a report showing "0.00 GC/s" would read as a healthy JVM.
        self.gc_unmeasured = False
        if self.jvm:
            gv = _vals(self.jvm, "gc_count_s")
            gc_zero = bool(gv) and max(gv) == 0
            pj = {}
            for r in self.jvm:
                pj.setdefault(r.get("pod", ""), []).append(r)
            fell = False
            for v in pj.values():
                seq = [F(x.get("heap_used_mb")) for x in
                       sorted(v, key=lambda y: F(y.get("elapsed_s"), 0) or 0)]
                seq = [x for x in seq if x is not None]
                if any(b < a - 1 for a, b in zip(seq, seq[1:])):
                    fell = True
                    break
            if gc_zero and fell:
                self.gc_unmeasured = True
                self.xchecks.append(
                    "Heap usage fell between samples, so the JVMs did run garbage collections, "
                    "yet the GC rate reads exactly 0.00/s for every pod in every sample. The GC "
                    "series was not resolved — it is shown as not measured rather than as zero.")

        # Heap is allocated inside the container, so it cannot exceed the
        # container's resident memory.
        pm, ph = {}, {}
        for r in self.pod:
            v = F(r.get("mem_mi"))
            if v is not None:
                pm[r["pod"]] = max(pm.get(r["pod"], 0), v)
        for r in self.jvm:
            v = F(r.get("heap_used_mb"))
            if v is not None:
                ph[r["pod"]] = max(ph.get(r["pod"], 0), v)
        over = [p for p in ph if p in pm and ph[p] > pm[p] * 1.02]
        if over:
            _reject("heap above container memory", ph[over[0]])
            self.xchecks.append(
                f"{len(over)} pod(s) report a JVM heap larger than the container's resident "
                "memory, which is impossible — heap lives inside that memory. The heap series is "
                "mis-resolved for those pods.")
            self.heap_bad = set(over)
        else:
            self.heap_bad = set()

        # ---- did any load actually reach this cluster? ----
        # Internal topics carry traffic whether or not a test is running:
        # __consumer_offsets takes a commit from every idle consumer, and the
        # strimzi.cruisecontrol.* topics are Cruise Control talking to itself. A
        # window where only those moved recorded an idle cluster, however many
        # MB/s the headline shows.
        def internal(t):
            return t.startswith("__") or t.startswith("strimzi.") or t.startswith("_schemas")

        self.app_topics = []
        tper = {}
        for r in self.top:
            tper.setdefault(r.get("topic", ""), []).append(r)
        for t, rs in tper.items():
            if internal(t):
                continue
            if (pk(rs, "messages_in_s") or 0) > 0 or (pk(rs, "bytes_in_mb_s") or 0) > 0:
                self.app_topics.append(t)
        self.idle_topics = [t for t in tper if internal(t)]
        self.req_rated = (pk(self.lat, "req_per_s") or 0) > 0
        # no application topic moved, and no request type had a measurable rate
        self.no_load = bool(tper) and not self.app_topics and not self.req_rated

    def ser(self, rows, col, scale=1.0):
        """time series for a chart. Validated like every other read, so a
        refused value cannot set the y-axis to 2,000,000,000,000,000."""
        out = {}
        for r in rows:
            v = one(r, col)
            if v is None:
                continue
            e = F(r.get("elapsed_s"), 0) or 0
            out[e] = out.get(e, 0.0) + v * scale
        return sorted(out.items())

    def pts(self, rows, col):
        """per-row points, validated, no summing — for one-entity series"""
        out = []
        for r in rows:
            v = one(r, col)
            if v is None:
                continue
            out.append((F(r.get("elapsed_s"), 0) or 0, v))
        return sorted(out)

    def xfmt(self):
        dur = self.dur or 1
        def f(v):
            v = max(0, int(round(v)))
            return f"{v//3600}h{(v%3600)//60:02d}m" if dur > 5400 else f"{v//60}:{v%60:02d}"
        return f


# ----------------------------------------------------------------- sections --
def sec_health(w):
    h = ['<h2>1 · Cluster health — the go / no-go checks</h2>']
    h.append('<p class="muted">These decide whether the run is valid at all. Throughput figures from a '
             'window with offline partitions or unclean elections describe a broken cluster, not a '
             'benchmark — and partitions sitting at the minimum ISR are one replica loss away from '
             'rejecting writes.</p>')
    rows = []
    if w.have_ctl:
        bad = len(w.ctl_bad)
        rows.append(("Active controllers", "1" if not bad else f"{int(min(w.ctl_bad))}–{int(max(w.ctl_bad))}",
                     not bad, "exactly one controller held the quorum all window",
                     f"{bad} sample(s) saw a count other than 1 — no leader, or split brain"))
    else:
        rows.append(("Active controllers", None, None, "", ""))
    rows.append(("Offline partitions", w.max_off, (w.max_off == 0) if w.max_off is not None else None,
                 "none at any point", "partitions had no leader — those reads and writes failed outright"))
    rows.append(("Under-replicated (peak)", w.max_urp, (w.max_urp == 0) if w.max_urp is not None else None,
                 "every replica stayed in sync",
                 "replicas fell behind — followers could not keep up with the leaders"))
    rows.append(("Unclean elections", w.max_uncl, (w.max_uncl == 0) if w.max_uncl is not None else None,
                 "none — no data was lost to a leader change",
                 "DATA LOSS: a replica outside the ISR became leader"))
    # At-min-ISR is a different question from under-replicated and a sharper one:
    # these partitions are IN sync, but with exactly min.insync.replicas members.
    # URP stays 0 while one more replica loss stops writes to them outright.
    rows.append(("At min ISR (peak)", w.max_atminisr,
                 (w.max_atminisr == 0) if w.max_atminisr is not None else None,
                 "every partition had spare in-sync replicas above the minimum",
                 "these partitions sit at exactly min.insync.replicas — one more "
                 "replica loss and producers get NOT_ENOUGH_REPLICAS on them"))
    h.append('<div class="tbl-wrap"><table><tr><th>Check</th><th class="num">Worst value</th>'
             '<th>Verdict</th><th>Reading</th></tr>')
    for name, val, ok, oktxt, badtxt in rows:
        if ok is None:
            h.append(f'<tr><td><b>{name}</b></td><td class="num"><span class="pill mut">not published</span></td>'
                     f'<td class="muted">unknown</td>'
                     f'<td class="muted">the JMX exporter does not expose this series</td></tr>')
            continue
        pill = f'<span class="pill {"good" if ok else "bad"}">{"PASS" if ok else "FAIL"}</span>'
        shown = val if isinstance(val, str) else n0(val)
        h.append(f'<tr><td><b>{name}</b></td><td class="num">{shown}</td><td>{pill}</td>'
                 f'<td class="muted">{oktxt if ok else badtxt}</td></tr>')
    h.append("</table></div>")
    return "".join(h)


def sec_window(w):
    h = ['<h2>2 · Load window</h2>']
    h.append('<div class="grid">')
    h.append(kpi("Window (IST)", f"{short_ts(w.start)} → {short_ts(w.end)}", "sm",
                 (w.start or " ").split(" ")[0]))
    h.append(kpi("Duration", f"{f1((w.dur or 0)/60)} min", "", f"{w.samples} samples"))
    h.append(kpi("Brokers", str(len(w.brokers)) or "–", "",
                 f"{len(w.ctrls)} controller(s)" if w.ctrls else "combined roles"))
    h.append(kpi("Peak ingress", f1(w.pk_in, " MB/s", 1), "", f"avg {f1(w.av_in,' MB/s')}"))
    h.append(kpi("Peak egress", f1(w.pk_out, " MB/s", 1), "", f"avg {f1(w.av_out,' MB/s')}"))
    h.append(kpi("Peak messages", f1(w.pk_msg, "/s", 0), "", f"avg {f1(w.av_msg,'/s',0)}"))
    if w.min_idle is None:
        h.append(kpi("Worst handler idle", "not measured", "",
                     "no usable idle series — see data quality"))
    elif getattr(w, "idle_frozen", False):
        # Showing the number next to "under 30% = starved" invites the reader to
        # judge it against that threshold, which is the one thing it cannot be
        # judged against.
        h.append(kpi("Worst handler idle", f1(w.min_idle, "% *"), "",
                     "* since-start average, not this window"))
    else:
        idle_cls = "bad" if w.min_idle < 30 else ("warn" if w.min_idle < 50 else "good")
        h.append(kpi("Worst handler idle", f1(w.min_idle, "%"), idle_cls,
                     "under 30% = handlers starved"))
    if w.max_lag is not None:
        h.append(kpi("Peak consumer lag", n0(w.max_lag),
                     "warn" if w.max_lag > 0 else "good", "summed across all groups"))
    h.append("</div>")
    pin = w.ser(w.clu, "bytes_in_mb_s")
    pout = w.ser(w.clu, "bytes_out_mb_s")
    if pin or pout:
        h.append('<h3>Cluster throughput across the window</h3>')
        h.append('<div class="chart">' + svg(
            [{"name": "in MB/s", "color": S1, "pts": pin, "area": True},
             {"name": "out MB/s", "color": S2, "pts": pout}], ylab="MB/s", xfmt=w.xfmt()) + "</div>")
        h.append(legend([("Bytes in (produce)", S1), ("Bytes out (consume + replication)", S2)]))
        h.append('<p class="muted">Egress above ingress is normal and expected: every message is read by '
                 'consumers and also copied to each follower replica. With replication factor 3 and one '
                 'consumer group, roughly 3× ingress is the floor.</p>')
    return "".join(h)


def sec_brokers(w):
    h = ['<h2>3 · Broker saturation</h2>']
    if not w.brk:
        return "".join(h) + '<p class="muted">No broker rows in this capture.</p>'
    h.append('<p class="muted"><b>Request handler idle %</b> is the single most useful Kafka saturation '
             'signal: it is the share of time the request-handler threads had nothing to do. Healthy is '
             'above 50%; below 30% the brokers cannot keep up and latency climbs non-linearly. '
             '<b>Request queue</b> and <b>purgatory</b> rising together confirm it.</p>')
    per = {}
    for r in w.brk:
        per.setdefault(r["pod"], []).append(r)
    h.append('<div class="tbl-wrap"><table><tr><th>Broker</th><th class="num">MB/s in pk</th>'
             '<th class="num">MB/s out pk</th><th class="num">msg/s pk</th>'
             '<th class="num">hnd idle min</th><th class="num">net idle min</th>'
             '<th class="num">reqQ pk</th><th class="num">purg pk</th>'
             '<th class="num">URP pk</th><th class="num">leaders</th><th>Reading</th></tr>')
    T = dict(i=0, o=0, m=0, q=0, p=0, u=0, l=0)
    for b in sorted(per):
        rs = per[b]
        i_, o_, m_ = pk(rs, "bytes_in_mb_s"), pk(rs, "bytes_out_mb_s"), pk(rs, "messages_in_s")
        hi, ni = mn(rs, "handler_idle_pct"), mn(rs, "network_idle_pct")
        q, pu = pk(rs, "request_queue"), pk(rs, "purgatory")
        u, le = pk(rs, "under_replicated"), pk(rs, "leaders")
        # "healthy headroom" must never be the fallback for a missing figure.
        # An absent or refused idle percentage is unknown, not good — and nor is
        # a frozen one. The banner and the summary both say saturation is
        # unknown when the series never varied, so this column has to agree:
        # three rows reading "healthy headroom" is what a client would quote.
        if (u or 0) > 0:
            cls, txt = "bad", "replicas fell behind on this broker"
        elif hi is None:
            cls, txt = "", "handler idle not measured — saturation unknown"
        elif getattr(w, "idle_frozen", False):
            cls, txt = "", "idle % is a since-start average — saturation unknown"
        elif hi < 30:
            cls, txt = "bad", "saturated — handlers starved"
        elif hi < 50:
            cls, txt = "warn", "working hard, little headroom"
        else:
            cls, txt = "good", "healthy headroom"
        for k, v in (("i", i_), ("o", o_), ("m", m_)):
            T[k] += v or 0
        T["q"] = max(T["q"], q or 0); T["p"] = max(T["p"], pu or 0)
        T["u"] = max(T["u"], u or 0); T["l"] += le or 0
        pill = f'<span class="pill {cls}">{f1(hi,"%")}</span>' if hi is not None else '<span class="pill mut">–</span>'
        h.append(f'<tr><td class="mono">{esc(b)}</td><td class="num">{f1(i_)}</td>'
                 f'<td class="num">{f1(o_)}</td><td class="num">{n0(m_)}</td>'
                 f'<td class="num">{pill}</td><td class="num">{f1(ni,"%")}</td>'
                 f'<td class="num">{n0(q)}</td><td class="num">{n0(pu)}</td>'
                 f'<td class="num">{n0(u)}</td><td class="num">{n0(le)}</td>'
                 f'<td class="muted">{txt}</td></tr>')
    h.append(f'<tr class="tot"><td>CLUSTER <span class="muted">({len(per)} brokers)</span></td>'
             f'<td class="num">{f1(T["i"])}</td><td class="num">{f1(T["o"])}</td>'
             f'<td class="num">{n0(T["m"])}</td><td class="num">{f1(w.min_idle,"%")}</td>'
             f'<td class="num">{f1(w.min_net,"%")}</td><td class="num">{n0(T["q"])}</td>'
             f'<td class="num">{n0(T["p"])}</td><td class="num">{n0(T["u"])}</td>'
             f'<td class="num">{n0(T["l"])}</td><td class="muted"></td></tr>')
    h.append("</table></div>")
    h.append('<p class="muted">The in/out/msg columns sum per-broker peaks, which may slightly overstate the '
             'cluster peak because brokers peak at different moments — the window chart above is the '
             'authority for a simultaneous cluster total. Idle and queue columns show the worst single '
             'broker, which is what actually limits the cluster.</p>')
    # idle chart
    series, cols = [], [S1, S2, S3, S4, S5]
    for n, b in enumerate(sorted(per)[:5]):
        pts = w.pts(per[b], "handler_idle_pct")
        if pts:
            series.append({"name": "-".join(b.split("-")[-2:]), "color": cols[n % 5], "pts": pts})
    if series:
        h.append('<h3>Request handler idle % per broker</h3>')
        h.append('<div class="chart">' + svg(series, ylab="idle %", xfmt=w.xfmt()) + "</div>")
        h.append(legend([(s["name"], s["color"]) for s in series]))
    # leader balance
    lv = [pk(per[b], "leaders") for b in sorted(per)]
    lv = [x for x in lv if x]
    if len(lv) > 1 and min(lv) > 0:
        r = max(lv) / min(lv)
        cls = "good" if r < 1.3 else ("warn" if r < 2 else "bad")
        h.append(f'<div class="verdict {cls}"><b>Leader balance {r:.2f}×.</b> '
                 + ("Partition leadership is spread evenly, so no single broker is carrying the cluster."
                    if r < 1.3 else
                    "Leadership is uneven — the busiest broker holds materially more partition leaders than "
                    "the quietest, so it saturates first. A partition reassignment (or Cruise Control) "
                    "would even this out.") + '</div>')
    return "".join(h)


def sec_latency(w):
    h = ['<h2>4 · Request latency — where the time goes</h2>']
    if not w.lat:
        return "".join(h) + '<p class="muted">No latency rows (the request-metrics series were not published).</p>'
    per = {}
    for r in w.lat:
        per.setdefault(r["request"], []).append(r)
    h.append('<p class="muted">Kafka splits each request into <b>queue time</b> (waiting for a handler '
             'thread) and <b>local time</b> (actually doing the work). The split is the diagnosis: queue '
             'time dominating means the brokers are <i>backed up</i> and need more capacity or partitions; '
             'local time dominating means each operation is genuinely slow — disk, or too-large batches.</p>')
    K = w.lat_label
    if w.lat_kind == "max":
        h.append('<div class="verdict warn"><b>These are Max-since-broker-start, not percentiles.</b> '
                 'The JMX exporter on this cluster publishes no <span class="mono">quantile</span> labels, '
                 'so the only latency figure available is the single worst request each broker has served '
                 'since it booted. A Max of tens of seconds is normal for <span class="mono">JoinGroup</span> '
                 '(it waits out the rebalance timeout) and says nothing about typical latency. Treat this '
                 'section as an outlier check, not a latency profile — add quantile rules to the exporter '
                 'config to get a real p99.</div>')
    h.append(f'<div class="tbl-wrap"><table><tr><th>Request</th><th class="num">{K} total ms</th>'
             '<th class="num">queue ms</th><th class="num">local ms</th><th class="num">queue share</th>'
             '<th class="num">req/s pk</th><th>Reading</th></tr>')
    order = sorted(per, key=lambda k: -(pk(per[k], "total_ms") or 0))
    shown, usable = 0, []
    for rq in order:
        rs = per[rq]
        t, q, l = pk(rs, "total_ms"), pk(rs, "queue_ms"), pk(rs, "local_ms")
        rate = pk(rs, "req_per_s")
        bad_split = lat_incoherent(t, q, l)
        if t is not None and not bad_split:
            shown += 1
            usable.append(rq)
        # No total time means every sample for this request was refused by the
        # validator. A queue share computed from refused numbers would be a
        # fabricated diagnosis, so the row states that instead.
        share = None if bad_split else ((q / t * 100) if (t and q is not None) else None)
        if t is None:
            cls, txt = "", "not usable — values outside any possible range"
        elif bad_split:
            cls, txt = "", "breakdown does not add up — the split is not measured"
        elif share is None:
            cls, txt = "", ""
        elif not rate:
            # Admin and quorum calls (DescribeClientQuotas, Vote, DescribeLogDirs)
            # are made a handful of times by Cruise Control and the operator. Their
            # queue share is arithmetically real and operationally meaningless, and
            # a tuning recommendation hung off one would send the client chasing a
            # setting that has nothing to do with their load.
            cls, txt = "", "no measurable request rate — background/admin call, not load"
        elif share > 70 and q >= MATERIAL_MS:
            # A queue-dominated request on brokers that still had idle handlers
            # is not a capacity problem — recommending more brokers there would
            # cost money and fix nothing.
            if w.min_idle is not None and w.min_idle >= 50:
                cls, txt = "warn", ("queue-dominated, but handlers kept "
                                    f"{f1(w.min_idle)}% idle — look at batch size, linger.ms and "
                                    "partition count before adding brokers")
            else:
                cls, txt = "bad", "backed up — add capacity, not tuning"
        elif share > 70:
            cls, txt = "good", f"queue-dominated, but only {f1(q)} ms — too small to act on"
        elif share > 40 and q >= MATERIAL_MS:
            cls, txt = "warn", "queueing noticeably"
        elif share > 40:
            cls, txt = "good", "mostly queueing, but the absolute time is small"
        else:
            cls, txt = "good", "time spent doing work, not waiting"
        sp = f'<span class="pill {cls}">{share:.0f}%</span>' if share is not None else "–"
        # A value whose own breakdown is self-contradictory is not a measurement,
        # so the cells are left empty rather than printed with a caveat.
        ct, cq, cl = ("–", "–", "–") if bad_split else (f1(t), f1(q), f1(l))
        h.append(f'<tr><td class="mono">{esc(rq)}</td><td class="num">{ct}</td>'
                 f'<td class="num">{cq}</td><td class="num">{cl}</td>'
                 f'<td class="num">{sp}</td><td class="num">{n0(rate)}</td>'
                 f'<td class="muted">{txt}</td></tr>')
    h.append("</table></div>")
    if shown == 0:
        h.append('<div class="verdict bad"><b>No latency figure in this section can be used.</b> '
                 'Either the values were outside the range a request time can physically take, or total, '
                 'queue and local time were identical — both mean the collector resolved a cumulative '
                 '<span class="mono">_total</span> / <span class="mono">_sum</span> counter instead of the '
                 'latency gauge. Run <span class="mono">./perf-kafka.sh --metrics totaltime</span> to see '
                 'which series names the exporter actually publishes. <b>No conclusion about queueing is '
                 'drawn from this data.</b></div>')
        return "".join(h)
    # Charted by traffic, and ONLY requests that carried traffic. Protocol waits
    # like JoinGroup (which simply sits out the rebalance timeout) and admin calls
    # from Cruise Control are the largest numbers in the table while carrying no
    # requests — on a shared y-axis they flatten Produce and Fetch into the
    # baseline, so a chart of them shows the cluster's idle chatter, not its load.
    carrying = [k for k in usable if (pk(per[k], "req_per_s") or 0) > 0]
    chart_order = sorted(carrying, key=lambda k: -(pk(per[k], "req_per_s") or 0))
    series, cols = [], [S1, S2, S3, S4]
    for n, rq in enumerate(chart_order[:4]):
        pts = w.pts(per[rq], "total_ms")
        if pts:
            series.append({"name": rq, "color": cols[n % 4], "pts": pts})
    if series:
        h.append(f'<h3>{K} total time by request type</h3>')
        h.append('<div class="chart">' + svg(series, ylab="ms", xfmt=w.xfmt()) + "</div>")
        h.append(legend([(s["name"], s["color"]) for s in series]))
    elif usable:
        h.append('<p class="muted"><b>No chart:</b> not one request type in this window had a '
                 'measurable request rate. Every row above is background chatter — Cruise Control '
                 'polling, the entity operator, KRaft quorum traffic — so there is no request '
                 'latency to plot. If a load test was running, its traffic did not reach these '
                 'brokers.</p>')
    return "".join(h)


def sec_lag(w):
    h = ['<h2>5 · Consumer lag</h2>']
    if not w.lag:
        return "".join(h) + ('<p class="muted">No consumer-group rows. Either the in-pod Kafka CLI was not '
                             'usable (TLS/SCRAM listeners need a --command-config file) or kafka-exporter is '
                             'not deployed. Run <span class="mono">perf-kafka.sh --check</span> — step 14 '
                             'says which.</p>')
    h.append('<p class="muted">Lag is the gap between what producers have written and what each consumer '
             'group has read. Lag that rises and stays high means consumers are the bottleneck, not the '
             'brokers — the cluster accepted the load fine, the application could not drain it.</p>')
    if w.lag_pts:
        h.append('<div class="chart">' + svg([{"name": "total lag", "color": S2,
                                               "pts": w.lag_pts, "area": True}],
                                             ylab="messages", xfmt=w.xfmt()) + "</div>")
        trend = ""
        if len(w.lag_pts) >= 3:
            first = w.lag_pts[0][1]; last = w.lag_pts[-1][1]
            if last > first * 1.5 and last > 1000:
                trend = ('<div class="verdict bad"><b>Lag grew through the window and did not recover.</b> '
                         'Consumers never caught up, so the sustainable throughput is lower than the rate '
                         'produced here. Add consumer instances or partitions before quoting this rate.</div>')
            elif last < max(x[1] for x in w.lag_pts) * 0.5:
                trend = ('<div class="verdict good"><b>Lag peaked and then drained.</b> Consumers kept up '
                         'overall — the peak is burst absorption, which is what the log is for.</div>')
        h.append(trend)
    per = {}
    for r in w.lag:
        per.setdefault((r.get("group", ""), r.get("topic", "")), []).append(r)
    h.append('<div class="tbl-wrap"><table><tr><th>Group</th><th>Topic</th>'
             '<th class="num">Peak lag</th><th class="num">Final lag</th>'
             '<th class="num">Members</th><th>Reading</th></tr>')
    tot_pk = tot_fin = 0
    for (g, t) in sorted(per, key=lambda k: -(pk(per[k], "lag") or 0)):
        rs = per[(g, t)]
        p, fin = pk(rs, "lag"), F(rs[-1].get("lag"))
        mem = pk(rs, "members")
        tot_pk += p or 0; tot_fin += fin or 0
        if (fin or 0) > (p or 0) * 0.8 and (fin or 0) > 1000:
            cls, txt = "bad", "still behind at the end"
        elif (p or 0) > 1000:
            cls, txt = "warn", "fell behind, then recovered"
        else:
            cls, txt = "good", "kept up"
        h.append(f'<tr><td class="mono">{esc(g)}</td><td class="mono">{esc(t)}</td>'
                 f'<td class="num">{n0(p)}</td>'
                 f'<td class="num"><span class="pill {cls}">{n0(fin)}</span></td>'
                 f'<td class="num">{n0(mem)}</td><td class="muted">{txt}</td></tr>')
    h.append(f'<tr class="tot"><td>TOTAL</td><td class="muted">{len(per)} group/topic pairs</td>'
             f'<td class="num">{n0(tot_pk)}</td><td class="num">{n0(tot_fin)}</td>'
             f'<td class="num"></td><td></td></tr>')
    h.append("</table></div>")
    return "".join(h)


def sec_topics(w):
    h = ['<h2>6 · Topics</h2>']
    if not w.top:
        return "".join(h) + '<p class="muted">No per-topic rows (the topic label was not present on the metrics).</p>'
    per = {}
    for r in w.top:
        per.setdefault(r["topic"], []).append(r)
    h.append('<div class="tbl-wrap"><table><tr><th>Topic</th><th class="num">MB/s in pk</th>'
             '<th class="num">MB/s in avg</th><th class="num">MB/s out pk</th>'
             '<th class="num">msg/s pk</th><th class="num">share of ingress</th></tr>')
    tot_av = sum((av(per[t], "bytes_in_mb_s") or 0) for t in per)
    tot = tot_av or 1          # share denominator only; never printed
    TI = TO = TM = 0.0
    for t in sorted(per, key=lambda k: -(pk(per[k], "bytes_in_mb_s") or 0)):
        rs = per[t]
        i_, ia, o_, m_ = (pk(rs, "bytes_in_mb_s"), av(rs, "bytes_in_mb_s"),
                          pk(rs, "bytes_out_mb_s"), pk(rs, "messages_in_s"))
        sh = (ia or 0) / tot * 100
        bw = max(1, int(round(sh * 1.5)))
        TI += i_ or 0; TO += o_ or 0; TM += m_ or 0
        h.append(f'<tr><td class="mono">{esc(t)}</td><td class="num">{f1(i_,"",2)}</td>'
                 f'<td class="num">{f1(ia,"",2)}</td><td class="num">{f1(o_,"",2)}</td>'
                 f'<td class="num">{n0(m_)}</td>'
                 f'<td class="num"><span class="bar" style="width:{bw}px"></span> {sh:.1f}%</td></tr>')
    h.append(f'<tr class="tot"><td>TOTAL <span class="muted">({len(per)} topics)</span></td>'
             f'<td class="num">{f1(TI,"",2)}</td><td class="num">{f1(tot_av,"",2)}</td>'
             f'<td class="num">{f1(TO,"",2)}</td><td class="num">{n0(TM)}</td>'
             f'<td class="num">100%</td></tr>')
    h.append("</table></div>")
    return "".join(h)


def sec_infra(w):
    h = ['<h2>7 · Pods, placement and storage</h2>']
    if w.pod:
        per = {}
        for r in w.pod:
            per.setdefault(r["pod"], []).append(r)
        h.append('<div class="tbl-wrap"><table><tr><th>Pod</th><th>Role</th><th>Node</th>'
                 '<th class="num">CPU pk</th><th class="num">Mem pk Mi</th>'
                 '<th class="num">Heap pk Mi</th><th class="num">GC/s pk</th>'
                 '<th class="num">Restarts</th><th>Ready</th></tr>')
        jper = {}
        for r in w.jvm:
            jper.setdefault(r["pod"], []).append(r)
        for p in sorted(per, key=lambda k: (per[k][-1].get("role", "z"), k)):
            rs = per[p]
            node = rs[-1].get("node", "–")
            rc = pk(rs, "restarts")
            js = jper.get(p, [])
            rcls = "bad" if (rc or 0) > 0 else ""
            # Where a cross-check proved the series wrong, the cell says so
            # instead of printing a number that would be read as a measurement.
            heap_c = ('<span class="pill mut">bad series</span>' if p in w.heap_bad
                      else n0(pk(js, "heap_used_mb")))
            gc_c = ('<span class="pill mut">not measured</span>' if w.gc_unmeasured
                    else f1(pk(js, "gc_count_s"), "", 2))
            h.append(f'<tr><td class="mono">{esc(p)}</td><td class="muted">{esc(rs[-1].get("role","–"))}</td>'
                     f'<td class="mono">{esc(node)}</td><td class="num">{n0(pk(rs,"cpu_m"))}m</td>'
                     f'<td class="num">{n0(pk(rs,"mem_mi"))}</td>'
                     f'<td class="num">{heap_c}</td>'
                     f'<td class="num">{gc_c}</td>'
                     f'<td class="num"><span class="pill {rcls}">{n0(rc)}</span></td>'
                     f'<td class="muted">{esc(rs[-1].get("ready","–"))}</td></tr>')
        h.append("</table></div>")
        # co-location
        last = {p: per[p][-1] for p in per}
        bynode = {}
        for p, r in last.items():
            if r.get("role") == "broker":
                bynode.setdefault(r.get("node", "?"), []).append(p)
        dupes = {k: v for k, v in bynode.items() if len(v) > 1}
        if dupes:
            lst = "; ".join(f"{k}: {', '.join(v)}" for k, v in dupes.items())
            h.append(f'<div class="verdict bad"><b>Brokers sharing a node.</b> {esc(lst)}. '
                     'Losing one node takes more than one broker with it, so a replication factor of 3 no '
                     'longer survives a single node failure. Pod anti-affinity on the broker node pool fixes '
                     'this.</div>')
        elif bynode:
            h.append(f'<div class="verdict good"><b>Each broker sits on its own node</b> '
                     f'({len(bynode)} nodes), so a single node failure costs exactly one replica.</div>')
    if w.dsk:
        per = {}
        for r in w.dsk:
            per.setdefault(r["pvc"], []).append(r)
        h.append('<h3>Storage</h3>')
        h.append('<div class="tbl-wrap"><table><tr><th>PVC</th><th>Broker</th>'
                 '<th class="num">Used GB</th><th class="num">Size GB</th>'
                 '<th class="num">Used %</th><th class="num">Grew GB</th><th>Reading</th></tr>')
        for v in sorted(per, key=lambda k: -(pk(per[k], "used_pct") or 0)):
            rs = per[v]
            u, c, p = pk(rs, "used_gb"), pk(rs, "capacity_gb"), pk(rs, "used_pct")
            grew = (F(rs[-1].get("used_gb"), 0) or 0) - (F(rs[0].get("used_gb"), 0) or 0)
            cls = "bad" if (p or 0) > 85 else ("warn" if (p or 0) > 75 else "good")
            txt = ("critical — brokers shut down when the log dir fills" if cls == "bad"
                   else "watch — retention or a bigger PVC needed" if cls == "warn" else "comfortable")
            h.append(f'<tr><td class="mono">{esc(v)}</td><td class="mono">{esc(rs[-1].get("pod","–"))}</td>'
                     f'<td class="num">{f1(u)}</td><td class="num">{f1(c)}</td>'
                     f'<td class="num"><span class="pill {cls}">{f1(p,"%")}</span></td>'
                     f'<td class="num">{f1(grew,"",2)}</td><td class="muted">{txt}</td></tr>')
        h.append("</table></div>")
        h.append('<p class="muted">"Grew GB" is the increase over this window only. Extrapolate it against '
                 'free space to find how long a sustained run of this shape can last before retention has '
                 'to be tightened.</p>')
    return "".join(h)


def sec_raw(w):
    h = ['<h2>8 · Raw samples</h2>']
    h.append('<p class="muted">This appendix prints the CSV rows <b>exactly as the collector wrote them</b>, '
             'with no validation applied — that is its purpose, so the capture can be audited against the '
             'files. Sections 1–7 above apply the checks; where a figure was refused there, the unfiltered '
             'value still appears here. <b>Quote figures from the sections above, not from this appendix.</b></p>')
    if w.clu:
        h.append(f'<details><summary>cluster — {len(w.clu)} samples</summary><div class="tbl-wrap"><table>'
                 '<tr><th>Time</th><th class="num">Elapsed</th><th class="num">Ctl</th>'
                 '<th class="num">Offline</th><th class="num">URP</th><th class="num">MB/s in</th>'
                 '<th class="num">MB/s out</th><th class="num">msg/s</th></tr>')
        for r in w.clu:
            h.append(f'<tr><td class="mono">{short_ts(r["ts_ist"])}</td>'
                     f'<td class="num">{n0(r.get("elapsed_s"))}</td>'
                     f'<td class="num">{n0(r.get("active_controllers"))}</td>'
                     f'<td class="num">{n0(r.get("offline_partitions"))}</td>'
                     f'<td class="num">{n0(r.get("under_replicated"))}</td>'
                     f'<td class="num">{f1(r.get("bytes_in_mb_s"),"",2)}</td>'
                     f'<td class="num">{f1(r.get("bytes_out_mb_s"),"",2)}</td>'
                     f'<td class="num">{n0(r.get("messages_in_s"))}</td></tr>')
        h.append(f'<tr class="tot"><td>PEAK <span class="muted">({len(w.clu)} samples)</span></td>'
                 f'<td class="num">{n0(w.dur)}</td><td class="num"></td>'
                 f'<td class="num">{n0(w.max_off)}</td><td class="num">{n0(w.max_urp)}</td>'
                 f'<td class="num">{f1(w.pk_in,"",2)}</td><td class="num">{f1(w.pk_out,"",2)}</td>'
                 f'<td class="num">{n0(w.pk_msg)}</td></tr>')
        h.append('</table></div><p class="muted">The TOTAL row shows the peak of each column, not a sum — '
                 'these are rates and counts, not accumulations.</p></details>')
    for name, rows, cols in (("broker", w.brk, ["pod", "bytes_in_mb_s", "bytes_out_mb_s",
                                                "handler_idle_pct", "request_queue", "purgatory",
                                                "under_replicated", "leaders"]),
                             ("latency", w.lat, ["request", "total_ms", "queue_ms", "local_ms", "req_per_s"]),
                             ("consumer lag", w.lag, ["group", "topic", "lag", "members"]),
                             ("health (CLI)", w.hlt, ["check", "value", "detail"])):
        if not rows:
            continue
        h.append(f'<details><summary>{name} — {len(rows)} rows</summary><div class="tbl-wrap"><table>'
                 '<tr><th>Time</th><th class="num">Elapsed</th>'
                 + "".join(f'<th class="num">{c}</th>' for c in cols) + "</tr>")
        for r in rows[:400]:
            cells = ""
            for c in cols:
                v = r.get(c, "")
                if F(v) is not None:
                    cells += f'<td class="num">{f1(v,"",2) if "." in str(v) else n0(v)}</td>'
                elif str(v).strip() == "":
                    # a metric the exporter never published — say so, don't
                    # leave a blank cell that reads as a broken table
                    cells += '<td class="num muted">–</td>'
                else:
                    cells += f'<td class="num">{esc(v)}</td>'
            h.append(f'<tr><td class="mono">{short_ts(r.get("ts_ist",""))}</td>'
                     f'<td class="num">{n0(r.get("elapsed_s"))}</td>{cells}</tr>')
        if len(rows) > 400:
            h.append(f'<tr><td colspan="{len(cols)+2}" class="muted">… {len(rows)-400} further rows in the CSV</td></tr>')
        h.append("</table></div></details>")
    return "".join(h)


def verdict(w):
    bits, cls = [], "good"
    # This leads, because every other figure below is a description of an idle
    # cluster and would otherwise read as a passed load test.
    if getattr(w, "no_load", False):
        names = ", ".join(f'<span class="mono">{esc(t)}</span>' for t in sorted(w.idle_topics)[:4])
        bits.append(
            "<b>No application load reached this cluster during the window.</b> Every topic that "
            f"moved is internal ({names}) — consumer-offset commits and Cruise Control's own "
            "metrics — and not one request type recorded a measurable rate. The throughput and "
            "latency figures below describe an idle Kafka cluster, not a load test, so they cannot "
            "be used to size anything. Check that the producers were pointed at this bootstrap "
            "address and namespace before re-running.")
        cls = "bad"
    if w.max_off and w.max_off > 0:
        bits.append(f"<b>{n0(w.max_off)} offline partitions</b> — reads and writes to them failed, so this "
                    "window is not a valid benchmark."); cls = "bad"
    if w.max_uncl and w.max_uncl > 0:
        bits.append("<b>Unclean leader elections occurred — data was lost.</b>"); cls = "bad"
    if w.ctl_bad:
        bits.append(f"The controller count left 1 on {len(w.ctl_bad)} sample(s)."); cls = "bad"
    if w.pk_in is not None:
        bits.append(f"Peak ingress <b>{f1(w.pk_in)} MB/s</b> ({n0(w.pk_msg)} msg/s), egress "
                    f"{f1(w.pk_out)} MB/s, across {len(w.brokers)} broker(s).")
    if getattr(w, "idle_frozen", False) and w.min_idle is not None:
        bits.append(f"The handler-idle series read a flat {f1(w.min_idle)}% for the whole window, "
                    "which is a since-start average rather than a measurement of this window — "
                    "<b>broker saturation is therefore unknown</b>, not good.")
        if cls == "good":
            cls = "warn"
    elif w.min_idle is not None:
        if w.min_idle < 30:
            bits.append(f"Request handlers bottomed out at <b>{f1(w.min_idle)}% idle</b> — the brokers were "
                        "saturated and this is the ceiling for this topology.")
            cls = "bad" if cls == "bad" else "warn"
        elif w.min_idle < 50:
            bits.append(f"Handlers bottomed out at {f1(w.min_idle)}% idle — working hard with limited headroom.")
            if cls == "good":
                cls = "warn"
        elif getattr(w, "no_load", False):
            # Headroom measured against no load is not a finding. Saying a cluster
            # has capacity to spare while nothing was asked of it is the kind of
            # sentence a client would reasonably read as a passed test.
            bits.append(f"Handlers sat at {f1(w.min_idle)}% idle, which only confirms the cluster "
                        "was idle — it is not evidence of headroom under load.")
        else:
            bits.append(f"Handlers never dropped below {f1(w.min_idle)}% idle — comfortable headroom.")
    elif w.brk:
        # The single most important saturation signal is missing. Saying nothing
        # would let the reader assume it was fine, so the gap is stated outright.
        bits.append("<b>Request handler idle was not measured</b>, so broker saturation is unknown "
                    "for this window — the headroom question is open, not answered.")
        if cls == "good":
            cls = "warn"
    if w.max_urp:
        bits.append(f"Under-replicated peaked at {n0(w.max_urp)}.")
        if cls == "good":
            cls = "warn"
    if w.max_lag is not None and w.max_lag > 0:
        bits.append(f"Peak consumer lag {n0(w.max_lag)} messages.")
    if w.restarts:
        bits.append(f"{n0(w.restarts)} pod restart(s) during the window.")
        if cls == "good":
            cls = "warn"
    if w.max_pvc:
        bits.append(f"Busiest log dir reached {f1(w.max_pvc)}% full.")
    if not bits:
        bits.append("The capture produced no metric rows — run <span class=\"mono\">--check</span>.")
        cls = "bad"
    return cls, " ".join(bits)


def quality_banner(wins):
    """Rendered only when something was refused. It names what was thrown away
    and how to fix the collector, so a thin report is never mistaken for a
    healthy cluster."""
    notes = []
    if REJECTED:
        items = ", ".join(f"<b>{esc(k)}</b> ({v} value{'s' if v != 1 else ''})"
                          for k, v in sorted(REJECTED.items()))
        notes.append(f"Refused as physically impossible, and therefore not displayed: {items}.")
    for w in wins:
        if getattr(w, "part_bad", False):
            notes.append(
                f"Window <span class=\"mono\">{esc(w.tag)}</span> reported fewer partition replicas than "
                "partition leaders, which cannot happen — every leader is itself a replica. Both figures "
                "have been withheld.")
        for x in getattr(w, "xchecks", []):
            notes.append(x)
    if not notes:
        return ""
    return ('<div class="verdict bad"><b>Data quality — read this before quoting any figure.</b> '
            'The checks below compare each series against what is arithmetically or physically '
            'possible, and against the other series measuring the same thing. Where a figure failed, '
            'it is absent rather than guessed; nothing else in the report is affected.'
            '<ul style="margin:9px 0 2px;padding-left:20px">'
            + "".join(f"<li style=\"margin:4px 0\">{n}</li>" for n in notes)
            + '</ul>To chase these down, list what the exporter actually publishes with '
            '<span class="mono">./perf-kafka.sh --metrics &lt;pattern&gt;</span>.</div>')


def build(wins):
    utc = datetime.datetime.now(datetime.timezone.utc)
    ist = utc + datetime.timedelta(hours=5, minutes=30)
    now = ist.strftime("%Y-%m-%d %H:%M IST") + " / " + utc.strftime("%H:%M UTC")
    ver = ""
    for w in wins:
        if w.info.get("script_version"):
            ver = " · collector build " + esc(w.info["script_version"].split()[0])
            break
    H = ['<!doctype html><html><head><meta charset="utf-8">'
         '<meta name="viewport" content="width=device-width, initial-scale=1">'
         '<title>SBIePay 2.0 - PERF Kafka Report</title>', CSS, '</head><body><div class="wrap">']
    H.append('<div class="masthead"><div class="kick">Kafka · Broker &amp; Consumer Load</div>'
             '<h1>SBIePay 2.0 - PERF Kafka Report</h1>'
             f'<div class="sub">Prepared by Saadi M Muzzammil · generated {now} · '
             f'report build {esc(KR_VERSION.split()[0])}</div>'
             f'<div class="sub">Windows: {esc(", ".join(w.tag for w in wins))} · '
             f'source: perf-kafka.sh (JMX via Thanos + in-pod Kafka CLI){ver}</div>'
             '<div class="sub" style="margin-top:11px">'
             '<a href="https://oneviewuat.sbi/" target="_blank" rel="noopener" style="margin-right:18px">Dynatrace ↗</a>'
             '<a href="https://console-openshift-console.apps.preprod.epay.sbi/" target="_blank" rel="noopener" '
             'style="margin-right:18px">OCP Console ↗</a>'
             '<a href="http://10.177.139.68:3000/" target="_blank" rel="noopener">Grafana ↗</a>'
             '</div></div>')
    multi = len(wins) > 1
    if multi:
        H.append('<div class="navbtns">' + "".join(
            f'<a href="#w-{esc(w.tag)}">Window · {esc(w.tag)}</a>' for w in wins) + '</div>')
    else:
        H.append('<div class="navbtns"><a href="#s1">1 · Health</a><a href="#s2">2 · Window</a>'
                 '<a href="#s3">3 · Brokers</a><a href="#s4">4 · Latency</a><a href="#s5">5 · Lag</a>'
                 '<a href="#s6">6 · Topics</a><a href="#s7">7 · Pods</a><a href="#s8">8 · Raw</a></div>')
    # Sections are rendered first so every rejection is already counted, then
    # the data-quality banner is placed above them — the reader must see what was
    # refused before they read any figure.
    body = []
    for w in wins:
        if multi:
            body.append(f'<span id="w-{esc(w.tag)}"></span><div class="groupband">'
                        f'<h2 class="group">Window · {esc(w.tag)}</h2>'
                        f'<span class="gsub">{short_ts(w.start)} → {short_ts(w.end)} IST</span></div>')
        pre = "" if multi else '<span id="s%d"></span>'
        secs = []
        for i, sb in enumerate((sec_health(w), sec_window(w), sec_brokers(w), sec_latency(w),
                                sec_lag(w), sec_topics(w), sec_infra(w), sec_raw(w)), start=1):
            secs.append((pre % i if pre else "") + sb)
        vc, vt = verdict(w)
        body.append(f'<div class="verdict {vc}"><b>Summary.</b> {vt}</div>')
        body.extend(secs)
    H.append(quality_banner(wins))
    H.extend(body)
    H.append('<p class="muted" style="text-align:center;margin-top:40px;border-top:1px solid var(--line);'
             'padding-top:14px">© Saadi M Muzzammil · read-only · generated from perf-kafka.sh CSVs.</p>')
    H.append("</div></body></html>")
    return "".join(H)


def main():
    ap = argparse.ArgumentParser(description="HTML report from perf-kafka.sh CSVs")
    ap.add_argument("--tag", action="append", default=[])
    ap.add_argument("--dir", default=".")
    ap.add_argument("-o", default="KAFKA_Report.html")
    ap.add_argument("--version", "-V", action="store_true",
                    help="print the report build and exit")
    a = ap.parse_args()
    if a.version:
        print(f"kafka_report.py {KR_VERSION}")
        return
    tags = a.tag
    if not tags:
        for f in sorted(os.listdir(a.dir)):
            if f.startswith("kafka_cluster_") and f.endswith(".csv"):
                tags.append(f[len("kafka_cluster_"):-4])
    if not tags:
        print("No kafka_cluster_<tag>.csv found — nothing to report on.")
        print(f"  looked in: {os.path.abspath(a.dir)}")
        raise SystemExit(2)
    wins = []
    for t in tags:
        w = Win(t, a.dir)
        if not w.ok:
            print(f"  [skip] {t}: no usable rows")
            continue
        wins.append(w)
        print(f"  [window] {t}: {w.samples} samples, {len(w.brokers)} brokers, "
              f"peak in {f1(w.pk_in)} MB/s")
    if not wins:
        print("No usable window.")
        raise SystemExit(2)
    with open(a.o, "w") as f:
        f.write(build(wins))
    print(f"[kafka-report] wrote {a.o}")


if __name__ == "__main__":
    main()
