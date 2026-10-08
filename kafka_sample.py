#!/usr/bin/env python3
# ============================================================================
# kafka_sample.py — one sample of perf-kafka.sh: read the collected temp files,
# append the CSV rows, print the live screen.
# Called by perf-kafka.sh with everything passed through the environment.
# Not meant to be run on its own.
# ============================================================================
import os, sys

E = os.environ.get
TMP = E("TMPD", "")
TS = E("TS", "")
EL = E("EL", "0")


def rd(name):
    """read a tab-separated 'key<TAB>value' dump into {key: float}"""
    out = {}
    try:
        with open(os.path.join(TMP, name)) as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 2:
                    continue
                k = parts[0].strip()
                try:
                    v = float(parts[-1])
                except ValueError:
                    continue
                if k and k != "-":
                    out[k] = out.get(k, 0.0) + v
    except OSError:
        pass
    return out


def lines(name):
    try:
        with open(os.path.join(TMP, name)) as f:
            return [l.rstrip("\n") for l in f if l.strip()]
    except OSError:
        return []


def f(v, d=1):
    try:
        return f"{float(v):,.{d}f}"
    except (TypeError, ValueError):
        return "-"


def n0(v):
    try:
        return f"{float(v):,.0f}"
    except (TypeError, ValueError):
        return "-"


def _n(v):
    return "" if v is None else f"{v:.0f}"


def _c(v, w):
    """right-aligned cell: a dash when the metric does not exist"""
    return ("-".rjust(w)) if v is None else f"{v:{w}.0f}"


def csv_append(path, row):
    if not path:
        return
    try:
        with open(path, "a") as fh:
            fh.write(",".join("" if c is None else str(c) for c in row) + "\n")
    except OSError:
        pass


MB = 1024.0 * 1024.0

bin_, bout, bmsg = rd("in"), rd("out"), rd("msg")
handler, netproc = rd("handler"), rd("netproc")
reqq, respq, purg = rd("reqq"), rd("respq"), rd("purg")
urp, parts, leaders = rd("urp"), rd("parts"), rd("leaders")
flag = rd("fetchlag")
shrink, expand = rd("isrshrink"), rd("isrexpand")
heap, gcc, gct = rd("heap"), rd("gcc"), rd("gct")
t_in, t_out, t_msg = rd("t_in"), rd("t_out"), rd("t_msg")
l_tot, l_q, l_l, l_r = rd("l_tot"), rd("l_q"), rd("l_l"), rd("l_r")
pvcu, pvcc = rd("pvcu"), rd("pvcc")

brokers = [b for b in E("BROKERS", "").split() if b]
controllers = [c for c in E("CONTROLLERS", "").split() if c]

# ---- pods: POD PHASE NODE RC READY -----------------------------------------
pods = {}
for l in lines("pods"):
    p = l.split()
    if len(p) < 3:
        continue
    name, phase, node = p[0], p[1], p[2]
    rc = p[3] if len(p) > 3 else "0"
    ready = p[4] if len(p) > 4 else "-"
    try:
        rcn = sum(int(x) for x in rc.split(",") if x.strip().lstrip("-").isdigit())
    except ValueError:
        rcn = 0
    nready = sum(1 for x in ready.split(",") if x.strip() == "true")
    ntot = len([x for x in ready.split(",") if x.strip()])
    pods[name] = dict(phase=phase, node=node.split(".")[0], rc=rcn,
                      ready=f"{nready}/{ntot}" if ntot else "-")

top = {}
for l in lines("top"):
    p = l.split()
    if len(p) >= 3:
        try:
            top[p[0]] = (float(p[1].rstrip("m") or 0), float(p[2].rstrip("Mi") or 0))
        except ValueError:
            pass


def role_of(p):
    if p in brokers:
        return "broker"
    if p in controllers:
        return "controller"
    if "kafka-exporter" in p:
        return "exporter"
    if "entity-operator" in p:
        return "entity-op"
    if "cruise-control" in p:
        return "cruise-ctl"
    return "other"


# ---- cluster scalars --------------------------------------------------------
def envnum(k):
    v = E(k, "")
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


HAVE = set(E("HAVE", "").split())


def opt(concept, d, key=None, agg=False):
    """None when the metric was never published; otherwise the value (or 0)."""
    if concept not in HAVE:
        return None
    if agg:
        return sum(d.values())
    return d.get(key, 0.0)


actctl, offp, uncl = envnum("ACTCTL"), envnum("OFFP"), envnum("UNCL")
tot_in = sum(bin_.values()) / MB
tot_out = sum(bout.values()) / MB
tot_msg = sum(bmsg.values())
tot_urp = opt("urp", urp, agg=True)
tot_parts = opt("parts", parts, agg=True)
tot_lead = opt("leaders", leaders, agg=True)
have_tput = "bytesin" in HAVE

csv_append(E("C_CLU"), [TS, EL,
                        "" if actctl is None else n0(actctl).replace(",", ""),
                        "" if offp is None else n0(offp).replace(",", ""),
                        "" if tot_urp is None else f"{tot_urp:.0f}",
                        "" if uncl is None else f"{uncl:.3f}",
                        "" if tot_parts is None else f"{tot_parts:.0f}",
                        "" if tot_lead is None else f"{tot_lead:.0f}",
                        "" if not have_tput else f"{tot_in:.3f}",
                        "" if not have_tput else f"{tot_out:.3f}",
                        "" if not have_tput else f"{tot_msg:.1f}"])

O = []
A = O.append

# ---- 1. cluster health ------------------------------------------------------
A("  [1] CLUSTER HEALTH")
A("  ----------------------------------------------------------------------------------")


def verdict(label, val, ok, bad_txt, ok_txt, fmt=n0):
    if val is None:
        A(f"      {label:<26} {'not published':>12}   (metric absent)")
        return
    mark = "OK " if ok else "!! "
    A(f"      {label:<26} {fmt(val):>12}   {mark}{ok_txt if ok else bad_txt}")


verdict("active controllers", actctl, actctl == 1 if actctl is not None else False,
        "must be exactly 1 — split brain or no leader", "quorum has one leader")
verdict("offline partitions", offp, (offp or 0) == 0, "DATA UNAVAILABLE", "none")
verdict("under-replicated", tot_urp, tot_urp == 0 if tot_urp is not None else False,
        "replicas behind — brokers or disk struggling", "all replicas in sync")
verdict("unclean elections/s", uncl, (uncl or 0) == 0,
        "DATA LOSS — a non-ISR replica became leader", "none", lambda v: f(v, 3))
if tot_parts is None:
    A(f"      {'partitions / leaders':<26} {'not published':>12}   (metric absent)")
else:
    A(f"      {'partitions / leaders':<26} {n0(tot_parts):>12} / {n0(tot_lead)}")
if have_tput:
    A(f"      {'throughput':<26} {f(tot_in,2):>12} MB/s in   {f(tot_out,2)} MB/s out   {n0(tot_msg)} msg/s")
else:
    A(f"      {'throughput':<26} {'not published':>12}   (no JMX exporter — see --check)")

# ---- 2. per-broker ----------------------------------------------------------
A("")
A("  [2] BROKERS   (idle% is the saturation signal: <30% = request handlers starved)")
A("  ----------------------------------------------------------------------------------")
hdr = (f"  {'BROKER':<28}{'MB/s in':>9}{'MB/s out':>9}{'msg/s':>9}"
       f"{'hnd idle':>9}{'net idle':>9}{'reqQ':>6}{'purg':>7}{'URP':>5}{'part':>6}{'lead':>6}")
A(hdr)
A("  " + "-" * (len(hdr) - 2))
min_idle = None
for b in sorted(brokers):
    i_ = bin_.get(b, 0) / MB
    o_ = bout.get(b, 0) / MB
    m_ = bmsg.get(b, 0)
    hi = handler.get(b)
    ni = netproc.get(b)
    # exporters publish idle as a 0-1 ratio in some versions, 0-100 in others
    if hi is not None and hi <= 1.0:
        hi *= 100
    if ni is not None and ni <= 1.0:
        ni *= 100
    if hi is not None and (min_idle is None or hi < min_idle):
        min_idle = hi
    flagc = "!" if (hi is not None and hi < 30) or (opt("urp", urp, b) or 0) > 0 else ""
    A(f"  {b[:27]:<28}"
      f"{(f'{i_:9.2f}' if have_tput else '        -')}"
      f"{(f'{o_:9.2f}' if have_tput else '        -')}"
      f"{(f'{m_:9.0f}' if have_tput else '        -')}"
      f"{(f'{hi:8.1f}%' if hi is not None else '        -'):>9}"
      f"{(f'{ni:8.1f}%' if ni is not None else '        -'):>9}"
      f"{_c(opt('reqq',reqq,b),6)}{_c(opt('purg',purg,b),7)}{_c(opt('urp',urp,b),5)}"
      f"{_c(opt('parts',parts,b),6)}{_c(opt('leaders',leaders,b),6)} {flagc}")
    csv_append(E("C_BRK"), [TS, EL, b,
                            f"{i_:.3f}" if have_tput else "",
                            f"{o_:.3f}" if have_tput else "",
                            f"{m_:.1f}" if have_tput else "",
                            "" if hi is None else f"{hi:.1f}",
                            "" if ni is None else f"{ni:.1f}",
                            _n(opt("reqq", reqq, b)), _n(opt("respq", respq, b)),
                            _n(opt("purg", purg, b)), _n(opt("urp", urp, b)),
                            _n(opt("parts", parts, b)), _n(opt("leaders", leaders, b)),
                            f"{shrink.get(b,0):.4f}", f"{expand.get(b,0):.4f}",
                            f"{flag.get(b,0):.0f}"])
if not brokers:
    A("      (no broker pods resolved)")

# partition balance across brokers
if tot_lead and len(brokers) > 1:
    lv = [leaders.get(b, 0) for b in brokers]
    if min(lv) > 0:
        A(f"      leader balance: max/min {max(lv)/min(lv):.2f}x "
          f"({'even' if max(lv)/min(lv) < 1.3 else 'UNEVEN — one broker is doing more work'})")

# ---- 3. latency -------------------------------------------------------------
if l_tot:
    A("")
    A("  [3] REQUEST LATENCY  (p99 ms, by request type)")
    A("  ----------------------------------------------------------------------------------")
    A(f"  {'REQUEST':<24}{'total ms':>10}{'queue ms':>10}{'local ms':>10}{'req/s':>10}")
    for rq in sorted(l_tot, key=lambda k: -l_tot[k])[:8]:
        A(f"  {rq[:23]:<24}{l_tot.get(rq,0):10.1f}{l_q.get(rq,0):10.1f}"
          f"{l_l.get(rq,0):10.1f}{l_r.get(rq,0):10.0f}")
        csv_append(E("C_LAT"), [TS, EL, rq, f"{l_tot.get(rq,0):.2f}",
                                f"{l_q.get(rq,0):.2f}", f"{l_l.get(rq,0):.2f}",
                                f"{l_r.get(rq,0):.1f}"])
    A("      queue ms climbing while local ms stays flat = brokers are backed up,")
    A("      not slow — add partitions/brokers rather than tuning the broker.")

# ---- 4. topics --------------------------------------------------------------
if t_in:
    A("")
    A("  [4] TOP TOPICS BY INGRESS")
    A("  ----------------------------------------------------------------------------------")
    A(f"  {'TOPIC':<42}{'MB/s in':>10}{'MB/s out':>10}{'msg/s':>10}")
    for t in sorted(t_in, key=lambda k: -t_in[k])[:10]:
        A(f"  {t[:41]:<42}{t_in[t]/MB:10.3f}{t_out.get(t,0)/MB:10.3f}{t_msg.get(t,0):10.0f}")
    for t in t_in:
        csv_append(E("C_TOP"), [TS, EL, t, f"{t_in[t]/MB:.4f}",
                                f"{t_out.get(t,0)/MB:.4f}", f"{t_msg.get(t,0):.1f}"])

# ---- 5. pods / JVM / disk ---------------------------------------------------
A("")
A("  [5] PODS   (placement, resources, JVM)")
A("  ----------------------------------------------------------------------------------")
A(f"  {'POD':<30}{'ROLE':<12}{'NODE':<16}{'RDY':>5}{'RST':>5}{'CPU':>8}{'MEM Mi':>9}{'heap Mi':>9}{'GC/s':>7}")
watch = [p for p in pods if role_of(p) != "other"] or list(pods)
for p in sorted(watch, key=lambda x: (role_of(x), x)):
    d = pods[p]
    cpu, mem = top.get(p, (0, 0))
    hp = heap.get(p, 0) / MB
    A(f"  {p[:29]:<30}{role_of(p):<12}{d['node'][:15]:<16}{d['ready']:>5}{d['rc']:>5}"
      f"{cpu:7.0f}m{mem:9.0f}{hp:9.0f}{gcc.get(p,0):7.2f}")
    csv_append(E("C_POD"), [TS, EL, p, role_of(p), d["node"], d["phase"],
                            d["ready"], d["rc"], f"{cpu:.0f}", f"{mem:.0f}"])
    if p in heap or p in gcc:
        csv_append(E("C_JVM"), [TS, EL, p, f"{hp:.0f}",
                                f"{gcc.get(p,0):.4f}", f"{gct.get(p,0)*1000:.2f}"])

# node co-location check — brokers should not share a node
bn = {}
for b in brokers:
    if b in pods:
        bn.setdefault(pods[b]["node"], []).append(b)
dupe = {k: v for k, v in bn.items() if len(v) > 1}
if dupe:
    A("      !! brokers sharing a node (a single node failure takes >1 broker):")
    for nd, bl in dupe.items():
        A(f"         {nd}: {', '.join(bl)}")

if pvcu:
    A("")
    A("  [6] STORAGE")
    A("  ----------------------------------------------------------------------------------")
    A(f"  {'PVC':<44}{'used GB':>10}{'size GB':>10}{'used %':>9}")
    for v in sorted(pvcu, key=lambda k: -(pvcu[k] / max(pvcc.get(k, 1), 1))):
        u = pvcu[v] / 1e9
        c = pvcc.get(v, 0) / 1e9
        pct = (u / c * 100) if c else 0
        mark = " !!" if pct > 75 else ""
        A(f"  {v[:43]:<44}{u:10.1f}{c:10.1f}{pct:8.1f}%{mark}")
        owner = ""
        for b in brokers + controllers:
            if v.endswith(b):
                owner = b
                break
        csv_append(E("C_DSK"), [TS, EL, owner, v, f"{u:.2f}", f"{c:.2f}", f"{pct:.1f}"])

print("\n".join(O))
# values the shell keeps for the closing summary
print(f"##STAT CUR_IN={f'{tot_in:.2f}' if have_tput else ''} "
      f"CUR_OUT={f'{tot_out:.2f}' if have_tput else ''} "
      f"CUR_MSG={f'{tot_msg:.0f}' if have_tput else ''} "
      f"CUR_IDLE={'' if min_idle is None else f'{min_idle:.1f}'} "
      f"CUR_URP={'' if tot_urp is None else f'{tot_urp:.0f}'}")
