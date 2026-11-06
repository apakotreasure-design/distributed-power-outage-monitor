"""
Distributed Power Outage & Load-Shedding Monitor (prototype, stdlib only)

Run everything:      python monitor.py
Run separately:      python monitor.py node A | node B | node C | sim
Dashboard:           http://localhost:8001/

Design (AP system):
  * Devices keep a persistent SQLite OUTBOX and flush to their regional node when reachable.
  * Events carry a UUID (idempotent ingest) and a Hybrid Logical Clock stamp (ordering).
  * Each regional node has its own SQLite DB and keeps serving during partitions.
  * Nodes converge by anti-entropy: pull events from peers by (peer seq) cursor.
  * Current state is DERIVED from the event log (LWW per device by HLC).
  * Failure detection: heartbeat timeout -> SUSPECT; last-gasp -> confirmed OUT.
  * Quorum: a cell is OUT when >= QUORUM of its devices are OUT/SUSPECT.
  * Whole region silent with no last gasp -> UNREACHABLE (power vs network is ambiguous).
"""
import sqlite3, threading, time, json, uuid, sys, random, os
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib import request as urlreq, parse

REGIONS = {"A": 8001, "B": 8002, "C": 8003}
SIM_PORT = 8000
CELLS = {"A": ["A1", "A2"], "B": ["B1", "B2"], "C": ["C1", "C2"]}
DEVICES_PER_CELL = 3          # device 0,1 have battery (last gasp); device 2 does not
QUORUM = 2                    # k-of-n devices must agree
HB_INTERVAL = 1.0
SUSPECT_AFTER = 5.0           # seconds without heartbeat
SYNC_INTERVAL = 1.0
J = "application/json"

ALL_DEVICES = [(f"{c}-d{i}", c, r) for r, cs in CELLS.items() for c in cs for i in range(DEVICES_PER_CELL)]


# ---------- helpers ----------
def http_get(url, timeout=2):
    with urlreq.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())


def http_post(url, obj, timeout=2):
    req = urlreq.Request(url, json.dumps(obj).encode(), {"Content-Type": J})
    with urlreq.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


class HLC:
    """Hybrid Logical Clock: (physical ms, logical counter, node id), string-sortable."""
    def __init__(self, node):
        self.l, self.c, self.node, self.lock = 0, 0, node, threading.Lock()

    def now(self):
        with self.lock:
            pt = int(time.time() * 1000)
            if pt > self.l:
                self.l, self.c = pt, 0
            else:
                self.c += 1
            return f"{self.l:013d}-{self.c:05d}-{self.node}"


def serve(port, handle):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a): pass

        def go(self, body=None):
            u = parse.urlparse(self.path)
            q = dict(parse.parse_qsl(u.query))
            try:
                code, obj, ct = handle(u.path, q, body)
            except Exception as e:
                code, obj, ct = 500, {"error": str(e)}, J
            data = obj if isinstance(obj, bytes) else json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", ct)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self): self.go()

        def do_POST(self):
            n = int(self.headers.get("Content-Length", 0))
            self.go(json.loads(self.rfile.read(n) or b"null"))

    srv = ThreadingHTTPServer(("0.0.0.0", port), H)
    srv.daemon_threads = True
    srv.serve_forever()


# ---------- regional node ----------
class Node:
    def __init__(self, name):
        self.name, self.partitioned, self.cursors = name, False, {}
        self.lock = threading.Lock()
        self.db = sqlite3.connect(f"node_{name}.db", check_same_thread=False)
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS events(
              seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE, device TEXT,
              cell TEXT, region TEXT, type TEXT, hlc TEXT, origin TEXT);
            CREATE INDEX IF NOT EXISTS ix_hlc ON events(hlc);
            CREATE INDEX IF NOT EXISTS ix_dev ON events(device, hlc);""")

    def add(self, evs, origin):
        n = 0
        with self.lock:
            for e in evs:   # INSERT OR IGNORE on UUID => idempotent (at-least-once safe)
                cur = self.db.execute(
                    "INSERT OR IGNORE INTO events(id,device,cell,region,type,hlc,origin) VALUES(?,?,?,?,?,?,?)",
                    (e["id"], e["device"], e["cell"], e["region"], e["type"], e["hlc"], origin))
                n += cur.rowcount
            self.db.commit()
        return n

    def since(self, after, limit=1000):
        with self.lock:
            rows = self.db.execute(
                "SELECT seq,id,device,cell,region,type,hlc FROM events WHERE seq>? ORDER BY seq LIMIT ?",
                (after, limit)).fetchall()
        keys = ["seq", "id", "device", "cell", "region", "type", "hlc"]
        return [dict(zip(keys, r)) for r in rows]

    def sync_loop(self):
        while True:
            time.sleep(SYNC_INTERVAL)
            if self.partitioned:
                continue
            for peer, port in REGIONS.items():
                if peer == self.name:
                    continue
                try:
                    after = self.cursors.get(peer, 0)
                    evs = http_get(f"http://127.0.0.1:{port}/events?after={after}")
                    if evs:
                        self.add(evs, peer)
                        self.cursors[peer] = evs[-1]["seq"]
                except Exception:
                    pass   # peer down/partitioned: keep serving locally, retry next round

    def state(self):
        now = int(time.time() * 1000)
        with self.lock:
            last = {d: h for d, h in self.db.execute("SELECT device,MAX(hlc) FROM events GROUP BY device")}
            st = {d: (t, h) for d, t, h in self.db.execute(
                "SELECT device,type,MAX(hlc) FROM events WHERE type!='HEARTBEAT' GROUP BY device")}
            total = self.db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            settled = self.db.execute("SELECT COUNT(*) FROM events WHERE hlc<?",
                                      (f"{now - 5000:013d}",)).fetchone()[0]
        dev = {}
        for d, c, r in ALL_DEVICES:
            h = last.get(d)
            if h is None:
                dev[d] = "SUSPECT"
            elif d in st and st[d][0] == "POWER_LOST" and st[d][1] == h:
                dev[d] = "OUT"                      # last gasp is the newest thing we know
            else:
                dev[d] = "SUSPECT" if now - int(h[:13]) > SUSPECT_AFTER * 1000 else "OK"
        cells = {}
        for r, cs in CELLS.items():
            rdev = [dev[d] for d, c, rr in ALL_DEVICES if rr == r]
            unreachable = all(x == "SUSPECT" for x in rdev)   # silent, no last gasp
            for c in cs:
                ds = {d: dev[d] for d, cc, rr in ALL_DEVICES if cc == c}
                bad = sum(1 for x in ds.values() if x in ("OUT", "SUSPECT"))
                status = ("UNREACHABLE" if unreachable else
                          "OUT" if bad >= QUORUM else "DEGRADED" if bad else "OK")
                cells[c] = {"status": status, "devices": ds}
        return {"node": self.name, "partitioned": self.partitioned,
                "total": total, "settled": settled, "cells": cells}


def run_node(name):
    node = Node(name)
    threading.Thread(target=node.sync_loop, daemon=True).start()

    def handle(path, q, body):
        if path == "/":
            return 200, DASH.encode(), "text/html"
        if path == "/ingest":
            return 200, {"added": node.add(body, node.name)}, J
        if path == "/events":
            if node.partitioned:
                return 503, {"error": "partitioned"}, J
            return 200, node.since(int(q.get("after", 0))), J
        if path == "/state":
            return 200, node.state(), J
        if path == "/admin/partition":
            node.partitioned = q.get("on") == "1"
            return 200, {"partitioned": node.partitioned}, J
        return 404, {}, J

    serve(REGIONS[name], handle)


# ---------- device simulator ----------
class Sim:
    def __init__(self):
        self.uplink = {r: True for r in REGIONS}
        self.lock = threading.Lock()
        self.db = sqlite3.connect("sim_outbox.db", check_same_thread=False)   # persistent outbox
        self.db.execute("CREATE TABLE IF NOT EXISTS outbox(id TEXT, device TEXT, payload TEXT)")
        self.devices = []
        for i, (d, c, r) in enumerate(ALL_DEVICES):
            self.devices.append(Device(self, d, c, r, battery=(i % DEVICES_PER_CELL) < 2))

    def start(self):
        for d in self.devices:
            threading.Thread(target=d.run, daemon=True).start()

    def set_power(self, cell, on):
        for d in self.devices:
            if d.cell != cell:
                continue
            d.power = on
            if not on and d.battery:
                d.emit("POWER_LOST")      # last gasp from battery/supercap
                d.flush()
            elif on:
                d.emit("POWER_OK")

    def status(self):
        with self.lock:
            n = self.db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
        return {"uplink": self.uplink, "outbox_pending": n,
                "power": {c: all(d.power for d in self.devices if d.cell == c)
                          for cs in CELLS.values() for c in cs}}


class Device:
    def __init__(self, sim, id, cell, region, battery):
        self.sim, self.id, self.cell, self.region, self.battery = sim, id, cell, region, battery
        self.power, self.hlc = True, HLC(id)

    def emit(self, typ):
        e = {"id": str(uuid.uuid4()), "device": self.id, "cell": self.cell,
             "region": self.region, "type": typ, "hlc": self.hlc.now()}
        with self.sim.lock:
            self.sim.db.execute("INSERT INTO outbox VALUES(?,?,?)", (e["id"], self.id, json.dumps(e)))
            self.sim.db.commit()

    def flush(self):
        if not self.sim.uplink[self.region]:
            return
        with self.sim.lock:
            rows = self.sim.db.execute(
                "SELECT id,payload FROM outbox WHERE device=? ORDER BY rowid LIMIT 200", (self.id,)).fetchall()
        if not rows:
            return
        try:
            http_post(f"http://127.0.0.1:{REGIONS[self.region]}/ingest", [json.loads(p) for _, p in rows])
            with self.sim.lock:   # delete only after ack => at-least-once
                self.sim.db.executemany("DELETE FROM outbox WHERE id=?", [(i,) for i, _ in rows])
                self.sim.db.commit()
        except Exception:
            pass   # node unreachable: keep buffering

    def run(self):
        while True:
            time.sleep(HB_INTERVAL * (0.8 + 0.4 * random.random()))
            if self.power:
                self.emit("HEARTBEAT")
                self.flush()
            elif self.battery:
                self.flush()


def run_sim():
    sim = Sim()
    sim.start()

    def handle(path, q, body):
        if path == "/sim/power":
            sim.set_power(q["cell"], q["on"] == "1")
        elif path == "/sim/uplink":
            sim.uplink[q["region"]] = q["on"] == "1"
        elif path != "/sim/status":
            return 404, {}, J
        return 200, sim.status(), J

    serve(SIM_PORT, handle)


# ---------- dashboard ----------
DASH = """<!doctype html><meta charset=utf-8><title>Outage Monitor</title>
<style>
body{font:14px system-ui;margin:16px;background:#0f1419;color:#e6e6e6}
.row{display:flex;gap:12px;flex-wrap:wrap}
.node{flex:1;min-width:280px;background:#1a212b;border-radius:8px;padding:10px}
.cells{display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-top:6px}
.cell{border-radius:6px;padding:6px;font-size:12px}
.OK{background:#1f6f3f}.DEGRADED{background:#a8760f}.OUT{background:#a82a2a}.UNREACHABLE{background:#555}
.dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:3px}
.d-OK{background:#5ee08a}.d-SUSPECT{background:#f0b429}.d-OUT{background:#ff5c5c}
button{margin:2px;padding:4px 8px;cursor:pointer}
#banner{font-size:18px;margin:10px 0}
</style>
<h2>Distributed Power Outage & Load-Shedding Monitor</h2>
<p style="color:#aaa;margin-top:-8px">Regional nodes • Persistent outbox • Anti-entropy sync • Quorum-based status</p>
<div id=banner></div><div id=ctl></div><br><div class=row id=nodes></div>
<script>
const H=location.hostname||'127.0.0.1',P={A:8001,B:8002,C:8003},SIM=`http://${H}:8000`;
const CELLS=['A1','A2','B1','B2','C1','C2'];
const call=u=>fetch(u).catch(()=>{});
ctl.innerHTML=
 CELLS.map(c=>`Cell ${c}: <button onclick="call(SIM+'/sim/power?cell=${c}&on=0')">Cut power</button><button onclick="call(SIM+'/sim/power?cell=${c}&on=1')">Restore power</button><br>`).join('')+'<br>'+
 Object.keys(P).map(r=>`Region ${r}: <button onclick="call(SIM+'/sim/uplink?region=${r}&on=0')">Cut device uplink</button><button onclick="call(SIM+'/sim/uplink?region=${r}&on=1')">Restore uplink</button> <button onclick="call('http://${H}:${P[r]}/admin/partition?on=1')">Partition node</button><button onclick="call('http://${H}:${P[r]}/admin/partition?on=0')">Heal node</button><br>`).join('');
async function tick(){
 const S={};
 await Promise.all(Object.entries(P).map(async([k,p])=>{try{S[k]=await (await fetch(`http://${H}:${p}/state`)).json()}catch(e){S[k]=null}}));
 let html='';
 for(const k in S){const s=S[k];
  if(!s){html+=`<div class=node><b>Node ${k}</b> DOWN</div>`;continue}
  html+=`<div class=node><b>Node ${k}'s view</b> ${s.partitioned?'<span style="color:#ff5c5c"> PARTITIONED</span>':''}<br>events: ${s.total} (settled: ${s.settled})<div class=cells>`+
  Object.entries(s.cells).map(([c,v])=>`<div class="cell ${v.status}"><b>${c}</b> ${v.status}<br>${Object.values(v.devices).map(d=>`<span class="dot d-${d}"></span>`).join('')}</div>`).join('')+'</div></div>';}
 nodes.innerHTML=html;
 const set=Object.values(S).filter(Boolean).map(s=>s.settled);
 banner.innerHTML=set.length&&Math.max(...set)==Math.min(...set)?'&#9989; All nodes converged':'&#9888;&#65039; Nodes diverged (partitioned or syncing)';
}
setInterval(tick,1000);tick();
</script>"""


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    if mode == "all":
        for f in os.listdir("."):
            if f.startswith(("node_", "sim_outbox")):
                os.remove(f)
        for r in REGIONS:
            threading.Thread(target=run_node, args=(r,), daemon=True).start()
        threading.Thread(target=run_sim, daemon=True).start()
        print("Dashboard: http://localhost:8001/   (Ctrl+C to stop)")
        while True:
            time.sleep(3600)
    elif mode == "node":
        run_node(sys.argv[2])
    elif mode == "sim":
        run_sim()