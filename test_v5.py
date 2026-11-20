# -*- coding: utf-8 -*-
"""Full verification suite for Snowluma V5.0.1 (follow + kou-digit).

Events are broadcast to ALL ports with per-port self_id (like real Snowluma).
Bot echo test: the bots' own replies are replayed as group messages to make
sure they are excluded from pairing (V5.0.1 fix).

Modes:
  follow       ~45s  @全体/image trigger, pair, bot-echo exclusion,
                     blacklist closes switch, multi-digit, FAIL, stale
  kou          ~70s  kou only from 3001; success closes V4 switch; cooldown
  mute         ~25s  kou rejected -> unban notice -> resend -> success
  mute_timeout ~135s kou rejected -> no notice -> give up at 2min
  kou_2s       ~12s  no response -> 2s timeout -> cooldown
"""
import base64, hashlib, json, socket, struct, sys, threading, time

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
GROUP = 1124334011
TARGET = "3288574505"
PORTS = [3001, 3002, 3003]
SELF_IDS = {3001: 999, 3002: 998, 3003: 997}   # each port = one QQ account
mode = sys.argv[1] if len(sys.argv) > 1 else "follow"

t_real = [None]
conns = {}
sends = []        # (t, port, content)
events = []       # (t, user, raw)  real group events for latency
state = {"ok": True, "swallow": False}
lock = threading.Lock()

def handshake(sock):
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            return False
        data += chunk
    key = ""
    for line in data.decode("latin1").split("\r\n"):
        if line.lower().startswith("sec-websocket-key:"):
            key = line.split(":", 1)[1].strip()
    if not key:
        return False
    accept = base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()
    sock.sendall(("HTTP/1.1 101 Switching Protocols\r\n"
                  "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                  "Sec-WebSocket-Accept: %s\r\n\r\n" % accept).encode())
    return True

def recv_frame(sock):
    hdr = b""
    while len(hdr) < 2:
        c = sock.recv(2 - len(hdr))
        if not c:
            return None, None
        hdr += c
    op = hdr[0] & 0x0F
    ln = hdr[1] & 0x7F
    if ln == 126:
        ext = b""
        while len(ext) < 2:
            ext += sock.recv(2 - len(ext))
        ln = struct.unpack(">H", ext)[0]
    elif ln == 127:
        ext = b""
        while len(ext) < 8:
            ext += sock.recv(8 - len(ext))
        ln = struct.unpack(">Q", ext)[0]
    payload = b""
    if hdr[1] & 0x80:
        mk = sock.recv(4)
        while len(payload) < ln:
            payload += sock.recv(min(65536, ln - len(payload)))
        payload = bytes(b ^ mk[i & 3] for i, b in enumerate(payload))
    else:
        while len(payload) < ln:
            c = sock.recv(min(65536, ln - len(payload)))
            if not c:
                return None, None
            payload += c
    return op, payload

def send_frame(sock, payload, opcode=0x1):
    n = len(payload)
    if n < 126:
        hdr = struct.pack("!BB", 0x80 | opcode, n)
    elif n < 65536:
        hdr = struct.pack("!BBH", 0x80 | opcode, 126, n)
    else:
        hdr = struct.pack("!BBQ", 0x80 | opcode, 127, n)
    sock.sendall(hdr + payload)

def reader(port):
    sock = conns[port]
    while True:
        try:
            op, payload = recv_frame(sock)
        except OSError:
            break
        if op is None or op == 8:
            break
        if op not in (1, 2):
            continue
        try:
            msg = json.loads(payload.decode("utf-8"))
        except Exception:
            continue
        if msg.get("action") != "send_group_msg":
            continue
        content = msg.get("params", {}).get("message", "")
        now = time.time() - t_real[0]
        with lock:
            sends.append((now, port, content))
        print("[SRV] %.2f port=%d reply %r" % (now, port, content), flush=True)
        if state["swallow"]:
            continue
        if state["ok"]:
            resp = {"status": "ok", "retcode": 0, "data": {"message_id": 1},
                    "echo": msg.get("echo", "")}
        else:
            resp = {"status": "failed", "retcode": 1200, "data": None,
                    "wording": "机器人已被禁言，无法发言",
                    "echo": msg.get("echo", "")}
        try:
            send_frame(sock, json.dumps(resp).encode())
        except OSError:
            break

def accept_loop(port):
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(5)
    print("[SRV] listening on %d" % port, flush=True)
    while True:
        c, _ = srv.accept()
        if handshake(c):
            break
        c.close()
    with lock:
        conns[port] = c
    print("[SRV] port %d connected" % port, flush=True)
    reader(port)

threads = [threading.Thread(target=accept_loop, args=(p,), daemon=True)
           for p in PORTS]
for th in threads:
    th.start()
while len(conns) < len(PORTS):
    time.sleep(0.05)
t_real[0] = time.time()
print("[SRV] all ports ready, t=0", flush=True)

def broadcast(user, raw, stale=False, record=True):
    """每个端口收到同一消息，但 self_id 是该端口自己的账号（真实行为）"""
    now = time.time() - t_real[0]
    if record:
        with lock:
            events.append((now, user, raw))
    for p in PORTS:
        ev = {"post_type": "message", "message_type": "group",
              "group_id": GROUP, "user_id": user,
              "self_id": SELF_IDS[p], "raw_message": raw}
        if stale:
            ev["time"] = int(time.time()) - 100
        send_frame(conns[p], json.dumps(ev).encode())
    print("[SRV] %.2f event(user=%s)%s %r" %
          (now, user, " [STALE]" if stale else "", raw), flush=True)

def bot_echo(content):
    """机器人自己的回复回显（三个账号各发一条，模拟群里看到自己的消息）"""
    now = time.time() - t_real[0]
    for p in PORTS:
        ev = {"post_type": "message", "message_type": "group",
              "group_id": GROUP, "user_id": str(SELF_IDS[p]),
              "self_id": SELF_IDS[p], "raw_message": content}
        send_frame(conns[p], json.dumps(ev).encode())
    print("[SRV] %.2f BOT-ECHO %r from %r" %
          (now, content, [SELF_IDS[p] for p in PORTS]), flush=True)

def notice_unban():
    now = time.time() - t_real[0]
    send_frame(conns[3001], json.dumps({
        "post_type": "notice", "notice_type": "group_ban",
        "group_id": GROUP, "user_id": str(SELF_IDS[3001]),
        "duration": 0, "self_id": SELF_IDS[3001],
    }).encode())
    print("[SRV] %.2f NOTICE unban" % now, flush=True)

AT_ALL = "[CQ:at,qq=all] 请扣3"
IMG = "[CQ:image,file=%d.image]"

def run_plan(pl):
    for item in pl:
        t = item[1]
        d = t - (time.time() - t_real[0])
        if d > 0:
            time.sleep(d)
        kind = item[0]
        if kind == "ev":
            broadcast(item[2], item[3], item[4] if len(item) > 4 else False)
        elif kind == "bot":
            bot_echo(item[2])
        elif kind == "notice":
            notice_unban()
        elif kind == "fail_on":
            state["ok"] = False
            print("[SRV] FAIL on", flush=True)
        elif kind == "fail_off":
            state["ok"] = True
            print("[SRV] FAIL off", flush=True)
        elif kind == "swallow_on":
            state["swallow"] = True
            print("[SRV] SWALLOW on", flush=True)

if mode == "follow":
    plan = [
        ("ev", 1.0, "111", "8"),                        # switch off
        ("ev", 2.0, TARGET, "[CQ:at,qq=all]"),          # @全体 ON
        ("ev", 4.0, "111", "9"),
        ("ev", 5.0, "222", "6"),
        ("ev", 6.0, "333", "9"),                        # round 9 (#1)
        ("ev", 8.0, "111", "9"),                        # closed
        ("ev", 9.0, "111", "88"),                       # multi-digit ignored
        ("ev", 11.0, TARGET, IMG % 2),                  # ON (clear)
        ("bot", 11.5, "9"),                             # BOT ECHO while ON!
        ("ev", 13.0, "111", "9"),                       # must be FIRST (no send!)
        ("ev", 14.0, "333", "9"),                       # pair -> round 9 (#2)
        ("ev", 16.0, "111", "5"),
        ("ev", 17.0, "3117669520", "随便说句话"),         # BL closes
        ("ev", 18.0, "333", "5"),                       # nothing
        ("ev", 20.0, TARGET, IMG % 3),                  # ON
        ("ev", 22.0, "111", "3"),
        ("ev", 23.0, "333", "3"),                       # round 3
        ("fail_on", 25.5),
        ("ev", 26.0, TARGET, IMG % 4),                  # ON
        ("ev", 27.0, "111", "2"),
        ("ev", 28.0, "333", "2"),                       # round 2 -> FAIL
        ("fail_off", 29.5),
        ("ev", 31.0, TARGET, IMG % 5),                  # ON
        ("ev", 32.0, "111", "7"),
        ("ev", 33.0, "333", "7"),                       # round 7
        ("ev", 35.0, TARGET, IMG % 6, True),            # STALE dropped
        ("ev", 36.0, "111", "6"),
        ("ev", 37.0, "333", "6"),                       # nothing
    ]
    expected = {p: ["9", "9", "3", "2", "7"] for p in PORTS}
elif mode == "kou":
    plan = [
        ("ev", 1.0, TARGET, AT_ALL),                    # kou 3 (3001) + switch OPEN
        ("ev", 3.0, "111", "9"),
        ("ev", 4.0, "333", "9"),                        # V5.1.0: follow 9 MUST work
        ("ev", 6.0, TARGET, "[CQ:at,qq=all] 扣5"),       # kou cd; re-open + clear
        ("ev", 8.0, "111", "8"),
        ("ev", 9.0, "333", "8"),                        # follow 8
        ("ev", 62.0, TARGET, "[CQ:at,qq=all] 扣7"),      # cd expired -> kou 7
        ("ev", 64.0, "111", "2"),
        ("ev", 65.0, "333", "2"),                       # follow 2 (still works after kou)
    ]
    expected = {p: ["3", "9", "8", "7", "2"] for p in PORTS}   # V5.1.1: kou from all ports
elif mode == "mute":
    plan = [
        ("fail_on", 1.0),
        ("ev", 2.0, TARGET, "[CQ:at,qq=all] 扣6"),
        ("fail_off", 5.0),
        ("notice", 6.0),
    ]
    expected = {p: ["6", "6"] for p in PORTS}
elif mode == "mute_timeout":
    plan = [
        ("fail_on", 1.0),
        ("ev", 2.0, TARGET, "[CQ:at,qq=all] 扣8"),
        ("fail_off", 5.0),
        ("ev", 125.0, TARGET, "[CQ:at,qq=all] 扣9"),
    ]
    expected = {p: ["8", "9"] for p in PORTS}
elif mode == "kou_2s":
    plan = [
        ("swallow_on", 0.5),
        ("ev", 1.0, TARGET, "[CQ:at,qq=all] 扣2"),
        ("ev", 5.0, TARGET, "[CQ:at,qq=all] 扣9"),
    ]
    expected = {p: ["2"] for p in PORTS}

run_plan(plan)
time.sleep(3.0)

with lock:
    S = list(sends)
    E = list(events)
print("[SUM] total sends=%d" % len(S), flush=True)
for t, p, ct in S:
    deltas = [t - et for et, u, r in E if r == ct or r.endswith("扣" + ct)]
    pos = [d * 1000 for d in deltas if d >= 0]
    lat = ("latency %.1f ms" % min(pos)) if pos else ""
    print("[SUM]   port=%d t=%.2f %r  %s" % (p, t, ct, lat), flush=True)

per_port = {p: [ct for tt, pp, ct in S if pp == p] for p in PORTS}
ok = all(per_port[p] == expected[p] for p in PORTS)

if mode == "follow":
    nines = [t for t, p, ct in S if p == 3001 and ct == "9"]
    echo_ok = len(nines) >= 2 and nines[1] > 13.5
    print("[KEY] bot-echo exclusion: %s (2nd '9' at t=%.2f, must be >13.5)"
          % ("PASS" if echo_ok else "FAIL", nines[1] if len(nines) > 1 else -1),
          flush=True)
    ok = ok and echo_ok

print("[KEY] %s: %s" % (mode, "PASS" if ok else "FAIL"), flush=True)
for p in PORTS:
    print("[KEY]   port %d: %r" % (p, per_port[p]), flush=True)
