# -*- coding: utf-8 -*-
"""3002's account QQ is INSIDE the blacklist. Its bot echoes must be excluded
BEFORE the blacklist logic, so they must NOT close the follow switch."""
import base64, hashlib, json, socket, struct, threading, time
GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
GROUP = 1124334011
TARGET = "3288574505"
PORTS = [3001, 3002, 3003]
# 3002's account IS a blacklist member; 3003's is not.
SELF_IDS = {3001: 999, 3002: 3117669520, 3003: 997}
t0 = [None]
conns = {}
sends = []
lock = threading.Lock()

def hs(c):
    d = b""
    while b"\r\n\r\n" not in d:
        k = c.recv(4096)
        if not k: return False
        d += k
    key = [l.split(":",1)[1].strip() for l in d.decode("latin1").split("\r\n") if l.lower().startswith("sec-websocket-key:")][0]
    a = base64.b64encode(hashlib.sha1((key+GUID).encode()).digest()).decode()
    c.sendall(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n" % a).encode())
    return True

def rf(s):
    h = b""
    while len(h) < 2: h += s.recv(2-len(h))
    op, ln = h[0]&0xF, h[1]&0x7F
    if ln == 126: ln = struct.unpack(">H", s.recv(2))[0]
    elif ln == 127: ln = struct.unpack(">Q", s.recv(8))[0]
    p = b""
    if h[1] & 0x80:
        mk = s.recv(4)
        while len(p) < ln: p += s.recv(min(65536, ln-len(p)))
        p = bytes(b ^ mk[i&3] for i, b in enumerate(p))
    else:
        while len(p) < ln:
            c = s.recv(min(65536, ln-len(p)))
            if not c: return None, None
            p += c
    return op, p

def sf(s, pl, op=0x1):
    n = len(pl)
    h = struct.pack("!BB", 0x80|op, n) if n < 126 else (struct.pack("!BBH", 0x80|op, 126, n) if n < 65536 else struct.pack("!BBQ", 0x80|op, 127, n))
    s.sendall(h + pl)

def reader(port):
    s = conns[port]
    while True:
        try: op, pl = rf(s)
        except OSError: break
        if op is None or op == 8: break
        if op not in (1, 2): continue
        try: m = json.loads(pl.decode("utf-8"))
        except Exception: continue
        if m.get("action") != "send_group_msg": continue
        ct = m.get("params", {}).get("message", "")
        now = time.time() - t0[0]
        with lock: sends.append((now, port, ct))
        print("[SRV] %.2f port=%d reply %r" % (now, port, ct), flush=True)
        sf(s, json.dumps({"status":"ok","retcode":0,"data":{"message_id":1},"echo":m.get("echo","")}).encode())

def accept_loop(port):
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port)); srv.listen(5)
    while True:
        c, _ = srv.accept()
        if hs(c): break
        c.close()
    with lock: conns[port] = c
    print("[SRV] port %d connected" % port, flush=True)
    reader(port)

for p in PORTS:
    threading.Thread(target=accept_loop, args=(p,), daemon=True).start()
while len(conns) < len(PORTS): time.sleep(0.05)
t0[0] = time.time()
print("[SRV] ready (3002 account=3117669520 IS blacklisted)", flush=True)

def ev(user, raw):
    for p in PORTS:
        sf(conns[p], json.dumps({"post_type":"message","message_type":"group","group_id":GROUP,
                                 "user_id":user,"self_id":SELF_IDS[p],"raw_message":raw}).encode())
    print("[SRV] %.2f event %s %r" % (time.time()-t0[0], user, raw), flush=True)

def bot_echo(content):
    for p in PORTS:
        sf(conns[p], json.dumps({"post_type":"message","message_type":"group","group_id":GROUP,
                                 "user_id":str(SELF_IDS[p]),"self_id":SELF_IDS[p],"raw_message":content}).encode())
    print("[SRV] %.2f BOT-ECHO %r from %r" % (time.time()-t0[0], content, [SELF_IDS[p] for p in PORTS]), flush=True)

def at(t):
    d = t - (time.time()-t0[0])
    if d > 0: time.sleep(d)

at(0.5); ev(TARGET, "[CQ:image,file=1.image]")     # round 1 ON
at(2.0); ev("111", "9")
at(3.0); ev("333", "9")                            # round 1 -> 3 replies
at(5.0); ev(TARGET, "[CQ:image,file=2.image]")     # round 2 ON (clear)
at(5.5); bot_echo("9")                             # blacklisted-bot echo -> must NOT close
at(7.0); ev("111", "9")
at(8.0); ev("333", "9")                            # round 2 -> 3 replies (proves switch alive)
time.sleep(2.0)
with lock: S = list(sends)
n = len(S)
ok = (n == 6)
print("[KEY] blacklisted-bot echo: %s (expect 6 replies = 2 rounds, got %d)" %
      ("PASS" if ok else "FAIL", n), flush=True)
