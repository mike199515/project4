# !usr/bin/env python3
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
import urllib
import json
import sys
import os
import http.client
from database import Database
from paxos_peer import PaxosPeer
from kvpaxos import KvpaxosHttpServer, ProjectHTTPRequestHandler
import threading
import queue
import time
'''
peers = ["localhost:8000", "localhost:8001", "localhost:8002"]
server_str = ["localhost:5000", "localhost:5001", "localhost:5002"]
server_tup = [s.split(":") for s in server_str]
server_tup = [(l[0], int(l[1])) for l in server_tup]
px = [PaxosPeer(peers, i) for i in range(3)]
servers = [KvpaxosHttpServer(i, px[i], server_tup[i], ProjectHTTPRequestHandler) for i in range(3)]


def run(i):
    servers[i].serve_forever()


for i in range(3):
    threading.Thread(target=run, args=(i,)).start()
'''

def dump(server_id):
    print("request_sent")
    conn = http.client.HTTPConnection(server_str[server_id])
    conn.request(method="GET", url='/kvman/dump')
    res = conn.getresponse()
    res_json = json.loads(res.read().decode('utf-8'))
    print(res_json)


def op(server_id, key):
    print("request_sent")
    conn = http.client.HTTPConnection(server_str[server_id])
    conn.request(method="POST", url='/kv/insert', body="key={}&value=v".format(key))
    res = conn.getresponse()
    res_json = json.loads(res.read().decode('utf-8'))
    print("request{}:{}".format((server_id,key),res_json))

'''
for i in range(30):
    threading.Thread(target=op, args=(i % 3, i)).start()
    time.sleep(0.01)

threading.Thread(target=dump, args=(i % 3,)).start()
'''
'''
for i in range(10):
    threading.Thread(target=op, args=(0, 0)).start()
    threading.Thread(target=op, args=(1, 0)).start()
    threading.Thread(target=op, args=(2, 0)).start()
'''

print("request_sent")
conn = http.client.HTTPConnection("127.0.0.1:8888")
conn.request(method="GET", url='/kvman/dump')
res = conn.getresponse()
res_json = json.loads(res.read().decode('utf-8'))
print(res_json)
time.sleep(2)

print("request_sent")
conn = http.client.HTTPConnection("127.0.0.1:8888")
conn.request(method="GET", url='/kvman/shutdown')
res = conn.getresponse()
res_json = json.loads(res.read().decode('utf-8'))
print(res_json)
time.sleep(2)

try:
    print("request_sent")
    conn = http.client.HTTPConnection("127.0.0.1:8888")
    conn.request(method="GET", url='/kvman/dump')
    res = conn.getresponse()
    res_json = json.loads(res.read().decode('utf-8'))
    print(res_json)
except Exception:
    pass
time.sleep(2)

print("request_sent")
conn = http.client.HTTPConnection("127.0.0.1:8888")
conn.request(method="GET", url='/kvman/restart')
res = conn.getresponse()
res_json = json.loads(res.read().decode('utf-8'))
print(res_json)
time.sleep(2)

print("request_sent")
conn = http.client.HTTPConnection("127.0.0.1:8888")
conn.request(method="GET", url='/kvman/dump')
res = conn.getresponse()
res_json = json.loads(res.read().decode('utf-8'))
print(res_json)
