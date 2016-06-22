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

cfg = json.load(open('conf/settings.conf'))
try:
    if len(sys.argv) != 2 and len(sys.argv) != 3:
        raise ValueError
    node_id = int(sys.argv[1])
except ValueError:
    print ("Usage: start_server node_id")
    exit()
if len(sys.argv) == 3:
    conn = http.client.HTTPConnection(cfg["n{0:02d}".format(node_id)] + ":" + cfg["port"])
    conn.request(method="GET", url='/kvman/restart')
    res = conn.getresponse()
    res_json = json.loads(res.read().decode('utf-8'))
else:
    os.system("gnome-terminal -x python3 kvpaxos.py {} & disown".format(node_id))