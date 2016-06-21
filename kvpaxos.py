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
import threading
import queue
import time


class ProjectHTTPRequestHandler(BaseHTTPRequestHandler):
    METHODS = {'insert', 'delete', 'get', 'update', 'serialize', 'countkey', 'dump', 'shutdown'}

    @staticmethod
    def parse_input(input_str):
        if input_str is None:
            return None
        ret = dict()
        inputs = input_str.split('&')
        for input in inputs:
            key, value = input.split('=')
            value = urllib.parse.unquote(value, encoding='utf-8', errors='replace')
            ret[key] = value
        #trick to avoid crash
        if "requestid" in ret:
            ret.pop("requestid")
        return ret

    @staticmethod
    def gen_output(output_dict):
        ret = json.dumps(output_dict)
        # print("output:{}".format(ret))
        return ret

    def countkey_request(self, ins):
        assert (self.command == "GET"), "wrong HTTP method"
        keycount = self.server.database.countkey()
        outs = {'result': str(keycount)}
        return outs

    def dump_request(self, ins):
        assert (self.command == "GET"), "wrong HTTP method"
        outs = self.server.database.dump()
        return outs

    def shutdown_request(self, ins):
        os.system('bin/stop_server -b')

    def serialize_request(self, ins):
        # we need to verify it is the other server that calls us
        assert (self.command == "GET"), "wrong HTTP method"
        data_str = self.server.database.serialize()
        outs = {'data': data_str}
        return outs

    def insert_request(self, ins):
        assert (self.command == "POST"), 'wrong HTTP method'
        assert (len(ins) == 2 and 'key' in ins and 'value' in ins), 'wrong input'
        key, value = ins['key'], ins['value']
        success = self.server.database.insert(key, value)
        outs = {'success': success}
        return outs

    def delete_request(self, ins):
        assert (self.command == "POST"), 'wrong HTTP method'
        assert (len(ins) == 1 and 'key' in ins), 'wrong input'
        key = ins['key']
        value = self.server.database.delete(key)
        if value:
            outs = {'success': True}
        else:
            outs = {'success': False}
        return outs

    def update_request(self, ins):
        assert (self.command == "POST"), 'wrong HTTP method'
        assert (len(ins) == 2 and 'key' in ins and 'value' in ins), 'wrong input'
        key, value = ins['key'], ins['value']
        success = self.server.database.update(key, value)
        outs = {'success': success}
        return outs

    def get_request(self, ins):
        assert (self.command == "GET"), 'wrong HTTP method'
        assert (len(ins) == 1 and '?key' in ins), 'wrong input'
        key = ins['?key']
        value = self.server.database.get(key)
        if value:
            outs = {'success': True, 'value': value}
        else:
            outs = {'success': False, 'value': ""}
        return outs

    def do_GET(self):
        if "?" in self.path:
            self.path = self.path.replace("?", "/?", 1)
        self.consensus_request()

    def do_POST(self):
        try:
            length = int(self.headers["Content-Length"])
            input_str = self.rfile.read(length).decode("utf-8")
            self.path += "/" + input_str
        except Exception as e:
            self.path = None
        self.consensus_request()

    def consensus_request(self):
        # print("receive op")
        with self.server.queue_lock:
            print("adding path {}".format(self.path))
            self.server.pending_queue.put((self, self.path))
            self.server.queue_cond.notify()
        self.handler_lock = threading.Lock()
        self.handler_cond = threading.Condition(self.handler_lock)
        with self.handler_lock:
            self.handler_cond.wait()
        # print("@@@@@@@@@@@ alive again")
        out_str = self.handle_request(self.path)
        # print("out_str={}".format(out_str))
        self.write_result(out_str)

    # return out_str
    def handle_request(self, path):
        try:
            assert (path), "POST fail"
            request = path.split('/')
            request = [r for r in request if r != ""]
            if len(request) == 3:
                name, request, input_str = request
            elif len(request) == 2:
                name, request = request
                input_str = None
            else:
                assert (False), 'wrong input size'
            assert (name in ('kv', 'kvman')), 'wrong name'
            assert (request in ProjectHTTPRequestHandler.METHODS), 'no such method'
            ins = self.parse_input(input_str)
            # print("receive request: {} {}".format(request, input_str))
            out_dict = getattr(self, request + "_request")(ins)
            out_str = self.gen_output(out_dict)
        except Exception as e:
            print("exception {}".format(e))
            out_dict = {'success': False, 'debug_info': str(e)}
            out_str = self.gen_output(out_dict)
        return out_str

    def write_result(self, out_str):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(out_str.encode(encoding="utf_8"))


class KvpaxosHttpServer(ThreadingMixIn, HTTPServer):
    def __init__(self, server_id, paxos_peer, *args, **kargs):
        self.database = Database()
        self.server_id = server_id
        self.paxos_peer = paxos_peer
        self.pending_queue = queue.Queue()
        self.queue_lock = threading.Lock()
        self.queue_cond = threading.Condition(self.queue_lock)
        self.seq = 0
        threading.Thread(target=self.pending_handler).start()
        super(KvpaxosHttpServer, self).__init__(*args, **kargs)

    def pending_handler(self):
        while True:
            with self.queue_lock:
                while self.pending_queue.empty():
                    # print("no operation,sleep")
                    self.queue_cond.wait()
                handler, path = self.pending_queue.get()
            while True:
                self.paxos_peer.start(self.seq, (self.server_id, path))
                while True:
                    t = 0.01
                    status = self.paxos_peer.status(self.seq)
                    if status.decided:
                        break
                    time.sleep(t)
                    if (t < 10):
                        t *= 2
                res_server_id, res_path = status.value
                # print("######### decided value {}".format(res_path))

                # self.paxos_peer.done(self.seq)
                self.seq += 1
                if res_server_id == self.server_id:  # this server's operation
                    print("{} do desired job @{}".format(self.server_id, self.seq))
                    with handler.handler_lock:
                        handler.handler_cond.notify()
                    break
                else:
                    print("{} do other's job @{}".format(self.server_id, self.seq))
                    handler.handle_request(res_path)


if __name__ == "__main__":
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


    def op(k):
        print("request_sent")
        conn = http.client.HTTPConnection(server_str[k])
        conn.request(method="POST", url='/kv/insert', body="key=k&value=v&requestid=423")
        res = conn.getresponse()
        res_json = json.loads(res.read().decode('utf-8'))
        print(res_json)


    threading.Thread(target=op, args=(0,)).start()
    threading.Thread(target=op, args=(0,)).start()
    threading.Thread(target=op, args=(0,)).start()
    threading.Thread(target=op, args=(0,)).start()
    threading.Thread(target=op, args=(0,)).start()
    time.sleep(5)
    threading.Thread(target=op, args=(1,)).start()
