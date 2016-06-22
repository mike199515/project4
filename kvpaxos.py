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
import re
import random
from IPython import embed


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
        # trick to avoid crash
        if "requestid" in ret:
            ret.pop("requestid")
        return ret

    @staticmethod
    def gen_output(output_dict):
        ret = json.dumps(output_dict)
        # print("output:{}".format(ret))
        return ret

    def countkey_request(self, command, ins):
        assert (command == "GET"), "wrong HTTP method"
        keycount = self.server.database.countkey()
        outs = {'result': str(keycount)}
        return outs

    def dump_request(self, command, ins):
        assert (command == "GET"), "wrong HTTP method"
        outs = self.server.database.dump()
        return outs

    def shutdown_request(self, ins):
        os.system('bin/stop_server -b')

    def serialize_request(self, command, ins):
        # we need to verify it is the other server that calls us
        assert (command == "GET"), "wrong HTTP method"
        data_str = self.server.database.serialize()
        outs = {'data': data_str}
        return outs

    def insert_request(self, command, ins):
        assert (command == "POST"), 'wrong HTTP method'
        assert (len(ins) == 2 and 'key' in ins and 'value' in ins), 'wrong input'
        key, value = ins['key'], ins['value']
        success = self.server.database.insert(key, value)
        outs = {'success': success}
        return outs

    def delete_request(self, command, ins):
        assert (command == "POST"), 'wrong HTTP method'
        assert (len(ins) == 1 and 'key' in ins), 'wrong input'
        key = ins['key']
        value = self.server.database.delete(key)
        if value:
            outs = {'success': True}
        else:
            outs = {'success': False}
        return outs

    def update_request(self, command, ins):
        assert (command == "POST"), 'wrong HTTP method'
        assert (len(ins) == 2 and 'key' in ins and 'value' in ins), 'wrong input'
        key, value = ins['key'], ins['value']
        success = self.server.database.update(key, value)
        outs = {'success': success}
        return outs

    def get_request(self, command, ins):
        assert (command == "GET"), 'wrong HTTP method'
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

    def _get_key(self, path):
        keys = re.findall(r"(?<=key=).*?(?=&)", path)
        if not keys:
            keys.append("")
        return keys[0]

    def consensus_request(self):
        # print("receive op")
        with self.server.queue_lock:
            print("adding path {}".format(self.path))
            key = self._get_key(self.path)
            self.server.pending_queue.setdefault(key, queue.Queue()).put((self, (self.command, self.path)))
            self.server.queue_cond.notify()
        self.handler_lock = threading.Lock()
        self.handler_cond = threading.Condition(self.handler_lock)
        with self.handler_lock:
            self.handler_cond.wait()
            # print("@@@@@@@@@@@ alive again")

    # return out_str
    def handle_request(self, command, path):
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
            out_dict = getattr(self, request + "_request")(command, ins)
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
        self.pending_queue = dict()
        self.queue_lock = threading.Lock()
        self.queue_cond = threading.Condition(self.queue_lock)
        self.seq_lock=threading.Lock()
        self.seq_cond=threading.Condition(self.seq_lock)
        self.max_seq=0
        self.working_seq = set()
        self.MAX_WORKER=5
        threading.Thread(target=self.pending_handler).start()
        super(KvpaxosHttpServer, self).__init__(*args, **kargs)

    def worker(self,is_self_server,my_seq , handler, command_path, nr_remain, lock, cond):
        def run():
            if is_self_server:  # this server's operation
                print("@@@@@@@@@@@@@ [{}]do desired job @{}, cpath={}".format(self.server_id, my_seq, command_path))
                out_str = handler.handle_request(*command_path)
                handler.write_result(out_str)
                with handler.handler_lock:
                    handler.handler_cond.notify()
            else:
                print("@@@@@@@@@@@@@ [{}]do other's job @{},cpath={}".format(self.server_id, my_seq, command_path))
                handler.handle_request(*command_path)
            with lock:
                nr_remain[0] -= 1
                if nr_remain[0] == 0:
                    cond.notify()

        threading.Thread(target=run).start()

    def pending_handler(self):
        while True:
            with self.queue_lock:
                while not self.pending_queue:
                    # print("no operation,sleep")
                    self.queue_cond.wait()
                pending_command_paths = dict()
                pending_handlers = dict()
                if "" in self.pending_queue:
                    pending_handlers[""], pending_command_paths[""] = self.pending_queue[""].get()
                else:
                    for key in self.pending_queue:
                        pending_handlers[key], pending_command_paths[key] = self.pending_queue[key].get()
                self.pending_queue = {k: v for k, v in self.pending_queue.items() if not v.empty()}
                with self.seq_lock:
                    while True:
                        if len(self.working_seq)<self.MAX_WORKER:
                            print("***** generate new worker")
                            threading.Thread(target=self.pending_worker,args=(pending_handlers,pending_command_paths)).start()
                            break
                        else:
                            print("***** wait")
                            self.seq_cond.wait()

    def pending_worker(self,pending_handlers, pending_command_paths):
        while True:
            with self.seq_lock:
                if self.working_seq:
                    my_seq=max(self.working_seq)+1
                else:
                    my_seq=self.max_seq+1
                self.max_seq=my_seq
                print("### [{}]assign seq number is {}".format(self.server_id,my_seq))
                assert(my_seq not in self.working_seq)
                self.working_seq.add(my_seq)
            print("[{}]start paxos @ {}".format(self.server_id,my_seq))
            self.paxos_peer.start(my_seq, (self.server_id, pending_command_paths))
            while True:
                t = 0.01
                status = self.paxos_peer.status(my_seq)
                if status.decided:
                    break
                time.sleep(t)
                if (t < 10):
                    t *= 2
            res_server_id, res_command_paths = status.value

            nr_remain = [len(res_command_paths)]
            lock = threading.Lock()
            cond = threading.Condition(lock)
            print("#### [{}]consensus {} done".format(self.server_id,my_seq))
            #start to wait until it is head
            with self.seq_lock:
                while True:
                    if my_seq!=min(self.working_seq):
                        self.seq_cond.wait()
                    else:
                        break
            print("#### [{}]start doing seq {}".format(self.server_id,my_seq))
            with lock:
                if res_server_id == self.server_id:
                    for key in res_command_paths:
                        self.worker(True,my_seq, pending_handlers[key], res_command_paths[key], nr_remain, lock, cond)
                else:
                    handler = next(iter(pending_handlers.values()))  # any is fine
                    for key in res_command_paths:
                        self.worker(False,my_seq, handler, res_command_paths[key], nr_remain, lock, cond)
                # print("sleep for a while")
                cond.wait()
            with self.seq_lock:
                assert(my_seq==min(self.working_seq)),"not the smallest"
                if (my_seq+1)%100==0:
                    self.paxos_peer.done(my_seq)
                self.working_seq.remove(my_seq)
                self.seq_cond.notify_all()
            if res_server_id == self.server_id:
                break
            print("$$$$$$$$$$$$$$$$$$$$$$$$ [{}]FAIL @{}, reassign$$$$$$$$$$$$$$$$".format(self.server_id,my_seq))


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
    for i in range(100):
        threading.Thread(target=op, args=(0, 0)).start()
        threading.Thread(target=op, args=(1, 0)).start()
        threading.Thread(target=op, args=(2, 0)).start()