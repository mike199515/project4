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

cfg = json.load(open('conf/settings.conf'))

class ProjectHTTPRequestHandler(BaseHTTPRequestHandler):
    METHODS = {'insert', 'delete', 'get', 'update', 'serialize', 'countkey', 'dump', 'shutdown', 'restart'}
    BOOL_MAP = {True: 'true', False: 'false'}

    def __init__(self, *args, **kargs):
        super(BaseHTTPRequestHandler, self).__init__(*args, **kargs)

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
        if isinstance(output_dict, dict):
            for k, v in output_dict.items():
                if v in ProjectHTTPRequestHandler.BOOL_MAP:
                    output_dict[k] = ProjectHTTPRequestHandler.BOOL_MAP[v]
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

    def shutdown_request(self, command, ins):
        self.server.running = False
        #os.system('bin/stop_server -b')

    def restart_request(self, command, ins):
        self.server.running = True

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
            outs = {'success': True, 'value': value}
        else:
            outs = {'success': False, 'value': ""}
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

    def check_kvman(self, path):
        request = path.split('/')
        request = [r for r in request if r != ""]
        if len(request) == 3:
            name, request, input_str = request
        elif len(request) == 2:
            name, request = request
        else:
            return False
        return name =='kvman'

    def do_GET(self):
        if not self.server.running and self.path != '/kvman/restart':
            return
        if "?" in self.path:
            self.path = self.path.replace("?", "/?", 1)
        if not self.check_kvman(self.path):
            self.consensus_request()
        else:
            print("kvman request")
            self.write_result(self.handle_request(self.command, self.path))

    def do_POST(self):
        if not self.server.running:
            return
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
        self.MAX_WORKER=10
        self.running = True
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
                        time.sleep(0.001)

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
    try:
        if len(sys.argv) != 2:
            raise ValueError
        node_id = int(sys.argv[1])
    except ValueError:
        print ("Usage: python3 kvpaxos.py node_id")
        exit()
    peers = []
    #peers = ['127.0.0.1:10000','127.0.0.1:10001','127.0.0.1:10002']
    #mapping_id = node_id - 1

    for key in cfg:
        if 'n' in key:
            peers.append(cfg[key] + ":" + str(9999 + int(key[-2:])))
        if key == "n{0:02d}".format(node_id):
            mapping_id = len(peers) - 1
    print(peers)
    print(mapping_id)
    server_str = cfg["n{0:02d}".format(node_id)] + ":" + str(int(cfg["port"]) + node_id)
    server_tup = server_str.split(":")
    server_tup = (server_tup[0], int(server_tup[1]))
    px = PaxosPeer(peers, mapping_id)
    server = KvpaxosHttpServer(mapping_id, px, server_tup, ProjectHTTPRequestHandler)
    print("Node {} started at {}".format(node_id, server_str))
    server.serve_forever()
