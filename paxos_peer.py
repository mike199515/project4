from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
import threading
import http
import json
import time
from urllib.parse import urlparse


class ThreadingHttpServer(ThreadingMixIn, HTTPServer):
    def __init__(self, peer, *args, **kargs):
        self.peer = peer
        super(ThreadingHttpServer, self).__init__(*args, **kargs)


class AcceptorState:
    def __init__(self):
        self.n_p = -1
        self.n_a = -1
        self.v_a = None


class PaxosState:
    def __init__(self):
        self.k = 0
        self.decided = False
        self.value = None

    def __str__(self):
        return "k:{} decided:{},value:{}".format(self.k, self.decided, self.value)


class PaxosRequestHandler(BaseHTTPRequestHandler):
    if __name__ == "__main__" or True:
       def log_message(self, format, *args):
            return

    def do_GET(self):
        pass

    def do_POST(self):
        self.state = self.server.peer.acceptor_state
        self.paxos_state = self.server.peer.paxos_state
        self.lock = self.server.peer.paxos_state_lock
        length = int(self.headers["Content-Length"])
        msg_str = self.rfile.read(length).decode("utf-8")
        method, args = json.loads(msg_str)
        getattr(self, method)(*args)

    def _reply(self, out):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        out_str = json.dumps(out)
        self.wfile.write(out_str.encode(encoding="utf_8"))
        #print("{} reply {}".format(self.server.peer.me,out))

    def prepare(self, seq, n):
        if seq < self.server.peer.min():
            return
        with self.lock:
            if seq not in self.state:
                self.state[seq] = AcceptorState()
            state = self.state[seq]
        if n > state.n_p:
            state.n_p = n
            self._reply(("prepare_ok", (state.n_a, state.v_a)))
        else:
            # print("reject prepare n_p={},n={}, v={}".format(state.n_p,n,state.v_a))
            self._reply(("prepare_reject", (state.n_p)))

    def accept(self, seq, n, v):
        if seq < self.server.peer.min():
            return
        with self.lock:
            if seq not in self.state:
                self.state[seq] = AcceptorState()
            state = self.state[seq]
        if n >= state.n_p:
            state.n_p = n
            state.n_a = n
            state.v_a = v
            self._reply(("accept_ok", n))
        else:
            self._reply(("accept_reject", (state.n_p)))

    def decided(self, seq, v):
        with self.lock:
            if seq not in self.paxos_state:
                self.paxos_state[seq] = PaxosState()
            self.paxos_state[seq].decided = True
            self.paxos_state[seq].value = v
        self._reply("decided_ok");

    def done_val(self):
        self._reply(self.server.peer.done_val)

class PaxosPeer:
    def parse_url(self, url):
        #print(url.split(":"))
        server, port = url.split(":")
        port = int(port)
        return server, port

    def __init__(self, peers, me):
        self.peers = peers
        self.me = me
        self.my_url = self.parse_url(self.peers[self.me])
        self.paxos_state = dict()
        self.acceptor_state = dict()
        self.paxos_state_lock = threading.Lock()
        self.dead = False
        self.done_val = -1
        self.server = None

        def run_server():
            self.server = ThreadingHttpServer(self, self.my_url, PaxosRequestHandler)
            self.server.serve_forever()

        m_thread = threading.Thread(target=run_server)
        m_thread.setDaemon(True)
        m_thread.start()

        if  __name__ != "__main__":
            print("acceptor server start at {}".format(self.my_url))

    def _send(self, msg, url):
        #print("send {} to {}".format(msg, url))
        flag = False
        fail_time = 0
        while fail_time < 5:
            conn = http.client.HTTPConnection(url)
            try:
                conn.request(method="POST", url="", body=json.dumps(msg))
                res = conn.getresponse()
                ret = json.loads(res.read().decode('utf-8'))
                return ret
            except ConnectionRefusedError:
                fail_time = fail_time + 1
            except:
                if __name__ == "__main__":
                    pass
                else:
                    raise
        return ("connection fail", ())

    def _send_all(self, msg, require_all=False):
        res = []
        lock = threading.Lock()
        cond = threading.Condition(lock)
        nr_finished = [0]

        def run_one(msg, url, nr_finished, require_all):
            ret = self._send(msg, url)
            with lock:
                if require_all:
                    res.append(ret)
                    nr_finished[0] += 1
                    if nr_finished[0] >= len(self.peers):
                        cond.notify()
                    return

                if not nr_finished[0] * 2 > len(self.peers):
                    res.append(ret)
                else:
                    return
                nr_finished[0] += 1
                if nr_finished[0] * 2 > len(self.peers):
                    cond.notify()

        with lock:
            for url in self.peers:
                threading.Thread(target=run_one, args=(msg, url, nr_finished, require_all)).start()
            cond.wait()
        return res

    def _majority(self, result, expect_reply):
        count = 0
        for r in result:
            if r[0] == expect_reply:
                count += 1
        return count * 2 > len(self.peers)

    def _propose(self, seq, v):
        print("start propose {},{}".format(seq, v))
        with self.paxos_state_lock:
            if seq not in self.paxos_state:
                self.paxos_state[seq] = PaxosState()
            state = self.paxos_state[seq]
        while not state.decided and seq >= self.min():
            if self.dead: break
            n = state.k * len(self.peers) + self.me
            res = self._send_all(("prepare", (seq, n)))
            max_seen=n
            for r in res:
                if r[0]=="prepare_reject" and r[1]>max_seen:
                    max_seen=r[1]

            if self._majority(res, "prepare_ok"):
                n_a = 0
                vp = v
                for r in res:
                    if r[0] == "prepare_ok" and r[1][0] > n_a:
                        n_a = r[1][0]
                        vp = r[1][1]
                accept_res = self._send_all(("accept", (seq, n, vp)))
                if self._majority(accept_res, "accept_ok"):
                    self._send_all(("decided", (seq, vp)))
                else:
                   for r in accept_res:
                       if r[0]=="accept_reject" and r[1]>max_seen:
                           max_seen=r[1]
                   state.k = max_seen // len(self.peers) + 1  # bigger than any one seen
            else:
                assert (n // len(self.peers) == state.k)
                state.k = max_seen // len(self.peers) + 1  # bigger than any one seen

    # start aggreement on new instance
    def start(self, seq, v):
        if(self.dead):
            return
        def run():
            self._propose(seq, v)

        if seq < self.min():
            print("invalid proposal")
            return
        threading.Thread(target=run).start()

    # get info about an instance
    def status(self, seq):
        if(self.dead):
            return None
        with self.paxos_state_lock:
            if seq in self.paxos_state:
                return self.paxos_state[seq]
            else:
                return PaxosState()

    # ok to forget all instance <=seq
    def done(self, seq):
        if self.dead:
            return
        def run():
            self.done_val = seq
            res = self._send_all(("done_val", ()), require_all=True)
            actual_val = min(res)
            print("in done {}, receive {}, decide {}".format(seq, res, actual_val))
            with self.paxos_state_lock:
                seqs = [k for k in self.paxos_state if k <= actual_val]
                for s in seqs:
                    self.paxos_state.pop(s)
                    self.acceptor_state.pop(s)

        threading.Thread(target=run).start()

    # highest instance seq known or -1
    def max(self):
        if self.dead:
            return -1
        with self.paxos_state_lock:
            if (len(self.paxos_state) == 0):
                return -1
            return max(self.paxos_state)

    # instances before this have been forgotten or -1
    def min(self):
        if self.dead:
            return -1
        with self.paxos_state_lock:
            if (len(self.paxos_state) == 0):
                return -1
            return min(self.paxos_state)

    def kill(self):
        if not self.dead:
            self.dead = True
            self.server.socket.close()
            self.server.server_close()

class PaxosTest:
    def __init__(self):
        self.peer = None
        self.px = None
        self.peer_num = 0
        self.success = True
        self.test_name = "Default Test Name"

    def init_peer_set(self, name, peer_num):
        self.test_name = name
        print("start " + name)
        self.success = True
        self.peer_num = peer_num
        self.peers = ["localhost:{0}".format(8000 + i) for i in range(peer_num)]
        self.px = [PaxosPeer(self.peers, i) for i in range(peer_num)]

    def start(self, peer_id, seq_id, value):
        self.px[peer_id].start(seq_id, value)

    def wait_status(self, peer_id, seq_id):
        while self.px[peer_id].status(seq_id).decided == False:
            pass
        return self.px[peer_id].status(seq_id)

    def kill(self, peer_id):
        self.px[peer_id].kill()

    def print_status(self, seq_id, status):
        print("seq_id: {} decided: {} value: {}".format(seq_id, status.decided, status.value))

    def wait_and_print(self, peer_id, seq_id):
        self.print_status(seq_id, self.wait_status(peer_id, seq_id))

    def wait_until_timeout(self, peer_id, seq_id):
        start = time.time()
        while self.px[peer_id].status(seq_id).decided == False:
            if time.time() - start > 5:
                break
        return self.px[peer_id].status(seq_id)

    def finalize(self):
        if(self.success):
            print(self.test_name + " succeeded")
        else:
            print(self.test_name + " failed")
        for i in range(self.peer_num):
            self.kill(i)

    def assert_status(self, peer_id, seq_id, decided, value = None):
        status = self.px[peer_id].status(seq_id)
        flag = True
        if status.decided == False:
            flag = (status.decided == decided)
        else:
            flag = (status.decided == decided and status.value == value)
        if not flag:
            self.success = False

    def test(self):
        self.basic_test()
        time.sleep(10)
        self.majority_test()
        time.sleep(10)
        self.done_test()
        print("All tests completed!")

    def basic_test(self):
        self.init_peer_set("Basic Test", 5)

        def vote(peer_id, value):
            seq = 0
            while(True):
                self.start(peer_id, seq, [peer_id, value])
                status = self.wait_status(peer_id, seq)
                if(status.value[0] == peer_id):
                    break
                seq = seq + 1
        threading.Thread(target=vote(0, 'hhh')).start()
        threading.Thread(target=vote(1, 'lol')).start()
        status1 = self.wait_status(0, 0)
        self.assert_status(1, 0, True, status1.value)
        status2 = self.wait_status(1, 1)
        self.assert_status(0, 1, True, status2.value)
        if(status1.value[0] == status2.value[0]):
            self.success = False
        self.finalize()

    def majority_test(self):
        self.init_peer_set("Majority Test", 3)
        self.start(0, 0, 0)
        self.wait_and_print(0, 0)
        self.assert_status(0, 0, True, 0)
        self.kill(2)
        self.start(0, 1, 1)
        self.wait_and_print(0, 1)
        self.assert_status(0, 1, True, 1)
        self.kill(1)
        self.start(0, 2, 2)
        self.wait_until_timeout(0, 2)
        self.assert_status(0, 2, False)
        self.finalize()

    def done_test(self):
        self.init_peer_set("Done Test", 5)
        self.start(0, 0, "how")
        self.start(0, 1, "are")
        self.finalize()

    #for k in range(20):
    #    px[0].start(k, k * k)
    #time.sleep(5)
    #px[0].done(17)
    #px[1].done(17)
    #px[2].done(17)
    #px[0].done(18)
    #px[1].done(18)
    #time.sleep(5)
    #for k in range(20):
    #    for i in range(3):
    #        if (k < px[i].min()):
    #            continue
    #        print("{} status[{}]:{}".format(k, i, px[i].status(k)))

if __name__ == "__main__":
    PaxosTest().test()