from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
import threading
import http
import json
from urllib.parse import urlparse

class ThreadingHttpServer(ThreadingMixIn, HTTPServer):
    def __init__(self,peer, *args, **kargs):
        self.peer=peer
        super(ThreadingHttpServer,self).__init__(*args,**kargs)



class AcceptorState:
    def __init__(self):
        self.n_p = -1
        self.n_a = -1
        self.v_a = None


class PaxosState:
    def __init__(self):
        self.decided = False
        self.value = None
    def __str__(self):
        return "decided:{},value:{}".format(self.decided,self.value)

class PaxosRequestHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        pass

    def do_POST(self):
        self.state=self.server.peer.acceptor_state
        self.paxos_state=self.server.peer.paxos_state
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
        print("{} reply {}".format(self.server.peer.me,out))
    def prepare(self, n):
        if n > self.state.n_p:
            self.state.n_p = n
            self._reply(("prepare_ok", (self.state.n_a, self.state.v_a)))
        else:
            self._reply(("prepare_reject", ()))

    def accept(self, n, v):
        if n >= self.state.n_p:
            self.state.n_p = n
            self.state.n_a = n
            self.state.v_a = v
            self._reply(("accept_ok", n))
        else:
            self._reply(("accept_reject", ()))
    def decided(self, v):
        self.paxos_state.decided=True
        self.paxos_state.value=v
        self._reply("decided_ok");

class PaxosPeer:
    def parse_url(self,url):
        print(url.split(":"))
        server,port=url.split(":")
        port=int(port)
        return server,port

    def __init__(self, peers, me):
        self.peers = peers
        self.me = me
        self.k = 0
        self.my_url = self.parse_url(self.peers[self.me])
        self.paxos_state = PaxosState()
        self.acceptor_state = AcceptorState()

        def run_server():
            server = ThreadingHttpServer(self,self.my_url, PaxosRequestHandler)
            server.serve_forever()

        threading.Thread(target=run_server).start()
        print("acceptor server start at {}".format(self.my_url))

    def _send(self, msg, url):
        print("send {} to {}".format(msg, url))
        conn = http.client.HTTPConnection(url)
        try:
            conn.request(method="POST", url="", body=json.dumps(msg))
            res = conn.getresponse()
            ret = json.loads(res.read().decode('utf-8'))
            return ret
        except ConnectionRefusedError:
            return ("connection fail",())


    def _send_all(self, msg):
        res = []
        lock=threading.Lock()
        cond=threading.Condition(lock)
        nr_finished=[0]
        def run_one(msg,url,nr_finished):
            ret=self._send(msg, url)
            with lock:
                if not nr_finished[0]*2>len(self.peers):
                    print("done")
                    res.append(ret)
                else:
                    return
                nr_finished[0]+=1
                if nr_finished[0]*2>len(self.peers):
                    print("notify")
                    cond.notify()
        with lock:
            for url in self.peers:
                threading.Thread(target=run_one,args=(msg,url,nr_finished)).start()
            print("begin wait")
            cond.wait()
        print(res)
        return res
    def _majority(self,result,expect_reply):
        count = 0
        for r in result:
            if r[0] == expect_reply:
                count+=1
        return count*2>len(self.peers)

    def _propose(self, v):
        print("start propose")
        while not self.paxos_state.decided:
            n = self.k * len(self.peers) + self.me
            self.k += 1
            res = self._send_all(("prepare", (n,)))
            if self._majority(res,"prepare_ok"):
                n_a=0
                vp=v
                for r in res:
                    if r[0]=="prepare_ok" and r[1][0]>n_a:
                        n_a=r[1][0]
                        vp=r[1][1]
                accept_res=self._send_all(("accept",(n,vp)))
                if self._majority(accept_res,"accept_ok"):
                    self._send_all(("decided", vp))

    # start aggreement on new instance
    def start(self, seq, v):
        def run():
            self._propose(v)
        threading.Thread(target=run).start()


    # get info about an instance
    def status(self, seq):
        return self.paxos_state


    def done(self, seq):
        pass

    # ok to forget all instance <=seq
    def max(self):
        pass

    # highest instnace seq known or -1
    def min(self):
        pass
        # instances before this have been forgotten


if __name__ == "__main__":
    peers = ["localhost:8000", "localhost:8001", "localhost:8002"]
    px = [PaxosPeer(peers, i) for i in range(3)]
    px[0]._propose("x")
    for i in range(3):
        print("status[{}]:{}".format(i,px[i].status(0)))