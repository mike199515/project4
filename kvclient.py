#!usr/bin/env python3
import http.client
import threading
import json
import time
import sys
import os
from read_write_lock import ReadWriteLock
from paxos_peer import PaxosPeer

cfg = json.load(open('conf/settings.conf'))

servers=[]
for k in cfg:
    if "n" in k:
        server_str = cfg[k] + ":" + cfg["port"]
        servers.append(server_str)


insert_url = "/kv/insert/key={0}&value={1}"
query_url = "/kv/get?key={0}"
update_url = "/kv/update/key={0}&value={1}"
delete_url = "/kv/delete/key={0}"
dump_url = "/kvman/dump"
count_url = "/kvman/countkey"
shutdown_url = "/kvman/shutdown"
restart_url = "/kvman/restart"


class KvClient:
    insert_statistic = []
    insert_lock = ReadWriteLock()
    get_statistic = []
    get_lock = ReadWriteLock()
    total_insert_num = 0
    suc_insert_num = 0
    result_flag = 'success'
    
    def same_dict(self,dict1,dict2):
        if len(dict1)!=len(dict2):return False
        for key,value in dict1.items():
            if key not in dict2:
                return False
            if dict2[key]!=value:
                return False
        return True
    
    def request(self, method_str, request_str, request_type=None, expect_dict=None):
        def func(request):
            if request_type == 'insert':
                self.total_insert_num += 1

            start = time.clock()

            server_id=0
            while True:
                try:
                    conn = http.client.HTTPConnection(servers[server_id])
                    if method_str == 'POST':
                        request_data = request_str.split('/')
                        request_data = [r for r in request_data if r != ""]
                        conn.request(method=method_str, url = '/' + request_data[0] + '/' + request_data[1], body = request_data[2])
                    else:
                        conn.request(method=method_str, url=request_str)
                except ConnectionRefusedError and ConnectionResetError:
                    server_id=(server_id+1)%len(servers)
                    print("[kvclient]fail, connecting next server")
            if len(sys.argv) == 1 and sys.argv[0] == '-d':
                print("request {}".format(request_str))
            res = conn.getresponse()
            res_json = json.loads(res.read().decode('utf-8'))
            if (request_type == 'delete' or request_type == 'get'):
                if res_json['success'] == 'false':
                    res_json['value'] = None
            if expect_dict is not None and not self.same_dict(expect_dict,res_json):
                print("failed at {}!={}".format(expect_dict, res_json))
                self.result_flag = 'fail'
            # maybe we should convert the value to a unicode string before output it
            if len(sys.argv) == 1 and sys.argv[0] == '-d':
                print("{}:{}".format(request_str, res_json))

            finish = time.clock()

            if request_type != 'get' and request_type != 'insert':
                return None
            if res_json['success'] != 'true':
                return None
            if request_type == 'insert':
                self.suc_insert_num += 1
            if request_type == 'get':
                mLock = self.get_lock
                mArray = self.get_statistic
            else:
                mLock = self.insert_lock
                mArray = self.insert_statistic
            mLock.acquire_write()
            mArray.append((finish - start) * 1000)
            mLock.release_write()

        t = threading.Thread(target=func, args=(request_str,))
        t.start()

    def analysis(self):
        if len(self.insert_statistic) == 0:
            self.insert_statistic.append(0)
        if len(self.get_statistic) == 0:
            self.get_statistic.append(0)
        print('Result: {0}'.format(self.result_flag))
        print('Insertion: {0}/{1}'.format(self.suc_insert_num, self.total_insert_num))
        print(
            'Averge latency: {0:.2f}ms/{1:.2f}ms'.format(sum(self.insert_statistic) / float(len(self.insert_statistic)),
                                                         sum(self.get_statistic) / float(len(self.get_statistic))))
        self.insert_statistic.sort()
        self.get_statistic.sort()

        print('Percentile latency: {0:.2f}ms/{1:.2f}ms, {2:.2f}ms/{3:.2f}ms, {4:.2f}ms/{5:.2f}ms, {6:.2f}ms/{7:.2f}ms'
              .format(self.insert_statistic[int(len(self.insert_statistic) * 0.2)],
                      self.get_statistic[int(len(self.get_statistic) * 0.2)],
                      self.insert_statistic[int(len(self.insert_statistic) * 0.5)],
                      self.get_statistic[int(len(self.get_statistic) * 0.5)],
                      self.insert_statistic[int(len(self.insert_statistic) * 0.7)],
                      self.get_statistic[int(len(self.get_statistic) * 0.7)],
                      self.insert_statistic[int(len(self.insert_statistic) * 0.9)],
                      self.get_statistic[int(len(self.get_statistic) * 0.9)]))

    def basic_func_test(self):
        time.sleep(1)
        key = "basic_func" + "hello"
        value = "basic_func" + "world"
        self.request("POST",insert_url.format(key,value),'insert',{'success':'true'})
        time.sleep(0.1)
        self.request("POST",insert_url.format(key,value),'insert',{'success':'false'})
        time.sleep(0.1)
        self.request("POST",update_url.format(key,value + '!'),'update',{'success':'true'})
        time.sleep(0.1)
        self.request("POST",update_url.format(value + '!', key),'update',{'success':'false'})
        time.sleep(0.1)
        self.request("GET",query_url.format(key),'get',{'success':'true', 'value':value+'!'})
        time.sleep(0.1)
        self.request("GET",query_url.format(value),'get',{'success':'false', 'value':None})
        time.sleep(0.1)
        self.request("POST",delete_url.format(key),'delete',{'success':'true', 'value':value+'!'})
        time.sleep(0.1)
        self.request("POST",delete_url.format(key),'delete',{'success':'false', 'value':None})
        time.sleep(5)
        
        self.request("GET",count_url,'count',{'result':'0'})
        time.sleep(3)
        self.request("POST",insert_url.format(key,value),'insert',{'success':'true'})
        self.request("POST",insert_url.format(key + '2',value + '2'),'insert',{'success':'true'})
        self.request("POST",insert_url.format(key + '3',value + '3'),'insert',{'success':'true'})
        time.sleep(3)
        self.request("GET",count_url,'count',{'result':'3'})
        #self.request("GET",dump_url,'count',{key:value, key+'2':value+'2', key+'3':value+'3'})
        #self.bak_request("GET",dump_url,'count',{key:value, key+'2':value+'2', key+'3':value+'3'})
        time.sleep(10)




    def multiple_key_test(self):
        keys = ["multiple_" + str(i) for i in range(100)]
        # os.system("bin//start_server -p")
        # os.system("bin//start_server -b")
        time.sleep(1)
        for key in keys:
            self.request("POST", insert_url.format(key, key), 'insert')
        time.sleep(1)
        for key in keys:
            self.request("GET", query_url.format(key), 'get')
        time.sleep(1)
        for key in keys:
            self.request("POST", update_url.format(key, key + key))
        time.sleep(1)
        for key in keys:
            self.request("GET", query_url.format(key), 'get')
        time.sleep(1)
        for key in keys:
            self.request("POST", delete_url.format(key), 'delete')
        time.sleep(1)

        # os.system("bin//stop_server -p")
        # os.system("bin//stop_server -b")
        # request("GET","/kvman/countkey")
        # request("GET","/kvman/dump")

    def single_key_pressure_test(self):
        time.sleep(1)
        key = "single_pressure"
        value = "init_val"
        iteration_time = 1000
        self.request("POST", insert_url.format(key, value), 'insert')

        for i in range(iteration_time):
            time.sleep(0.02)
            self.request("POST", update_url.format(key, str(i)), 'update')
            time.sleep(0.02)
            self.request("GET", query_url.format(key), 'get', expect_dict={'success':'true','value':str(i)});
        time.sleep(2)

    def key_delete_test(self):
        time.sleep(1)
        key = 'delete_test'
        value = "delete_val"
        iteration_time = 200

        for i in range(iteration_time):
            time.sleep(0.02)
            self.request("POST", insert_url.format(key, value), 'insert')
            time.sleep(0.02)
            self.request("POST", delete_url.format(key), 'delete')
        time.sleep(2)


a = KvClient()
a.basic_func_test()
a.key_delete_test()
a.multiple_key_test()
a.single_key_pressure_test()
a.analysis()
