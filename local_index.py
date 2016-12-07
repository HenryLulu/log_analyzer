from pymongo import *
import re
import socket
import os
import time
import threading
#test deployer
mongo_addr = "mongodb://183.250.179.150:27017,117.145.178.217:27017,117.145.178.218:27017"
import socket
import fcntl
import struct
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_ip =  socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915, # SIOCGIFADDR
        struct.pack('256s', 'eth5'[:15])
    )[20:24])
except:
    ips = os.popen("LANG=C ifconfig | grep \"inet addr\" | grep -v \"127.0.0.1\" | awk -F \":\" '{print $2}' | awk '{print $1}'").readlines()
    if len(ips) > 0:
        server_ip = ips[0]

def ifjam(u):
    seg_mode_time = 4 if u["seg_t"] else 10
    return (u["end"]-u["start"]-(u["seg_e"]-u["seg_s"])*seg_mode_time) > seg_mode_time
def calculate(file):
    req_re = re.compile(r"^http://(\w+)\..+(\d)_/seg(\d).+(\d{9})")
    live_re = re.compile(r"^http://(\w+)\..+/live/(flv|ld/trans)/")
    logs = open("/data/proclog/log/pzs/back/"+file,'r').readlines()
    log_list = []
    live_list = []
    for l in logs:
        try:
            agent = l.split('"')[1].decode("utf-8",'ignore')
        except:
            continue
        x_group = l.split(" ")
        if len(x_group)<13:
            continue
        ip = x_group[1]
        tim = int(x_group[0][0:10])
        status = x_group[6]=="200" or x_group[6]=="206" or x_group[6]=="304"
        flu = int(x_group[7])

        req_ma = req_re.match(x_group[11])
        if req_ma:
            channel = req_ma.group(1)
            rate = str(int(req_ma.group(2))%5)
            seg = req_ma.group(3)==u"1"
            segnum = int(req_ma.group(4))
            r = (ip+agent,tim,status,channel,rate,seg,segnum,ip,agent,flu)
            log_list.append(r)
        elif live_re.match(x_group[11]):
            channel = live_re.match(x_group[11]).group(1)
            rate = x_group[3]
            try:
                live_jam = int(x_group[2])>0
            except:
                live_jam = False
            r = (ip+agent,tim,status,channel,rate,"",live_jam,ip,agent,flu)
            live_list.append(r)

    user_list = {}
    channel_list = {}
    rate_list = {}
    suc_n = 0
    jam_n = 0
    flu_total = 0

    for l in log_list:
        if user_list.has_key(l[0]):
            user_list[l[0]]["end"] = l[1]
            user_list[l[0]]["seg_e"] = l[6]
            user_list[l[0]]["req_n"] += 1
            if l[2]:
                user_list[l[0]]["suc_n"] += 1
        else:
            user_list[l[0]] = {
                "u_ip":l[7],
                "req_n":1,
                "suc_n":1 if l[2] else 0,
                "start":l[1],
                "end":l[1],
                "seg_t":l[5],
                "seg_s":l[6],
                "seg_e":l[6],
                "agent":l[8]
            }

        if channel_list.has_key(l[3]):
            channel_list[l[3]] += 1
        else:
            channel_list[l[3]] = 1
        if rate_list.has_key(l[4]):
            rate_list[l[4]] += 1
        else:
            rate_list[l[4]] = 1
        if l[2]:
            suc_n += 1
        flu_total += l[9]

    for u in user_list:
        jam = ifjam(user_list[u])
        user_list[u]["jam"] = jam
        if jam:
            jam_n += 1
        user_list[u]["s_ip"] = server_ip
        del user_list[u]["seg_t"]
        del user_list[u]["seg_s"]
        del user_list[u]["seg_e"]

    for l in live_list:
        if user_list.has_key(l[0]):
            user_list[l[0]]["req_n"] += 1
            if l[2]:
                user_list[l[0]]["suc_n"] += 1
        else:
            user_list[l[0]] = {
                "u_ip":l[7],
                "req_n":1,
                "suc_n":1 if l[2] else 0,
                "start":l[1],
                "end":l[1],
                "agent":l[8],
                "jam": l[6],
                "s_ip": server_ip
            }

        if channel_list.has_key(l[3]):
            channel_list[l[3]] += 1
        else:
            channel_list[l[3]] = 1
        if rate_list.has_key(l[4]):
            rate_list[l[4]] += 1
        else:
            rate_list[l[4]] = 1
        if l[2]:
            suc_n += 1
        flu_total += l[9]

    try:
        client = MongoClient(mongo_addr)
        db = client.log_db
        log_table = db.log_table
        user_table = db.user_table

        ins_user_res = user_table.insert_many(user_list.values())
        log_info = {
            "s_ip":server_ip,
            "start":file[7:21],
            "req_n":len(log_list)+len(live_list),
            "suc_n":suc_n,
            "user_n":len(user_list),
            "jam_n":jam_n,
            "flu":flu_total,
            "users":ins_user_res.inserted_ids,
            "rate_n":rate_list,
            "channal_n":channel_list
        }

        log_table.insert_one(log_info)
        print "Info:Complete"
    except Exception,e:
        print Exception,":",e
        print "Error:Unable to write to Mongo"

def n_thread(file):
    print file
    try:
        calculate(file)
    except Exception,e:
        print Exception,":",e

def monitor():
    dir = "/data/proclog/log/pzs/back"
    origin = set([_f[2] for _f in os.walk(dir)][0])
    while True:
        time.sleep(5)
        final = set([_f[2] for _f in os.walk(dir)][0])
        dif = final.difference(origin)
        origin = final
        while len(dif) > 0:     #change to while
            try:
                file = dif.pop()
                if re.compile(r"^access_.+log$").match(file):
                    t = threading.Thread(target = n_thread, args = (file,))
                    t.start()
                    t.join()
            except:
                print "Error:Unable to create new thread"

def main():
    print "start..."+server_ip
    try:
        monitor()
    except:
        print "Error:Init fail"

main()

