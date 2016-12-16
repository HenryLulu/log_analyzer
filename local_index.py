log_type = 1
mongo_addr = "mongodb://n0.g1.pzt.powzamedia.com:27017,n1.g1.pzt.powzamedia.com:27017,n2.g1.pzt.powzamedia.com:27017"
if log_type ==1:
    log_dir = "/data/proclog/log/pzs/back"
else:
    log_dir = "/home/fivemin/logback"

from pymongo import *
import re
import os
import time
import threading
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
    else:
        server_ip = "unknow"
server_ip = server_ip.replace("\n","")

def ifjam(u):
    seg_mode_time = 4 if u["seg_t"] else 10
    return (u["end"]-u["start"]-(u["seg_e"]-u["seg_s"])*seg_mode_time) > seg_mode_time
def calculate(file):
    req_re = re.compile(r"^http://(\w+)\..+(\d)_/seg(\d).+(\d{9})")
    live_re = re.compile(r"^http://(\w+)\..+/live/(flv|ld/trans)/")
    long_rate_re = re.compile(r'^(\d+)_(\d+)\|(\d+)_(\d+)\|(\d+)_(\d+)\|(\d+)_(\d+)$')
    logs = open(log_dir+"/"+file,'r').readlines()
    log_list = []
    live_list = []

    #format log lines(normal CDN)
    if log_type==1:
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

    #format log lines(Dilian CDN)
    else:
        for l in logs:
            x_group = l.split("\t")
            # try:
            #     agent = l.split('"')[1].decode("utf-8",'ignore')
            # except:
            #     continue
            # x_group = l.split(" ")
            if len(x_group)<36:
                continue
            try:
                agent = x_group[16].decode("utf-8",'ignore')
            except:
                continue
            ip = x_group[0]
            tim = int(x_group[1][0:10])
            status = x_group[4]=="200" or x_group[4]=="206" or x_group[4]=="304"
            flu = int(x_group[6])

            req_ma = req_re.match(x_group[8])
            if req_ma:
                channel = req_ma.group(1)
                rate = str(int(req_ma.group(2))%5)
                seg = req_ma.group(3)==u"1"
                segnum = int(req_ma.group(4))
                r = (ip+agent,tim,status,channel,rate,seg,segnum,ip,agent,flu)
                log_list.append(r)
            elif live_re.match(x_group[11]):
                channel = live_re.match(x_group[8]).group(1)
                rate = x_group[35].replace("\r\n","")
                try:
                    live_jam = int(x_group[34])>0
                except:
                    live_jam = False
                r = (ip+agent,tim,status,channel,rate,"",live_jam,ip,agent,flu)
                live_list.append(r)
    user_list = {}
    channel_list = {}
    rate_list = {
        "1":0,
        "2":0,
        "3":0,
        "4":0
    }
    suc_n = 0
    jam_n = 0
    flu_total = 0

    #seg logs
    for l in log_list:
        #add users
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

        #channel count
        if channel_list.has_key(l[3]):
            channel_list[l[3]] += 1
        else:
            channel_list[l[3]] = 1

        #time of 4 rates
        seg_mode_time = 4 if l[5] else 10
        if rate_list.has_key(l[4]):
            rate_list[l[4]] += seg_mode_time
        else:
            rate_list[l[4]] = seg_mode_time

        #success request count
        if l[2]:
            suc_n += 1

        #flu total
        flu_total += l[9]

    #trim users in seg logs
    for u in user_list:
        jam = ifjam(user_list[u])
        user_list[u]["jam"] = jam
        if jam:
            jam_n += 1
        user_list[u]["s_ip"] = server_ip
        del user_list[u]["seg_t"]
        del user_list[u]["seg_s"]
        del user_list[u]["seg_e"]

    #live logs
    for l in live_list:
        #add users
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
        #channal count
        if channel_list.has_key(l[3]):
            channel_list[l[3]] += 1
        else:
            channel_list[l[3]] = 1

        #rate_count
        lrm = long_rate_re.match(l[4])
        if lrm:
            i = 1
            while i<5:
                k = str((2500-int(lrm.group(i*2-1)))/500)
                if rate_list.has_key(k):
                    rate_list[k] += int(lrm.group(i*2))
                else:
                    rate_list[k] = int(lrm.group(i*2))

        #success request count
        if l[2]:
            suc_n += 1

        #flu count
        flu_total += l[9]

    #average rate
    rate_a = (rate_list["1"]*2000+rate_list["2"]*1500+rate_list["3"]*850+rate_list["4"]*500)/(rate_list["1"]+rate_list["2"]+rate_list["3"]+rate_list["4"])

    #write into mongo
    try:
        client = MongoClient(mongo_addr)
        db = client.log_db
        log_table = db.log_table
        user_table = db.user_table

        ins_user_res = user_table.insert_many(user_list.values())
        req_n = len(log_list)+len(live_list)
        start = file[7:21]
        log_info = {
            "s_ip":server_ip,
            "start":int(time.mktime((int(start[0:4]),int(start[4:6]),int(start[6:8]),int(start[8:10]),int(start[10:12]),int(start[12:14]),0,0,0))),
            "req_n":req_n,
            "suc_n":suc_n,
            "suc_r":round(float(suc_n*100)/req_n,2),
            "user_n":len(user_list),
            "jam_n":jam_n,
            "freeze_r":round(float(jam_n*100)/len(user_list),2),
            "flu":flu_total,
            "band":round(float(flu_total)*8/300/1024,2),
            "users":ins_user_res.inserted_ids,
            "rate_n":rate_list,
            "bitrate":rate_a,
            "channal_n":channel_list
        }

        log_table.insert_one(log_info)

        #save result locally
        file = open("/data/log_summary","a")
        try:
            del log_info['users']
            try:
                del log_info['_id']
            except:
                pass
        except:
            pass
        file.write(str(log_info)+'\n')
        file.close()

        print "Info:Complete"
    except Exception,e:
        print type(e),":",e,e.args
        print "Error:Unable to write to Mongo"

def n_thread(file):
    print file
    try:
        calculate(file)
    except Exception,e:
        print type(e),":",e,e.args

def monitor():
    dir = log_dir
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

