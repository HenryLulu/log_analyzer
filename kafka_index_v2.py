log_type = 1
kafka_addr = "n0.g1.pzt.powzamedia.com:9092,n1.g1.pzt.powzamedia.com:9092,n2.g1.pzt.powzamedia.com:9092"
if log_type ==1:
    log_dir = "/data/proclog/log/pzs/back"
else:
    log_dir = "/home/fivemin/logback"

from kafka import KafkaClient, SimpleProducer, SimpleConsumer
import re
import os
import time
import threading
import socket
import fcntl
import struct
import json
import random
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
def conn_kafka(user_list,log_info,log_state,user_state):
    try:
        client = KafkaClient(hosts=kafka_addr)
        producer = SimpleProducer(client,async=True)
        if log_state==False:
            try:
                producer.send_messages("logs",log_info)
                log_state=True
            except:
                log_state=False
        if user_state==False:
            try:
                producer.send_messages("users",user_list)
                user_state=True
            except:
                user_state=False
    except Exception,e:
        print Exception,":",e
    return (log_state,user_state)

def calculate(file):
    req_re = re.compile(r"^http://(\w+)\..+(\d)_/seg(\d).+(\d{9})")
    live_re = re.compile(r"^http://(\w+)\..+/live/(ld/flv|ld/trans|flv|trans)/")
    m3u8_re = re.compile(r"^http.+index\.(m3u8|bootstrap)")
    long_rate_re = re.compile(r'^(\d+)_(\d+)\|(\d+)_(\d+)\|(\d+)_(\d+)\|(\d+)_(\d+)$')
    logs = open(log_dir+"/"+file,'r').readlines()

    top_list = {
        'hls_0' : {
            'type' : 1,
            'list' : [],
            'users' : {},
            "req_n":0,
            "suc_n":0,
            "suc_r":0,
            "user_n":0,
            "jam_n":0,
            "freeze_r":0,
            "flu":0,
            "band":0,
            "rate_n":{
                "1":0,
                "2":0,
                "3":0,
                "4":0
            },
            "bitrate":0,
            "channel_n":{}
        },
        'hds_1' : {
            'type' : 1,
            'list' : [],
            'users' : {},
            "req_n":0,
            "suc_n":0,
            "suc_r":0,
            "user_n":0,
            "jam_n":0,
            "freeze_r":0,
            "flu":0,
            "band":0,
            "rate_n":{
                "1":0,
                "2":0,
                "3":0,
                "4":0
            },
            "bitrate":0,
            "channel_n":{}
        },
        'ld/flv' : {
            'type' : 2,
            'list' : [],
            'users' : {},
            "req_n":0,
            "suc_n":0,
            "suc_r":0,
            "user_n":0,
            "jam_n":0,
            "freeze_r":0,
            "flu":0,
            "band":0,
            "rate_n":{
                "1":0,
                "2":0,
                "3":0,
                "4":0
            },
            "bitrate":0,
            "channel_n":{}
        },
        'ld/trans' : {
            'type' : 2,
            'list' : [],
            'users' : {},
            "req_n":0,
            "suc_n":0,
            "suc_r":0,
            "user_n":0,
            "jam_n":0,
            "freeze_r":0,
            "flu":0,
            "band":0,
            "rate_n":{
                "1":0,
                "2":0,
                "3":0,
                "4":0
            },
            "bitrate":0,
            "channel_n":{}
        },
        'flv' : {
            'type' : 2,
            'list' : [],
            'users' : {},
            "req_n":0,
            "suc_n":0,
            "suc_r":0,
            "user_n":0,
            "jam_n":0,
            "freeze_r":0,
            "flu":0,
            "band":0,
            "rate_n":{
                "1":0,
                "2":0,
                "3":0,
                "4":0
            },
            "bitrate":0,
            "channel_n":{}
        },
        'trans' : {
            'type' : 2,
            'list' : [],
            'users' : {},
            "req_n":0,
            "suc_n":0,
            "suc_r":0,
            "user_n":0,
            "jam_n":0,
            "freeze_r":0,
            "flu":0,
            "band":0,
            "rate_n":{
                "1":0,
                "2":0,
                "3":0,
                "4":0
            },
            "bitrate":0,
            "channel_n":{}
        },
    }
    total = {
        'user_list':[],
        'req_n':0,
        'suc_n':0,
        'jam_n':0,
        'flu':0,
        'band':0,
        'rate_n':{
            '1':0,
            '2':0,
            '3':0,
            '4':0
        },
        'channel_n':{}
    }

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
            live_ma = live_re.match(x_group[11])
            if req_ma:
                channel = req_ma.group(1)
                rate = str(int(req_ma.group(2))%5)
                seg = req_ma.group(3)==u"1"
                segnum = int(req_ma.group(4))
                r = (ip+agent,tim,status,channel,rate,seg,segnum,ip,agent,flu)
                if seg:
                    top_list['hds_1']['list'].append(r)
                else:
                    top_list['hls_0']['list'].append(r)
            elif live_ma:
                type = live_ma.group(2)
                channel = live_ma.group(1)
                rate = x_group[3]
                try:
                    live_jam = int(x_group[2])>0
                except:
                    live_jam = False
                r = (ip+agent,tim,status,channel,rate,"",live_jam,ip,agent,flu)
                if top_list.has_key(type):
                    top_list[type]['list'].append(r)

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
            live_ma = live_re.match(x_group[8])
            if req_ma:
                channel = req_ma.group(1)
                rate = str(int(req_ma.group(2))%5)
                seg = req_ma.group(3)==u"1"
                segnum = int(req_ma.group(4))
                r = (ip+agent,tim,status,channel,rate,seg,segnum,ip,agent,flu)
                if seg:
                    top_list['hds_1']['list'].append(r)
                else:
                    top_list['hls_0']['list'].append(r)
            elif live_ma:
                type = live_ma.group(2)
                channel = live_ma.group(1)
                rate = x_group[35].replace("\r\n","")
                try:
                    live_jam = int(x_group[34])>0
                except:
                    live_jam = False
                r = (ip+agent,tim,status,channel,rate,"",live_jam,ip,agent,flu)
                if top_list.has_key(type):
                    top_list[type]['list'].append(r)

    for category_name in top_list:
        current_category = top_list[category_name]
        log_list = current_category['list']
        user_list = current_category['users']
        rate_list = current_category['rate_n']
        channel_list = current_category['channel_n']
        if current_category['type']==1:
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
                    channel_list[l[3]] += l[9]
                else:
                    channel_list[l[3]] = l[9]
                if total['channel_n'].has_key(l[3]):
                    total['channel_n'][l[3]] += l[9]
                else:
                    total['channel_n'][l[3]] = l[9]

                seg_mode_time = 4 if l[5] else 10
                if rate_list.has_key(l[4]):
                    rate_list[l[4]] += seg_mode_time
                else:
                    rate_list[l[4]] = seg_mode_time

                if l[2]:
                    current_category['suc_n'] += 1
                #flu total
                current_category['flu'] += l[9]

            for u in user_list:
                jam = ifjam(user_list[u])
                user_list[u]["jam"] = jam
                if jam:
                    current_category['jam_n'] += 1
                user_list[u]["s_ip"] = server_ip
                del user_list[u]["seg_t"]
                del user_list[u]["seg_s"]
                del user_list[u]["seg_e"]

        elif current_category['type']==2:
            for l in log_list:
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
                    channel_list[l[3]] += l[9]
                else:
                    channel_list[l[3]] = l[9]
                if total['channel_n'].has_key(l[3]):
                    total['channel_n'][l[3]] += l[9]
                else:
                    total['channel_n'][l[3]] = l[9]

                lrms = long_rate_re.findall(l[4])
                for lrm in lrms:
                    k = str((2500-int(lrm[0]))/500)
                    if rate_list.has_key(k):
                        rate_list[k] += int(lrm[1])
                    else:
                        rate_list[k] = int(lrm[1])

                if l[2]:
                    current_category['suc_n'] += 1
                #flu total
                current_category['flu'] += l[9]
            for u in user_list:
                if user_list[u]["jam"]:
                    current_category['jam_n'] += 1

        current_category['req_n'] = len(log_list)
        current_category['user_n'] = len(user_list)
        if current_category['req_n']!=0:
            current_category['suc_r'] = round(float(current_category['suc_n']*100)/current_category['req_n'],2)
        if len(user_list)!=0:
            current_category['freeze_r'] = round(float(current_category['jam_n']*100)/len(user_list),2)
        current_category['band'] = round(float(current_category['flu'])*8/300/1024,2)
        try:
            current_category['bitrate'] = (rate_list["1"]*2000+rate_list["2"]*1500+rate_list["3"]*850+rate_list["4"]*500)/(rate_list["1"]+rate_list["2"]+rate_list["3"]+rate_list["4"])
        except:
            current_category['bitrate'] = 0

        #to total
        total['user_list'].extend(user_list.values())
        total['req_n'] += current_category['req_n']
        total['suc_n'] += current_category['suc_n']
        total['jam_n'] += current_category['jam_n']
        total['flu'] += current_category['flu']
        total['band'] += current_category['band']
        total['rate_n']['1'] += current_category['rate_n']['1']
        total['rate_n']['2'] += current_category['rate_n']['2']
        total['rate_n']['3'] += current_category['rate_n']['3']
        total['rate_n']['4'] += current_category['rate_n']['4']
        #clear
        del current_category['type']
        del current_category['list']
        del current_category['users']

    start = file[7:21]
    starttm = int(time.mktime((int(start[0:4]),int(start[4:6]),int(start[6:8]),int(start[8:10]),int(start[10:12]),int(start[12:14]),0,0,0)))

    user_list = total['user_list']
    log_info = top_list
    log_info['s_ip'] = server_ip
    log_info['start'] = starttm
    log_info['req_n'] = total['req_n']
    log_info['suc_n'] = total['suc_n']
    if total['req_n']!=0:
        log_info['suc_r'] = round(float(total['suc_n']*100)/total['req_n'],2)
    log_info['user_n'] = len(user_list)
    log_info['jam_n'] = total['jam_n']
    if len(user_list)!=0:
        log_info['freeze_r'] = round(float(total['jam_n']*100)/len(user_list),2)
    log_info['flu'] = total['flu']
    log_info['band'] = total['band']
    log_info['rate_n'] = total['rate_n']
    try:
        log_info['bitrate'] = (log_info['rate_n']["1"]*2000+log_info['rate_n']["2"]*1500+log_info['rate_n']["3"]*850+log_info['rate_n']["4"]*500)/(log_info['rate_n']["1"]+log_info['rate_n']["2"]+log_info['rate_n']["3"]+log_info['rate_n']["4"])
    except:
        log_info['bitrate'] = 0
    log_info['channel_n'] = total['channel_n']

    user_list_json = json.JSONEncoder().encode(user_list)
    log_info_json = json.JSONEncoder().encode(log_info)

    #write into kafka
    time.sleep(random.randint(0,30))
    retry_time = 10
    log_state = False
    user_state = False
    while retry_time>0:
        retry_time -= 1
        res = conn_kafka(user_list_json,log_info_json,log_state,user_state)
        log_state = res[0]
        user_state = res[1]
        if log_state and user_state:
            print "Info:Complete"
            break
        time.sleep(5)
    if retry_time == 0:
        print "Error:Kafka error and retry failed"

def n_thread(file):
    print file
    try:
        calculate(file)
    except Exception,e:
        print Exception,":",e

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
