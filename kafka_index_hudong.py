log_type = 1
code_version = "ICSAgent V3.3"
code_build = "2017041701"
log_duration = 60  #s
code_name = "/usr/local/pzs/pzt/local_index.py"
pzt_dir = "/usr/local/pzs/pzt/"

fail_times = 0
import time
last_fail_time = time.time()
ftp_conf = {
    "addr": "monitor1.powzamedia.com",
    "port": "20021",
    "user": "upload",
    "pwd": "sjdd123",
    "remote_dir":"/data2/upload/"
}
kafka_addr = ["n0.g1.pzt.powzamedia.com:9092","n1.g1.pzt.powzamedia.com:9092","n2.g1.pzt.powzamedia.com:9092"]
log_dir = "/usr/local/pzs/pzlogbak"

from kafka import KafkaProducer
# from multiprocessing import Process
import threading
from ftplib import FTP
ftp = FTP()
import re
import os
import socket
import fcntl
import struct
import json
json.encoder.FLOAT_REPR = lambda x: format(x, '.2f')
import random
import signal
import logging

#init public vars
try:
    from hashlib import md5
    m = md5()
    a_file = open(code_name, 'rb')
    m.update(a_file.read())
    a_file.close()
    md5_str = m.hexdigest()
except:
    md5_str = "unknow"

try:
    in_ip_re = re.compile(r"(10\..+)|(172\.((1[6-9])|(2[0-9])|(3[0-1]))\..+)|(192\.168\..+)")
    server_ip = "unknow"
    ips = os.popen("LANG=C ifconfig | grep \"inet addr\" | grep -v \"127.0.0.1\" |grep -v \"0.0.0.0\"| awk -F \":\" '{print $2}' | awk '{print $1}'").readlines()
    for ip in ips:
        ip = ip.replace("\n","")
        if not in_ip_re.match(ip):
            server_ip = ip
            break
except:
    server_ip = "unknow"

cdn_name = "unknow"
if log_type==1:
    cdn_name = "kw"
elif log_type==2:
    cdn_name = "dl"
elif log_type==3:
    cdn_name = "ws"
elif log_type==4:
    cdn_name = "pbs"

class TimeOutException(Exception):
    pass

def init_log():
    try:
        os.rename("/usr/local/pzs/pzt/info_2.log","/usr/local/pzs/pzt/info_3.log")
    except:
        pass
    try:
        os.rename("/usr/local/pzs/pzt/info_1.log","/usr/local/pzs/pzt/info_2.log")
    except:
        pass
    try:
        os.rename("/usr/local/pzs/pzt/info.log","/usr/local/pzs/pzt/info_1.log")
    except:
        pass
    logging.basicConfig(level=logging.INFO,
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filename='/usr/local/pzs/pzt/info.log',
        filemode='w')

def ifjam(u):
    seg_mode_time = 4 if u["seg_t"] else 10
    return (u["end"]-u["start"]-(u["seg_e"]-u["seg_s"])*seg_mode_time) > seg_mode_time
def stringtify_user_obj(u):
    channel_s = ""
    rate_s = ""
    for c in u['channel_n']:
        channel_s = channel_s + c + ':' + str(u['channel_n'][c]) + ','
    for r in u['rate_n']:
        rate_s = rate_s + r + ':' + str(u['rate_n'][r]) + ','    
    return str(u['u_ip'])+'_'+str(u['flu'])+'_'+str(u['start'])+'_'+str(u['end'])+'_'+str(u['jam'])+'_'+str(u['req_n'])+'_'+str(u['suc_n'])+'_'+rate_s+'_'+channel_s
def conn_kafka(user_list,log_info,log_state,user_state):
    random.shuffle(kafka_addr)
    producer = None
    #find an available broker
    for broker in kafka_addr:
        try:
            producer = KafkaProducer(bootstrap_servers=broker)
            logging.info("connected to broker: "+broker)
            break
        except Exception,e:
            logging.debug(str(Exception)+":"+str(e))
    if producer is not None:
        if log_state==False:
            try:
                res_log = producer.send("logs",log_info)
                time.sleep(5)
                if res_log.is_done:
                    log_state=True
            except:
                log_state=False
        if user_state==False:
            try:
                res_user = producer.send("users",user_list)
                time.sleep(5)
                if res_user.is_done:
                    user_state=True
            except:
                user_state=False
        producer.close()
    else:
        logging.debug("no broker available")

    return (log_state,user_state)

def calculate(file):
    start = file[7:21]
    starttm = int(time.mktime((int(start[0:4]),int(start[4:6]),int(start[6:8]),int(start[8:10]),int(start[10:12]),int(start[12:14]),0,0,0)))

    logging.info("start analyzing:"+file)
#define reg
    req_re = re.compile(r"^(.+)(\d)_/seg(\d).+(\d{9})")
    live_re = re.compile(r"^(.*)/live/(ld/flv|ld/trans|flv|trans)/")
    long_rate_re = re.compile(r'(\d+)_(\d+)')
    channel_re = re.compile(r'^([^\d\.]+[^\.]*)\..*')
    logs = open(log_dir+"/"+file,'r').readlines()

#init top_list
    top_list = {
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
            "rate_n":{},
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
            "rate_n":{},
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
            "rate_n":{},
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
            "rate_n":{},
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
        'rate_n':{},
        'channel_n':{}
    }

#format logs
    for l in logs:
        try:
            agent = l.split('"')[1].decode("utf-8",'ignore')
        except:
            continue
        try:
            x_group = l.split(" ")
            # 0Begin_Time, 1User_IP, 2ResponseCode, 3Flu, 4Duration, 5Freeze_Count, 6Bitrate, 7Domain, 8Port, 9URI, 10UserAgent
            if len(x_group)<11:
                continue
            ip = x_group[1]
            tim = int(x_group[0])
            status = bool(re.compile(r"^(2|3)\d{2}$").match(x_group[2]))
            flu = int(x_group[3])
            duration = int(x_group[4])
            channel_ma = channel_re.match(x_group[7])
            live_ma = live_re.match(x_group[9])
            if channel_ma:
                channel = channel_ma.group(1)
            else:
                channel = "unknow"
            if live_ma:
                type = live_ma.group(2)
                rate = x_group[6]
                try:
                    live_jam = int(x_group[5])>0
                except:
                    live_jam = False
                r = (ip+agent,tim,status,channel,rate,"",live_jam,ip,agent,flu,duration)
                if top_list.has_key(type):
                    top_list[type]['list'].append(r)
        except:
            pass

#analyze top_list
    for category_name in top_list:
        current_category = top_list[category_name]
        log_list = current_category['list']
        user_list = current_category['users']
        rate_list = current_category['rate_n']
        channel_list = current_category['channel_n']

        if current_category['type']==2:
            for l in log_list:
                if user_list.has_key(l[0]):
                    user_list[l[0]]["req_n"] += 1
                    if l[2]:
                        user_list[l[0]]["suc_n"] += 1
                    user_list[l[0]]["flu"] += l[9]
                    user_list[l[0]]["duration"] += l[10]
                else:
                    user_list[l[0]] = {
                        "u_ip":l[7],
                        "req_n":1,
                        "suc_n":1 if l[2] else 0,
                        "start":l[1],
                        "end":l[1],
                        "agent":l[8],
                        "jam": l[6],
                        "flu":l[9],
                        "duration":l[10],
                        "rate_n":{},
                        "channel_n":{},
                        "type":category_name
                    }
                if channel_list.has_key(l[3]):
                    channel_list[l[3]] += l[9]
                else:
                    channel_list[l[3]] = l[9]
                if total['channel_n'].has_key(l[3]):
                    total['channel_n'][l[3]] += l[9]
                else:
                    total['channel_n'][l[3]] = l[9]
                if user_list[l[0]]['channel_n'].has_key(l[3]):
                    user_list[l[0]]['channel_n'][l[3]] += l[9]
                else:
                    user_list[l[0]]['channel_n'][l[3]] = l[9]

                lrms = long_rate_re.findall(l[4])
                for lrm in lrms:
                    k = lrm[0]
                    if rate_list.has_key(k):
                        rate_list[k] += int(lrm[1])
                    else:
                        rate_list[k] = int(lrm[1])
                    if user_list[l[0]]['rate_n'].has_key(k):
                        user_list[l[0]]['rate_n'][k] += int(lrm[1])
                    else:
                        user_list[l[0]]['rate_n'][k] = int(lrm[1])

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
        current_category['band'] = round(float(current_category['flu'])*8/log_duration/1000,2)
        try:
            current_category['bitrate'] = (rate_list["0"]*4000+rate_list["1"]*2000+rate_list["2"]*1500+rate_list["3"]*850+rate_list["4"]*500)/(rate_list["1"]+rate_list["2"]+rate_list["3"]+rate_list["4"])
        except:
            current_category['bitrate'] = 0

        #to total
        total['user_list'].extend(list(map(stringtify_user_obj,user_list.values())))
        total['req_n'] += current_category['req_n']
        total['suc_n'] += current_category['suc_n']
        total['jam_n'] += current_category['jam_n']
        total['flu'] += current_category['flu']
        total['band'] += current_category['band']
        for rate in current_category['rate_n']:
            if total['rate_n'].has_key(rate):
                total['rate_n'][rate] += current_category['rate_n'][rate]
            else:
                total['rate_n'][rate] = current_category['rate_n'][rate]
        #clear
        del current_category['type']
        del current_category['list']
        del current_category['users']

#add total keys
    user_list = total['user_list']
    log_info = top_list
    log_info['from'] = log_type
    log_info['version'] = code_version+' '+code_build
    log_info['duration'] = log_duration
    log_info['md5'] = md5_str
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
        log_info['bitrate'] = 0
        total_rate = 0
        total_time = 0
        for rate in total['rate_n']:
            total_rate += int(rate)*int(total['rate_n'][rate])
            total_time += float(total['rate_n'][rate])
        log_info['bitrate'] = round(total_rate/total_time,2)
    except:
        log_info['bitrate'] = 0
    log_info['channel_n'] = total['channel_n']

#send to kafka
    user_list_json = json.JSONEncoder().encode({
        'log_time':starttm,
        'from':log_type,
        's_ip':server_ip,
        'users':user_list
    })
    log_info_json = json.JSONEncoder().encode(log_info)

    retry_time = 10
    log_state = False
    user_state = False
    global fail_times
    global last_fail_time
    while retry_time>0:
        retry_time -= 1
        res = conn_kafka(user_list_json,log_info_json,log_state,user_state)
        log_state = res[0]
        user_state = res[1]
        if log_state and user_state:
            logging.info("complete analyzing:"+file)
            break
        time.sleep(5)
    if retry_time == 0:
        if time.time()-last_fail_time>600:
            fail_times = 0
        else:
            last_fail_time = time.time()
        if fail_times > 10:
            logging.error("kill myself")
            os._exit(0)
        else:
            logging.error("Kafka error and retry failed")
            fail_times += 1
            raise TimeOutException()

    #func end

def handler(signum, frame):
    logging.error("Log Timeout")
    raise TimeOutException()

def upload(file):
    logging.info("start uploading:"+file)
    re_up_time = 0
    while re_up_time <3:
        re_up_time = re_up_time+1
        try:
            ftp.connect(ftp_conf["addr"],ftp_conf["port"])
            ftp.login(ftp_conf["user"],ftp_conf["pwd"])
            ftp.cwd(ftp_conf["remote_dir"])
            file_stream = open(log_dir+"/"+file,'rb')
            ftp.storbinary("STOR "+cdn_name+"_"+server_ip+"_"+file,file_stream)
            ftp.quit()
            break
        except Exception,e:
            logging.debug(str(Exception)+":"+str(e)+str(e.args))
            logging.debug("fail to upload:" + file + ", now retry...")
    if re_up_time < 3:
        logging.info("complete uploading:"+file)
    else:
        logging.error("failed to upload:"+file+",and retry failed")

def monitor():
    dir = log_dir
    origin = set([_f[2] for _f in os.walk(dir)][0])
    while True:
        time.sleep(3)
        final = set([_f[2] for _f in os.walk(dir)][0])
        dif = final.difference(origin)
        origin = final
        while len(dif) > 0:
            file = dif.pop()
            if re.compile(r"^access_.+log$").match(file):
                err_try_time = 0
                try:
                    signal.signal(signal.SIGALRM, handler)
                    signal.alarm(50)
                    time.sleep(random.randint(0,10))
                    calculate(file)
                    error_files = open(pzt_dir+"timeout_logs",'w+').readlines()
                    while len(error_files)>0:
                        err_file = error_files.pop(0)
                        open(pzt_dir+"timeout_logs",'w+').writelines(error_files)
                        err_f_ma = re.compile(r"^(access_.+log):(\d).+").match(err_file)
                        if err_f_ma:
                            file = err_f_ma.group(1)
                            err_try_time = int(err_f_ma.group(2))
                            if err_try_time < 9:
                                err_try_time += 1
                                try:
                                    calculate(file)
                                except:
                                    logging.error("File: "+file+" doesn't exist")

                        #new_progress(file)
                    signal.alarm(0)
                except TimeOutException, e:
                    try:
                        add_f = open(pzt_dir+"timeout_logs",'w+').readlines()
                        add_f.append(file+":"+str(err_try_time)+"\n")
                        open(pzt_dir+"timeout_logs",'w+').writelines(add_f)
                    except:
                        logging.error("add timeout file error")
                except Exception,e:
                    logging.error(str(Exception)+":"+str(e)+str(e.args))

            elif re.compile(r"^access_.+log.7z$").match(file):
                try:
                    # time.sleep(10)
                    # p = Process(target=upload, args=(file,))
                    # p.start()
                    # p.join()
                    threading.Thread(target=upload, args=(file,)).start() 
                except:
                    logging.error("fail to start upload progress:"+file)

def main():
    init_log()
    logging.info("start..."+server_ip)
    try:
        monitor()
    except:
        logging.error("Init fail")

main()
