from __future__ import division

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import time
from operator import add
import re

server_list = [
    "0.0.0.0"
]
req_re = re.compile(r"^http://(\w+)\..+(\d)_/seg(\d).+(\d{9})")
def trim(l):
    x = l[1]
    x_group = x.split(" ")
    print x_group
    time = int(x_group[1][0:10])
    ip = x_group[2]
    server_ip = x_group[0]
    if x_group[7]=="200" or x_group[7]=="206" or x_group[7]=="304":
        status = 1
    else:
        status = 0
    req = x_group[12]

    req_ma = req_re.match(req)
    if req_ma:
        channel = req_ma.group(1)
        rate = int(req_ma.group(2))%5
        seg = req_ma.group(3)==u"1"
        segnum = int(req_ma.group(4))
        r = (server_ip,ip,time,status,channel,rate,seg,segnum,ip + x_group[13])
    else:
        r = None
    return r

def jam(x):
    server_ip = x[0][0]
    jam_list = x[1]
    j = len(jam_list)-1

    #find start
    seg_mode = jam_list[0][6]
    jam_start = (jam_list[0][2],jam_list[0][7])

    #find end
    jam_end = (jam_list[j][2],jam_list[j][7])

    #calculate time
    play_time = jam_end[0]-jam_start[0]
    seg_mode_time = 4 if seg_mode else 10
    seg_time = (jam_end[1]-jam_start[1])*seg_mode_time

    #if jam
    if play_time - seg_time > seg_mode_time:
        return (server_ip,1)
    else:
        return (server_ip,0)

def calculate(time,all_rdd):
    now = time.time()
    ### no sql no for
    rdd = all_rdd
    if rdd.isEmpty() is False:
        rdd.cache()
        total = rdd.map(lambda x:(x[0],1)).reduceByKey(add).cache()
        success = rdd.map(lambda x:(x[0],x[3])).reduceByKey(add).cache()
        channel = rdd.map(lambda x:((x[0],x[4]),1)).reduceByKey(add).cache()
        rate = rdd.map(lambda x:((x[0],x[5]),1)).reduceByKey(add).cache()

        user_group = rdd.groupBy(lambda x:(x[0],x[8])).cache()
        user = user_group.map(lambda x:(x[0][0],1)).reduceByKey(add).cache()
        jams = user_group.mapValues(list).map(jam).reduceByKey(add).cache()

        file = open("/root/bsresults/out.txt","a")
        file.write("start time:"+str(now)+";\n")
        for server in server_list:
            file = open("/root/bsresults/out.txt","a")
            file.write("server:"+str(server)+";")
            file.write("  requests:"+str(len(total.lookup(server))>0 and total.lookup(server)[0] or 0)+";")
            file.write("  users:"+str(len(user.lookup(server))>0 and user.lookup(server)[0] or 0)+";")
            file.write("  success:"+str(len(success.lookup(server))>0 and success.lookup(server)[0] or 0)+";")
            file.write("  jam:"+str(len(jams.lookup(server))>0 and jams.lookup(server)[0] or 0)+";\n")
        file.write("end time:"+str(time.time())+";\n")
        file.close()
    else:
        file = open("/root/bsresults/out.txt","a")
        file.write("start time:"+str(now)+";empty\n")
        file.close()

master = "spark://master:7077"
appName = "whypyspark"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 60)
zkQuorum = 'master:2181,slave3:2181,slave4:2181'
groupid = 'kafka_group_1'
topic = {"kafka1":1}
log = KafkaUtils.createStream(ssc, zkQuorum, groupid, topic)
res = log.map(trim).filter(lambda x:x!=None)

res.foreachRDD(calculate)

ssc.start()
ssc.awaitTermination()
