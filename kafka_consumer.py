consumer="c0"
kafka_addr = "n0.g1.pzt.powzamedia.com:9092,n1.g1.pzt.powzamedia.com:9092,n2.g1.pzt.powzamedia.com:9092"
mongo_addr = "mongodb://n0.g1.pzt.powzamedia.com:27017,n1.g1.pzt.powzamedia.com:27017,n2.g1.pzt.powzamedia.com:27017"

from pykafka import KafkaClient
from pymongo import *
import multiprocessing
import time
import json

def conn_kafka_log():
    log_pool = []
    client = KafkaClient(hosts=kafka_addr)
    log_topic = client.topics['logs']
    logs = log_topic.get_simple_consumer(consumer_group=consumer,reset_offset_on_start=True)
    for log in logs:
        if log is not None:
            current_log = json.JSONDecoder().decode(log.value)
            if isinstance(current_log,dict):
                log_pool.append(current_log)
        if len(log_pool)>10:
            log_pool = conn_mongo("log",log_pool)


def conn_kafka_user():
    log_pool = []
    client = KafkaClient(hosts=kafka_addr)
    user_topic = client.topics['users']
    users = user_topic.get_simple_consumer(consumer_group=consumer,reset_offset_on_start=True)
    for log in users:
        if log is not None:
            current_log = json.JSONDecoder().decode(log.value)
            if isinstance(current_log,list):
                log_pool.extend(current_log)
            elif isinstance(current_log,dict):
                log_pool.extend(current_log.values())
        if len(log_pool)>0:
            log_pool = conn_mongo("user",log_pool)


def conn_mongo(table,data):
    try_time = 10
    while try_time>0:
        try_time -= 1
        try:
            client = MongoClient(mongo_addr)
            print str(client)
            db = client.log_db
            if table=="log":
                tb = db.log_table
                tb.insert_many(data)
                print "Info:Complete log"
                return []
            elif table=="user":
                tb = db.user_table
                tb.insert_many(data)
                print "Info:Complete user"
                return []
        except Exception,e:
            time.sleep(5)
            print type(e),":",e,e.args
            print "Error:Connect Mongo error:"+table

    if try_time == 0:
        print "Error:Write Mongo error and retry failed"
        return data

    client = MongoClient(mongo_addr)
    db = client.log_db
    log_table = db.log_table
    user_table = db.user_table

def log_pro():
    print 11
    while True:
        try:
            conn_kafka_log()
        except Exception,e:
            time.sleep(5)
            print type(e),":",e,e.args
            print "Error:Loss connect to log kafka"

def user_pro():
    print 22
    while True:
        try:
            conn_kafka_user()
        except Exception,e:
            time.sleep(5)
            print type(e),":",e,e.args
            print "Error:Loss connect to user kafka"

def main():
    try:
        pool = multiprocessing.Pool(processes=2)
        try:
            pool.apply_async(log_pro, ())
        except Exception,e:
            print type(e),":",e,e.args
            print "Error:Init log process error"

        try:
            pool.apply_async(user_pro, ())
        except Exception,e:
            print type(e),":",e,e.args
            print "Error:Init user process error"
        pool.close()
        pool.join()
    except Exception,e:
        print type(e),":",e,e.args
        print "Error:Init process pool error"

main()
