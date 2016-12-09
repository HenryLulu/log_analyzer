import time
from pymongo import *
import os
# file_dir = '/Users/henry/bsfiles/'
file_dir = '/data/'

mongo_addr = "mongodb://183.250.179.150:27017,117.145.178.217:27017,117.145.178.218:27017"

def get():
    now = time.localtime(time.time())
    start = time.mktime((now.tm_year,now.tm_mon,now.tm_mday-1,0,0,0,0,0,0))
    end = time.mktime((now.tm_year,now.tm_mon,now.tm_mday,0,0,0,0,0,0))

    client = MongoClient(mongo_addr)
    db = client.log_db
    tb = db.log_table
    logs = tb.find({
        'start':{
            '$gte':start,
            '$lt':end
        }
    })
    year = str(now.tm_year)
    month = ('0'+str(now.tm_mon)) if now.tm_mon<10 else str(now.tm_mon)
    day = ('0'+str(now.tm_mday-1)) if (now.tm_mday-1)<10 else str(now.tm_mday-1)
    f = open(file_dir+'summary_'+year+month+day+'.tmp','w')
    for log in logs:
        f.write(str(log['start'])+" "+str(log['s_ip'])+" "+str(log['band'])+" "+str(log['jam_r'])+" "+str(log['suc_r'])+" "+str(log['rate_a'])+" \n")
    f.close()
    os.rename(file_dir+'summary_'+year+month+day+'.tmp',file_dir+'summary_'+year+month+day)

def main():
    while True:
        current_time = time.localtime(time.time())
        if((current_time.tm_hour == 1) and (current_time.tm_min == 0)):
            try:
                get()
            except Exception,e:
                print "get function Error:"
                print Exception,":",e
            finally:
                time.sleep(30)
        time.sleep(30)

main()