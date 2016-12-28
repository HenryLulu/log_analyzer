from fabric.api import *
#use "fab function_name" in console to do a job

env.passwords = {
    'root@tj:12321':'qqnkm4kevgjtsetq1c',
    'root@sd1:12321':'ejidjwmipv7wryf4qm',
    'root@sd2:12321':'hgo8poyy9rxnxsokxk'
}
env.hosts = [
    'root@tj:12321',
    'root@sd1:12321',
    'root@sd2:12321'
]

def upload():
    put('local_index.py', '~/local_index.py')

def restart():
    try:
        run("ps -ef|grep local_index.py|grep -v grep|awk '{print $2}'|xargs kill -9")
    finally:
        with cd("/root/"):
            run("nohup python -u local_index.py & sleep 1")

def start():
    with cd("/root/"):
        run("nohup python -u local_index.py & sleep 1")

def check():
    run("ps -ef|grep local_index.py|grep -v grep")

def tail():
    run("tail -n 100 nohup.out")

def clear():
    run("cat /dev/null > nohup.out")

def fetch(file):
    get('/data/proclog/log/pzs/back/'+file,'/Users/henry/bsfiles/access.log')

def up_cons():
    put('kafka_consumer.py', '/usr/local/kafka_consumer/kafka_consumer.py')
def re_cons():
    try:
        run("ps -ef|grep kafka_consumer.py|grep -v grep|awk '{print $2}'|xargs kill -9")
    finally:
        with cd("/data/kafka_nohup/consumer/"):
            run("nohup python -u /usr/local/kafka_consumer/kafka_consumer.py & sleep 1")
def kill_cons():
    try:
        run("ps -ef|grep kafka_consumer.py|grep -v grep|awk '{print $2}'|xargs kill -9")
    finally:
        pass

def check_cons():
    run("ps -ef|grep kafka_consumer.py|grep -v grep")
def log_cons():
    with cd("/data/kafka_nohup/consumer/"):
        run("tail -n 100 nohup.out")
def clear_cons():
    with cd("/data/kafka_nohup/consumer/"):
        run("cat /dev/null > nohup.out")

def free_com(where,what):
    with cd(where):
        run(what)

