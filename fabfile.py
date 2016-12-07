from fabric.api import *
#use "fab function_name" in console to do a job

env.passwords = {
    'root@113.6.235.22:12321':'ir8ayqvrjaxkumk0af',
    'root@113.6.235.23:12321':'cr9ftwlbiiy6mxhymc',
    'root@113.6.235.59:12321':'fzzy1wrorqohr7aupi',
    'root@113.6.235.60:12321':'fh0icvs3lndzbshunp',
    'root@113.6.235.61:12321':'lgirsxgehun61tpwmn',
    'root@183.250.179.150:12321':'sh3aiecgmakjb9urdc'
}
env.hosts = [
    'root@113.6.235.22:12321',
    'root@113.6.235.23:12321',
    'root@113.6.235.59:12321',
    'root@113.6.235.60:12321',
    'root@113.6.235.61:12321',
    'root@183.250.179.150:12321'
]

def upload():
    put('local_index.py', '~/local_index.py')

def restart():
    run("ps -ef|grep local_index.py|grep -v grep|awk '{print $2}'|xargs kill -9")
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