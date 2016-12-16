#!/bin/sh
while true; do
    NUM=`ps aux | grep local_index.py | grep -v grep |wc -l`
    if [ "${NUM}" == "0" ];then
        nohup python -u ./local_index.py &
        sleep 5
    fi
done