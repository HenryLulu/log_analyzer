#!/bin/sh
while true; do
    NUM=`ps aux | grep local_index.py | grep -v grep |wc -l`
    if [ "${NUM}" -lt "1" ];then
        python -u ./local_index.py
    fi
done