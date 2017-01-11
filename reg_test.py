import re
ip = "172.15.1.1\n"

ip_re = re.compile(r"(10\..+)|(172\.((1[6-9])|(2[0-9])|(3[0-1]))\..+)|(192\.168\..+)")

req_ma = not ip_re.match(ip)

print ""
