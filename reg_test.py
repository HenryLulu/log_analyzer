import re
log = '1468178999.000 112.26.229.28 - - 2016-07-11 03:29:59 200 722292 cctv5.vtime.cntv.cloudcdn.net 0 GET http://cctv5.vtime.cntv.cloudcdn.net:80/cache/212_/seg1/indexSeg367044738-Frag367044738 "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36" "-" - "http://cctv5.vtime.cntv.cloudcdn.net:80/cache/212_/seg1/indexSeg367044738-Frag367044738"'

req_re = re.compile(r'^(\d{10})\.\d{3}\s([\d\.]+).+\s(\d{3})\s.+http://(\w+)\..+(\d)_/seg(\d).+(\d{9})\s"(.+)"\s"-.+$')
total_ma = req_re.match(log)


print(total_ma.group(1))
print(total_ma.group(2))
print(total_ma.group(3))
print(total_ma.group(4))
print(total_ma.group(5))
print(total_ma.group(6))
print(total_ma.group(7))
print(total_ma.group(8))
