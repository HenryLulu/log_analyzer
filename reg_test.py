import re

str1 = "500_1|850_2|1500_3|2000_4"

long_rate_re = re.compile(r'(\d+)_(\d+)')

lrms = long_rate_re.findall(str1)
print lrms
rate_list = {
    '1':10
}
for lrm in lrms:
    k = str((2500-int(lrm[0]))/500)
    if rate_list.has_key(k):
        rate_list[k] += int(lrm[1])
    else:
        rate_list[k] = int(lrm[1])

print rate_list
