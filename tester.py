
import re
print not re.compile(r"(10\..+)|(172\.((1[6-9])|(2[0-9])|(3[0-1]))\..+)|(192\.168\..+)").match(("172.16.184.64"))

# log_dir = "/Users/henry/bsfiles/new"
# files = ["access_20170302160500.log",
#          "access_20170302160600.log",
#          "access_20170302160700.log",
#          "access_20170302160800.log",
#          "access_20170302160900.log"]
#
# flu = 0
# num = 0
# flu_list = []
# for file in files:
#     logs = open(log_dir+"/"+file,'r').readlines()
#     for l in logs:
#         group = l.split(" ")
#         flu += int(group[3])
#         flu_list.append(group[3])
#         num +=1
#
# print flu
# print num
#
# log_dir = "/Users/henry/bsfiles/new"
# file = "access_20170302160500.old.log"
#
# flu_old = 0
# num = 0
# flu_old_list = []
# old_list = []
# logs = open(log_dir+"/"+file,'r').readlines()
# for l in logs:
#     group = l.split(" ")
#     if group[6]=="0":
#         continue
#     flu_old += int(group[7])
#     flu_old_list.append(group[7])
#     old_list.append(l)
#     num += 1
#
# print flu_old
# print num
#
# numm = 0
# old_flag = 0
# new_flag = 0
# old_len = len(flu_old_list)
# while old_flag < old_len:
#     if flu_old_list[old_flag] == flu_list[new_flag]:
#         old_flag += 1
#         new_flag += 1
#     else:
#         print old_list[old_flag]
#         numm +=1
#         old_flag += 1
#
# print numm
#
