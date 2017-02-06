import os

dir = "/Users/henry/test"
origin = set([_f[2] for _f in os.walk(dir)][0])
while True:
    final = set([_f[2] for _f in os.walk(dir)][0])
    dif = final.difference(origin)