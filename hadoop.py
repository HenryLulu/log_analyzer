from hdfs.client import Client
client = Client("http://139.217.15.189:50070")

client.upload("/","/Users/henry/bsfiles/access_20161222095000.log.tar.gz")
print ""



# from snakebite.namenode import Namenode
# from snakebite.client import HAClient
# n1 = Namenode("139.217.15.189", 9999)
# client = HAClient([n1])
# df = client.df
# # rp = client.service_stub_class.DESCRIPTOR.methods_by_name['getDatanodeReport']()
# for x in client.ls(['/']):
#     print x