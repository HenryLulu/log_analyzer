from ftplib import FTP
log_dir = "/Users/henry/bsfiles/new"
file = "access_20170226113800.log"

ftp_conf = {
    "addr": "monitor2.powzamedia.com",
    "port": "20021",
    "user": "upload",
    "pwd": "sjdd123",
    "remote_dir":"/data2/upload/"
}

log_type = 1
cdn_name = "unknow"
if log_type==1:
    cdn_name = "kw"
elif log_type==2:
    cdn_name = "dl"
elif log_type==3:
    cdn_name = "ws"
elif log_type==4:
    cdn_name = "pbs"

server_ip = "215.12.8.93"

# ftp = FTP()
re_up_time = 0
while re_up_time <1:
        re_up_time = re_up_time+1
        try:
            ftp = FTP()
            ftp.connect(ftp_conf["addr"],ftp_conf["port"])
            ftp.login(ftp_conf["user"],ftp_conf["pwd"])
            ftp.cwd(ftp_conf["remote_dir"]+cdn_name+"_"+file[7:15]+"/"+file[15:19]+"/")
            file_stream = open(log_dir+"/"+file,'rb')
            ftp.storbinary("STOR "+cdn_name+"_"+server_ip+"_"+file,file_stream)
            ftp.quit()
            break
        except Exception,e:
            print(str(Exception)+":"+str(e)+str(e.args))
            print("fail to upload:" + file + ", now retry...")
print re_up_time
if re_up_time < 3:
        print("complete uploading:"+file)
else:
        print("failed to upload:"+file+",and retry failed")
