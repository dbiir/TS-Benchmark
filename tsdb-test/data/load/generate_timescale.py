import time
data_src = open('./load.data')
data_target= open('timescaledb.csv','a')
index=1
while True:
    line = data_src.readline()
    if not line:
        break
    if (line.strip() != ''):
        date_str=line[0:13]
        str_suffix=line[13:]
        timeArray = time.localtime(int(date_str)/1000)
        otherStyleTime = time.strftime("%Y%m%d %H:%M:%S", timeArray)
        data_target.write(otherStyleTime+str_suffix)
        if(index%1000==0):
            print(index)
        index=index+1
data_target.close()
data_src.close()
