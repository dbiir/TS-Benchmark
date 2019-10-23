
data_src = open('./load.data')
data_target= open('influxdb.csv','a')
head_line="# DDL\n# CREATE DATABASE ruc_test\n"
head_line2="# DML\n# CONTEXT-DATABASE: ruc_test\n"
data_target.write(head_line)
data_target.write(head_line2)
index=1
while True:
    line = data_src.readline()
    if not line:
        break
    if (line.strip() != ''):
        columns = line.split(',')
        date_str=columns[0]
        f = columns[1]
        d = columns[2]
        c_index=3;
        while(c_index<53):
            value = columns[c_index]
            influx_line = "sensor,f="+f+",d="+d+",s=s"+str(c_index-2)+" value="+str(value.strip())+" "+date_str+"\n"
            # print(influx_line)
            data_target.write(influx_line)
            c_index=c_index+1
        if(index%1000==0):
            print(index)
        index=index+1
data_target.close()
data_src.close()
