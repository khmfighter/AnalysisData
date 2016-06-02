#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os, sys, datetime, time, MySQLdb, warnings
from threading import Thread
warnings.filterwarnings("ignore")

def mysql_op(dict_val):
	conn = MySQLdb.connect (host = "192.168.30.23", user = "root", passwd = "1qaz@WSX", db = "stormMetric", charset = "utf8")
	cursor = conn.cursor ()
	cursor.execute ("""CREATE TABLE if not exists MetricResult(topic VARCHAR(100),datetime VARCHAR(40),total VARCHAR(100),emit VARCHAR(100),delynum VARCHAR(100),errornum VARCHAR(100),formatNum VARCHAR(100),totalsize VARCHAR(100), PRIMARY KEY (topic,datetime)) ENGINE=InnoDB  DEFAULT CHARSET=utf8""")
	for k,v in dict_val.iteritems():
		line="\'"+(k+","+v).replace(',','\',\'')+"\'"
		#print line
		sql='insert into MetricResult (topic, datetime, total, emit, delynum, errornum, formatNum, totalsize) values(%s)'%(line)
		#print sql
		cursor.execute (sql)
	conn.commit()
	cursor.close ()
	conn.close ()


# 获取前一天时间（YYYY-MM-DD）
def get_Yesterday():
    dd = datetime.date.today() + datetime.timedelta(days=-1)
    return dd.strftime('%Y%m%d')

def get_Today():
    dd = datetime.date.today()
    return dd.strftime('%Y%m%d')

def ComputerContent(strdir):
	dictval = {}
	for parent, dirs, files in os.walk(strdir):
		for strfile in files:
			if (strfile.startswith("Result")):
				f = open(strdir + "/" + strfile, 'r')
				for line in f.readlines():
					s = line.strip('\n').split(':')
					key=s[0]+","+s[1][-12:]
					values=s[2].split('|')
					if len(values)==9:
						if(dictval.__contains__(key)):
							#curspeed = float(values[0])+float(dictval[key].split(',')[0])
							#avgspeed = float(values[1])+float(dictval[key].split(',')[1])
							total = float(values[2])+float(dictval[key].split(',')[0])
							emit = float(values[3])+float(dictval[key].split(',')[1])
							delynum = float(values[4])+float(dictval[key].split(',')[2])
							errornum = float(values[5])+float(dictval[key].split(',')[3])
							formatNum = float(values[6])+float(dictval[key].split(',')[4])
							totalsize = float(values[7])+float(dictval[key].split(',')[5])
							#avgmsgsize =  float(values[7])+float(dictval[key].split('#')[7])
							#value=curspeed+"#"+avgspeed+"#"+total+"#"+emit+"#"+delynum+"#"+errornum+"#"+totalsize+"#"+avgmsgsize
							value=str(total)+","+str(emit)+","+str(delynum)+","+str(errornum)+","+str(formatNum)+","+str(totalsize)
						else:
							#curspeed = float(values[0])
							#avgspeed = float(values[1])
							total = float(values[2])
							emit = float(values[3])
							delynum = float(values[4])
							errornum = float(values[5])
							formatNum = float(values[6])
							totalsize = float(values[7])
							#avgmsgsize =  float(values[7])
							#value=curspeed+"#"+avgspeed+"#"+total+"#"+emit+"#"+delynum+"#"+errornum+"#"+totalsize+"#"+avgmsgsize
							#value=str(curspeed)+","+str(avgspeed)+","+str(total)+","+str(emit)+","+str(delynum)+","+str(errornum)
							value=str(total)+","+str(emit)+","+str(delynum)+","+str(errornum)+","+str(formatNum)+","+str(totalsize)
						dictval[key]=value
				f.close()
	dicfile = open(strdir + "/" + 'Result.csv', 'w')
	dicfile.write("topic,datetime,total,emit,delynum,errornum,formatNum,totalsize"+'\n')
	for k,v in dictval.iteritems():
		dicfile.write(k+","+v+'\n')
	dicfile.close()
	mysql_op(dictval)

def cmd_execute(strcmd):
	os.system(strcmd)

if __name__ == '__main__':
	threads=[]
	strdir="/root/Metric"
	current_dir="/root/Collect/"
	computer_dir="/root/Collect"
	os.system("rm -rf /root/Collect/*")
	for i in range(1,21):
		if(i<10):
			host='ZJHZ-Storm0'+str(i)
		else:
			host='ZJHZ-Storm'+str(i)
		scpstr=host+":"+strdir+"/"+get_Today()+"_*"
		print scpstr
		#多线程拉取数据
		cmd="scp "+scpstr+" "+current_dir+"Get"+str(i)
		t=Thread(target=cmd_execute,args=(cmd,))
		t.start()
		threads.append(t)
	for t in threads:
		t.join()


	cmd1="cat "+current_dir+"*" +">>"+current_dir+"Result.info;rm -rf "+current_dir+"Get*"
	print cmd1
	os.system(cmd1)
	ComputerContent(computer_dir)