#!/usr/bin/python
import sys,os
import base64
import re
import json
import time
import paramiko


from elasticsearch import Elasticsearch
from datetime import datetime

version="1.0.1"

nmon_directory="/tmp/nmon"
elastic_server="pvmlsup02:9200"
time_=""
date_=""
interval_=""
timestamp_=""
disks_per_line_=""
info=dict()
data=dict()
tokens=dict()
ZZZ=dict()
current_hostname=""
current_serial=""
time.time()


keys1="^DISK|NET|JFS|MEM|IO|FILE|PAGE|LPAR|LARGE|PROC|POOLS|NFS|CPU|SCPU|TOP|UAR"

es=Elasticsearch()

def daemonize():
    try:
        pid = os.fork()
        if pid > 0:
            # exit first parent
            sys.exit(0)
    except OSError as err:
        sys.stderr.write('_Fork #1 failed: {0}\n'.format(err))
        sys.exit(1)
    # decouple from parent environment
    os.chdir('/')
    os.setsid()
    os.umask(0)
    # do second fork
    try:
        pid = os.fork()
        if pid > 0:
            # exit from second parent
            sys.exit(0)
    except OSError as err:
        sys.stderr.write('_Fork #2 failed: {0}\n'.format(err))
        sys.exit(1)
    # redirect standard file descriptors
    sys.stdout.flush()
    sys.stderr.flush()
    si = open(os.devnull, 'r')
    so = open(os.devnull, 'w')
    se = open(os.devnull, 'w')
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

metrics=dict();
metric_data=dict()


if nmon_directory is None:
	print "Nmon files directory is not defined\n"
	exit(-1)

if elastic_server is None:
	print "Elastic server is not defined\n"
	exit(-2)

dirs=os.listdir(nmon_directory)
for file_name in dirs:
	file=nmon_directory + "/" + file_name
	fo=open(file)
	for line in fo:
		t=line.rstrip().split(",") 
		key=t[0]		
		if re.search("^AAA",line):
			if t[1] == 'disks_per_line':
				disks_per_line_=t[2]
			elif t[1] == 'timestampsize':
				timestamp_=t[2]
			elif t[1] == 'time':
				time_=t[2]
			elif t[1] == 'date':
				date_=t[2]
			elif t[1] == 'interval':
				interval_=t[2]
			elif re.match("run|Node|host|hardware|Serial|Machi|ker|LPAR|cpu|version|TL|AIX",t[1]):
				if re.match("NodeName",t[1]):
					current_hostname=t[2]
				elif re.match("SerialN",t[1]):
					current_serial=t[2]
				str="".join(t.pop())
				info[t[1]]=str
		if re.search("^BBB",line):
				str="".join(t.pop())
				datas=data.get(key)
				if datas is None:
					datas=dict()
					data[key]=datas;
				datas[t[1]]=str					
		elif len(t) > 3 and  re.match(keys1,line) and not re.match("\w+",t[1]):
			#print t
			token=t[0]
			key=t[1]
			pid=0
			if re.match("TOP",token) and re.match("\w+",t[1]):
				key=t[1]
				pid=t[2]
			nmondata=metrics.get('nmon2elk')
			if nmondata is None:
				nmondata=dict()
				metrics['nmon2elk']=nmondata
			ldata=nmondata.get(token)
			if re.match("TOP",token):
				t.remove(t[0])
				key.replace("+","")
			if ldata is None:
				ldata=[]
				nmondata[token]=ldata
			for name in t:
				name=name.replace("+PID","PID")
				ldata.append(name)
					
		       	print token,json.dumps(ldata)
			#	exit()
		elif len(t) > 5 and re.match("^TOP",key) and re.match("^\d+",t[1]):
			pid=t[1]
			key=t[2]
			nmon=metrics.get('nmon2elk')
			topdata=nmon.get(ltoken)
			keyvals=dict()
			if topdata is not None:
					i=1
					for attribut in topdata:
						value=t[i]
						keyvals[attribut]=value
						i+=1
			source=dict()
			timestamp=ZZZ[key]
			out=dict()
			out['@timestamp']=timestamp
                        out['version']=version
                        out['program']="nmon2elastic"
                        out['system']=source
			source["TOP"]=keyvals
			print json.dumps(out)
		elif re.match(keys1,line) and re.match("^T\d+",t[1]):
			ltoken=t[0]
			key=t[1]
			pid=0
			diskio=metrics.get('nmon2elk')
			tab=diskio.get(ltoken)
			if diskio is not None:
				metric_data=diskio.get(key)
				if metric_data is None:
					metric_data=dict()
					diskio[key]=metric_data
				metric_name=metric_data.get(ltoken)
				if metric_name is None:
					metric_name=dict()
					metric_data[ltoken]=metric_name
				idx=0
				t.remove(t[0])
				t.remove(t[0])
				out=dict()
				source=dict()
				timestamp=ZZZ[key]
				out['@timestamp']=timestamp
				out['version']=version
				out['program']="nmon2elastic"
				out['system']=source
				if re.match("CPU",ltoken):
					source['CPU']=metric_name
				elif re.match("DISK",ltoken):
					source['DISK']=metric_name
				elif re.match("LPAR",ltoken):
					source['LPAR']=metric_name
				elif re.match("PAGE",ltoken):
					source['SWAP']=metric_name
				elif re.match("LARGEPAGE",ltoken):
					source['LARGEPAGE']=metric_name
				elif re.match("JFS",ltoken):
					source['FILESYSTEMS']=metric_name
				elif re.match("PROC",ltoken):
					source['PROC']=metric_name
				elif re.match("MEM",ltoken):
					source['MEM']=metric_name
				elif re.match("NFS",ltoken):
					source['NFS']=metric_name
				elif re.match("NET",ltoken):
					source['NETWORKS']=metric_name
				elif re.match("TOP",ltoken):
					source['TOP']=metric_name
				else:
					source[ltoken]=metric_name
				if tab is not None:
					idx=0
					for value in tab:
						k=float(t[idx].lower())
						idx+=1
						if re.match("CPU|LCPU|SCPU",ltoken):
							metric_name['value']=k
							metric_name['attribut']=value.lower().replace("%","_pct")
							metric_name['name']=ltoken
						elif re.match("DISK",ltoken):
							metric_name['value']=k
							metric_name['attribut']=ltoken
							metric_name['name']=value.lower()
						elif re.match("LPAR",ltoken):
							metric_name['value']=k.lower()
							metric_name['attribut']=value.lower().replace('%','_pct')
							metric_name['name']=current_hostname
						elif re.match("PAGE",ltoken):
							metric_name['value']=k.lower()
							metric_name['attribut']=value.lower().replace('%','_pct')
							metric_name['name']=ltoken
						elif re.match("LARGEPAGE",ltoken):
							metric_name['value']=k.lower()
							metric_name['attribut']=value.lower().replace('%','_pct')
							metric_name['name']=ltoken
						elif re.match("JFS",ltoken):
							metric_name['value']=k.lower()
							metric_name['name']=ltoken
							metric_name['attribut']=ltoken
							metric_name['name']=value
						elif re.match("PROC",ltoken):
							metric_name['value']=k.lower()
							metric_name['attribut']=value
							metric_name['name']=ltoken
						elif re.match("NET",ltoken):
							x=value.split('-')
							metric_name['value']=k.lower()
							metric_name['key']=ltoken
							metric_name['attribut']=x[1]
							if len(x) > 2:
								metric_name['unit']=x[2]
							else:
								metric_name['unit']='count'
							metric_name['name']=x[0]
						elif re.match("UAR",ltoken):
							tab=diskio.get(ltoken)
							x=dict()
							i=0
							print t
							print tab
							if len(t) >= len(tab):
								for k in tab:
									value=t[i].lower()
									i+=1
									x[k]=float(value)
							#metric_name['PID']=x['PID']							        	
							metric_name['attributs']=x
						else:
							metric_name['value']=float(k.lower())
							metric_name['attribut']=value.lower().replace('%','_pct_')
							metric_name['name']=ltoken
							
						#print out
				#es.index(index=index_name.lower(),doc_type="nmon-json",body=json.dumps(out))

		if len(t) > 2 and re.search("T\d+",t[1]) and re.match("^ZZZ",t[0]):
			str_time=t[3] + " " + t[2]
			dt=datetime.strptime(str_time,"%d-%b-%Y %X");
			timestamp = timestamp = (dt - datetime(1970, 1, 1)).total_seconds()
			ZZZ[t[1]]=datetime.strftime(dt,"%Y-%m-%dT%H:%M:%S")
	metric_data	
	metrics.clear()
