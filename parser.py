from flask import Flask, request,jsonify
from flask_restful import Resource, Api, reqparse
import sqlparse
import re
import json
import time
import os
import subprocess

app = Flask(__name__)
api = Api(app)
app.config['SECRET_KEY'] = 'hello'

query = ""
ps = reqparse.RequestParser()
ps.add_argument('query')
def write_to_file(data):
	fil=open("/home/aurav/code/python/hadoop/pro/mapperip.txt","w")
	json.dump(data,fil)
#	fil.wrtie("\n");
	fil.close()
	return data
def re_print_header(s,h,g):
	if(h=="*"):
		if( s==""):
			return g
		else:
			return "{}    {}".format(g,s)
	else:
		if(s==""):
			return "{}    {}".format(g,h)
		else:
			return "{}    {}   {}".format(g,s,h)
	
def ma_print_header(data,table):
	fi=open("/home/aurav/code/python/hadoop/pro/data/{}.txt".format(table))
	sel=data['columns']
	agg_col=data['group']
	se=[]
	hav=data['agg']
	selc=[]
	for x in sel:
		if hav not in x:
			if x not in agg_col:
				se.append(x)
	if("*" in se):
		for v in line.keys():
			selc.append(v);
	else:
		selc=se
	
	return "{}###{}     {}".format(",".join(selc),hav, ",".join(agg_col))

def heade(data,table):
	x=ma_print_header(data,table)
	line = x.strip()
	line = line.split('     ')
	lin=line[0].split('###')
	s=lin[0]
	h=lin[1]
	g=line[1]
	re1={"select columns" :s.replace('\t',"   ")," having/aggrigation column ": h.replace('\t',"   "),"  group column": g.replace('\t',"   ")}
	y=re_print_header(s,h,g)
	line=y.strip()
	line=line.split('    ')
	re2={}
	if len(line)==1:
		re2={"group by columns" :line[0].replace('\t',"   "),   "select column ":line[1].replace('\t',"   ")}
	else:
		re2={"reducer result groupby columns" :line[0].replace('\t',"   ")}
	return re1,re2
	
def extractlhs_and_rhs(exp):
	al_op = ['<=', '==', '>=', '>', '<','!=',"="]
	for o in al_op:
		if exp.find(o)!=-1:
			lhs,rhs=exp.split(o)
			return lhs.rstrip().lstrip(),o,rhs.rstrip().lstrip()
			
			
def extractfunccol(lhs):
	agg=["SUM","COUNT","MAX","MIN","AVG"]
	hav=None
	col=None
	for y in agg:
		if y in lhs:
			a,b=re.split(y,lhs)
			e,f=re.split("\(",b)
			c,d=re.split("\)",f)
			return y,c
def spark_out():
	path=subprocess.check_output("ls /home/aurav/code/python/hadoop/pro/*.csv/*.csv", shell=True).splitlines()[0]
	#print(path)
	fi=open(path,"r")
	res=[]
	for line in fi:
		res.append(line.rstrip().replace('\t','    ').lstrip())
		
	return 	res
def spark():
	s=time.time()
	command="/home/aurav/hadoop-3.3.0/spark-3.0.1-bin-hadoop3.2/bin/spark-submit /home/aurav/code/python/hadoop/pro/spark_run.py"
	os.system(command)
	
	#fi=open("/home/aurav/code/python/hadoop/pro/sparkresult.csv","r")
	#res=""
	#for li in fi:
	#	res.append(li)
	#return res
	return time.time()-s

def cmd_output():
	cat = subprocess.Popen(["/home/aurav/hadoop-3.3.0/bin/hadoop", "fs", "-cat", "/user/hadoop/out/part-00000"], stdout=subprocess.PIPE)
	res=[]
	for line in cat.stdout:
		t =  line.decode('utf-8')
		x=str(t).replace("\t", "    ").rstrip().replace("\n","")
		#temp_dict = dict(zip(col_names, line.decode('utf-8').split('\n')[0].replace(",", "\t").split('\t')))	
		#print(temp_dict)
		res.append(x);
	cmd="/home/aurav/hadoop-3.3.0/bin/hdfs dfs -rm /user/hadoop/out/*"
	os.system(cmd)
	cmd="/home/aurav/hadoop-3.3.0/bin/hdfs dfs -rmdir /user/hadoop/out"
	os.system(cmd)
	return res

def spark_trans():
	fi=open("/home/aurav/code/python/hadoop/pro/spark_transformations.txt","r")
	li=fi.readline()
	res=json.loads(li)
	return 	res

def run_cmd(table):
	#command = "/home/aurav/hadoop-3.3.0/bin/hadoop jar /home/aurav/hadoop-3.3.0/share/hadoop/tools/lib/hadoop-streaming-3.3.0.jar -file ~/code/python/hadoop/pro/mapper.py    -mapper  ~/code/python/hadoop/pro/mapper.py -file  ~/code/python/hadoop/pro/reducer.py -reducer  ~/code/python/hadoop/pro/reducer.py -input /user/hadoop/{ip}.txt -output /user/hadoop/out".format(ip=table)
	#print(command)
	command = "/home/aurav/hadoop-3.3.0/bin/hadoop jar /home/aurav/hadoop-3.3.0/share/hadoop/tools/lib/hadoop-streaming-3.3.0.jar -mapper ~/code/python/hadoop/pro/mapper.py -reducer ~/code/python/hadoop/pro/reducer.py -input /user/hadoop/{ip}.txt -output /user/hadoop/out".format(ip=table)
	start= time.time()
	os.system(command)
	time_delta = time.time() - start
	#print(res)
	return time_delta
#	return "yes"
	
def parse_query(query):
	parsed=sqlparse.parse(query.upper().rstrip().lstrip())
	stmt=parsed[0]
	columns=stmt.tokens[2]
	tables=stmt.tokens[6]
	where=stmt.tokens[8]
	group=stmt.tokens[11]
	compar=stmt.tokens[-1]
	where=str(where)
	#print(where)
	wh,where=where.split()
	if(where=="*"):
		wlhs="*"
		wo="*"
		wrhs="*"
	else:
		#print(where)
		wlhs,wo,wrhs=extractlhs_and_rhs(str(where))
		#print(lhs,o,rhs)
	columns=str(columns)
	columns=columns.split(",")
	column=[]
	#print(compar)
	compar=str(compar)
	if(compar=="*"):
		func="*"
		o="*"
		rhs="*"
		agg="*"
	else:
		#print(where)
		#wlhs,wo,wrhs=extractlhs_and_rhs(str(where))
		#print(lhs,o,rhs)
		lhs,o,rhs=extractlhs_and_rhs(compar)
		func,agg=extractfunccol(lhs)
	for co in columns:
		co=co.lstrip().rstrip()
		if '('in co:
			x,h=re.split('\(',co)
			h,z=re.split('\)',h)
			if(agg=='*' and func=='*'):
				agg=h
				func=x
			elif (agg==h and func==x):
				continue
			else:
				return "wrong query two agg func used"
		else: 
			column.append(co)
	gr=[]
	for g in str(group).split(','):
		gr.append(g.rstrip().lstrip())
	data={"columns":column, "tables":str(tables).lstrip().rstrip(), "wlhs":wlhs, "wo":wo, "wrhs":wrhs, "group":gr, "func":func, "agg":agg, "op":o, "rhs":rhs}
	write_to_file( data)
	table=str(tables).lstrip().rstrip().lower()
	
	# yaha see
	# karna hai
	# 
	# 
	
	
	t1=run_cmd(table)
	t2=spark()
	res1=cmd_output()
	res2=spark_out()
	restr1=spark_trans()
	restr2,restr3=heade(data,table)
	ret={"mapper : ":restr2,"reducer:  ":restr3}
	return jsonify({"mapper reducer time":t1,"spark time" :t2,"mapper reducer output":res1,"spark output":res2," spark transformations" :restr1,"mapper reducer intermediate outputs":ret})
	
	#return data
class RunQuery(Resource):
	def get(self):
		return("use post method")
	def post(self):
		args = ps.parse_args()
		query = args['query']
		return parse_query(query)
api.add_resource(RunQuery, '/query')
app.run(debug=True)
