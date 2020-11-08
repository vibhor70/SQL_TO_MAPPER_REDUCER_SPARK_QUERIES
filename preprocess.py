import re
import json
elements=[]
def readelement(f,fcat,frev,fpro,fsim):
	idl=f.readline()
	z=re.search("Id:   ",idl)
	while(z!=None):
		idl=f.readline()
		z=re.search(":   ",idl)
	idl=f.readline()
	#print("{"+idl+"}")
	x,id=re.split(":   ",idl)
	#print(id.rstrip())
	ail=f.readline()
	#print(ail)
	x,amazonid=re.split(": ",ail)
	#print(amazonid.rstrip())
	tl=f.readline()
	check=re.search("discontinued product",tl)
	if(check!=None):
		
		element={"PID":id.rstrip(),"ASIN":amazonid.rstrip(),"TITLE":"discontinued product","GROUP":"0","SALESRANK":"0"}
		json.dump(element,fpro)
		fpro.write("\n")
		return
		#return element
	x,title=re.split(": ",tl,1)
	#print(title.rstrip())
	gl=f.readline()	
	x,group=re.split(": ",gl)
	#print(group.rstrip())
	sl=f.readline()
	x,srank=re.split(": ",sl)
	#print(srank.rstrip())
	sil=f.readline()
	x,sim=re.split(": ",sil)
	tok=sim.split()
	simc=tok[0];
	#print(simc)
	#similar=[]
	for i in range(int(simc)):
#		similar.append()
		z={"ASIN":amazonid.rstrip(),"PID":id.rstrip(),"SIMILAR":tok[i+1]}
		json.dump(z,fsim)
		fsim.write("\n")
	#print(similar.rstrip())
	cil=f.readline()	
	x,c=re.split(": ",cil)
	#print(c.rstrip())
	#categories=[]
	cnames=set()
	for i in range(int(c)):
		ce=f.readline()
		cats=ce.split("|")
		for a in cats:
			#print(a)
			if("["in a):
				#print(a)
				na,ci=a.rsplit("[",1)
				ci,x=re.split("\]",ci)
				cnames.add("{}####{}".format(ci,na))
			#print(na,ci,depth)
#			categories.append()
	for a in cnames:
		na,ci=a.split("####")
		z={"ASIN":amazonid.rstrip(),"PID":id.rstrip(),"CID":na,"CATNAME":ci}
		json.dump(z,fcat)
		fcat.write("\n")	
	#print(categories)
	cil=f.readline()	
	r,t,c,d,dc=re.split(": ",cil)
	#print(t)
	#print(c)
	#print("["+d+"]")
	#print(dc)
	#print(count)
	cound,download=re.split("  ",d);
	count,download=re.split("  ",c);
	#print(cound)
	#print(count)
	#if cound!=count:
	#	print("error")
	#	exit()
	#reviews=[]
	for i in range(int(cound)):
		rev=f.readline()
		ti,cus,ra,vo,he=re.split(": ",rev)
		time,x=ti.split();
		customer,x=cus.split();
		rating,x=ra.split()
		votes,x=vo.split();
		helpful=he.rstrip();
		#print(time,customer,rating,votes,helpful)
		
		z={"ASIN":amazonid.rstrip(),"TIME":time,"PID":id.rstrip(),"USERID":customer,"RATING":rating,"VOTES":votes,"HELPFUL":helpful}
		json.dump(z,frev)
		frev.write("\n")
	#print(reviews)
	#element={"PID":id.rstrip(),"ASIN":amazonid.rstrip(),"TITLE":title.rstrip(),"GROUP":group.rstrip(),"SALESRANK":srank.rstrip(),"SIMILAR":similar,"CATEGORIES":categories,"REVIEWS":reviews}
	element={"PID":id.rstrip(),"ASIN":amazonid.rstrip(),"TITLE":title.rstrip(),"GROUP":group.rstrip(),"SALESRANK":srank.rstrip()}
	json.dump(element,fpro)
	fpro.write("\n")
	#return element	
f=open("/home/aurav/Downloads/amazon-meta.txt","rt")

f.readline()
f.readline()
fcat=open("data/categories.txt","w")
frev=open("data/reviews.txt","w")
fpro=open("data/products.txt","w")
fsim=open("data/similar.txt","w")
for i in range(548551):
#for i in range(10):
	#f.readline()
	
	x=readelement(f,fcat,frev,fpro,fsim)
	#json.dump(x,write)
	#write.write("\n")
#f.readline()
#readelement(f)

