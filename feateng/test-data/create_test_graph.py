import random as r

index = 0
NRows=1000*1000

f = open('graph.csv', 'w')
for i in range(1,NRows+1):
	f.write("%09d,pig-%d,%d,GAO123,%09d,5yrs\n" % (index,index,r.randint(1,100),r.randint(1,NRows)))
	index+=1
f.close()

