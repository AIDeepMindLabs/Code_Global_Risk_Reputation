import sys, getopt

with open('out_25may2_inm.csv') as f: # your input file goes here, the file must be pipe delimited
    all_lines = f.readlines()


all_lines_tmp = all_lines[:]
all_lines = []
for i in reversed(all_lines_tmp):
	all_lines.append(i)
#sys.exit(0)

'''
dict_stype = {}
dict_ptype = {}

sing_list = []
mult_list = []
hold_all_lines = all_lines[:]
counter_del = 0

for i in range(0, len(all_lines)-1):
	this_line = all_lines[i][0:-1]
	this_sessionid = this_line.split(",")[1]
	this_accountid = this_line.split(",")[2]

	next_line = all_lines[i+1][0:-1]
	next_sessionid = next_line.split(",")[1]
	next_accountid = next_line.split(",")[2]

	if this_accountid != next_accountid:
		sing_list.append(all_lines[i])
	else:
		mult_list.append(all_lines[i])

# process single transcations
#print "===singles"
for each_row in sing_list:
	row_split = each_row[0:-1].split(",")
	row_split[7] = 1.0
	#print row_split

double_list = []
triple_plus_list = []

for i in range(0, len(mult_list)-3):
	this_line = mult_list[i][0:-1]
	this_sessionid = this_line.split(",")[1]
	this_accountid = this_line.split(",")[2]

	next_line = mult_list[i+1][0:-1]
	next_sessionid = next_line.split(",")[1]
	next_accountid = next_line.split(",")[2]

	next2_line = mult_list[i+2][0:-1]
	next2_sessionid = next2_line.split(",")[1]
	next2_accountid = next2_line.split(",")[2]

	next3_line = mult_list[i+3][0:-1]
	next3_sessionid = next3_line.split(",")[1]
	next3_accountid = next3_line.split(",")[2]

	if this_accountid != next_accountid and next_accountid == next2_accountid and next2_accountid != next3_accountid: 
		double_list.append(mult_list[i+1])
		double_list.append(mult_list[i+2])		
	else:
		triple_plus_list.append(mult_list[i])

# process double transcations
#print "===doubles"
for i in range(0, len(double_list)-1, 2):

	first_row_split = double_list[i][0:-1].split(",")
	second_row_split = double_list[i+1][0:-1].split(",")

	first_row_split[7] = 1.0
	second_row_split[6] = float(second_row_split[6])*0.01*75
	second_row_split[7] = round(2.0+(float(second_row_split[7])/5.0),1)

	#print first_row_split
	#print second_row_split

#print "===triple plus"
#for i in range(0, len(triple_plus_list)-1):


	#print triple_plus_list[i][0:-1].split(",")

	first_row_split = double_list[i][0:-1].split(",")
	second_row_split = double_list[i+1][0:-1].split(",")

	first_row_split[7] = 1.0
	second_row_split[6] = float(second_row_split[6])*0.01*75
	second_row_split[7] = round(2.0+(float(second_row_split[7])/5.0),1)



	print first_row_split
	
	#print second_row_split
'''
i = 0

results_dump = []

while (i < (len(all_lines)-1)-1):

	this_line = all_lines[i][0:-1]
	this_sessionid = this_line.split(",")[1]
	this_accountid = this_line.split(",")[2]
	this_row_split = this_line.split(",")

	next_line = all_lines[i+1][0:-1]
	next_sessionid = next_line.split(",")[1]
	next_accountid = next_line.split(",")[2]
	next_row_split = next_line.split(",")

	next_line2 = all_lines[i+2][0:-1]
	next_sessionid2 = next_line2.split(",")[1]
	next_accountid2 = next_line2.split(",")[2]
	next_row_split2 = next_line2.split(",")

	#sys.exit(0)
	if (this_accountid != next_accountid and next_accountid != next_accountid2 and next_accountid2 != this_accountid):
		
		next_row_split[6] = 100.0
		next_row_split[7] = 1.0

		#next_row_split[6] = float(next_row_split[6])*0.01*75
		#next_row_split[7] = round(2.0+(float(next_row_split[7])/5.0),1) 

		#print ",".join(map(str, this_row_split))
		results_dump.append(",".join(map(str, next_row_split)))

		i += 1

	
	elif (this_accountid != next_accountid and next_accountid == next_accountid2):
		
		next_row_split[6] = 100.0
		next_row_split[7] = 1.0

		next_row_split2[6] = float(next_row_split2[6])*0.01*75
		next_row_split2[7] = round(2.0+(float(next_row_split2[7])/5.0),1) 

		#print ",".join(map(str, this_row_split))
		results_dump.append(",".join(map(str, next_row_split)))
		results_dump.append(",".join(map(str, next_row_split2)))


		i += 2

	else:
		results_dump.append(this_line)

		i += 1

clean_results_dump = []
clean_results_dump2 = []

for i in reversed(results_dump):
	clean_results_dump.append(i)

for i in range(0, len(clean_results_dump)-1):
	this_time = clean_results_dump[i].split(",")[0]
	next_time = clean_results_dump[i+1].split(",")[0]

	this_sessionid = clean_results_dump[i].split(",")[1]
	next_sessionid = clean_results_dump[i+1].split(",")[1]

	if this_time == next_time and this_sessionid == next_sessionid:
		pass
	else:
		print clean_results_dump[i]
