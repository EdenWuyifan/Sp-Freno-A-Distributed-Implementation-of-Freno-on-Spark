import sys
import os
import random
import argparse

parser = argparse.ArgumentParser(description='argparse')
parser.add_argument('--trx', '-t', help='num of trx', required=True, type=int)
parser.add_argument('--itm', '-i', help='num of items', required=True, type=int)
parser.add_argument('--avglen', '-avl', help='average length', required=False, default=30, type=int)
parser.add_argument('--agg', '-a', help='aggregation', required=False, default=5, type=int)
#parser.add_argument('--fre', '-f', help='frequent sets num', required=False, default=20, type=int)
args = parser.parse_args()


# Not random

NAME = str(args.trx)+"_"+str(args.itm)+".txt"
f = open(NAME, "w")

total_trx = args.trx

#Generate flat database
flat_list = [i for i in range(1,args.itm+1)]
random.shuffle(flat_list)
flat_list = [str(int) for int in flat_list]

flat_div = 20
total_trx -= flat_div
for i in range(flat_div):
    line = " ".join(flat_list[i*(args.itm//flat_div):(i+1)*args.itm//flat_div])
    line += "\n"
    f.write(line)

#50% dataset
trx_lst = []
curr_len = random.randint(args.avglen-args.agg,args.avglen+args.agg)
for j in range(curr_len):
    item = random.randint(1,args.itm)
    trx_lst.append(item)
for i in range(args.trx//2):
    if random.randint(0,100)>=100:
        trx_lst[random.randint(0,curr_len-1)] += 1
    temp_lst = [str(int) for int in trx_lst]
    temp_lst = list(set(temp_lst))
    line = " ".join(temp_lst)
    line += "\n"
    #print(line)
    f.write(line)
    total_trx -= 1

#33% dataset
trx_lst = []
curr_len = random.randint(args.avglen-args.agg,args.avglen+args.agg)
for j in range(curr_len):
    item = random.randint(1,args.itm)
    trx_lst.append(item)
for i in range(args.trx//3):
    if random.randint(0,100)>=100:
        trx_lst[random.randint(0,curr_len-1)] += 1
    temp_lst = [str(int) for int in trx_lst]
    temp_lst = list(set(temp_lst))
    line = " ".join(temp_lst)
    line += "\n"
    #print(line)
    f.write(line)
    total_trx -= 1

#random dataset
while total_trx != 0:
    trx_lst = []
    curr_len = random.randint(args.avglen-args.agg,args.avglen+args.agg)
    for j in range(curr_len):
        item = random.randint(1,args.itm)
        trx_lst.append(item)
    temp_lst = [str(int) for int in trx_lst]
    temp_lst = list(set(temp_lst))
    line = " ".join(temp_lst)
    line += "\n"
    #print(line)
    f.write(line)
    total_trx -= 1

f.close()
