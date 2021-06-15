from pyspark import RDD, SparkConf, SparkContext
import os
import numpy as np
import math

from tree import Tree, TreeNode
import threading

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import time



def scanDB(path, seperation):
    db = []
    f = open(path, 'r')
    for line in f:
        if line:
            temp_list = line.rstrip().split(seperation)
            temp_list = [int(i) for i in temp_list]
            temp_list.sort()
            temp_list = [str(i) for i in temp_list]
            db.append(temp_list)
    f.close()
    return db

def runFreno(transactions, minsup):
    # for each worker: input (transactions line, minsup) and return minsup list
    
    tree = Tree(minsup)
    for trx in transactions:
        tree.insert(tree._root,trx)
    return tree.__repr__()

def distFreno(inFile, min_sup, sc, k):
    
    transDataRaw = scanDB(inFile, " ")
    numTrans = len(transDataRaw)
    #print(transDataRaw[:5])
    
    minsup = min_sup * numTrans
    #print("minsup", minsup)
    
    out_rdd = []
    for trx in transDataRaw:
        out_rdd.extend([trx[i:] for i in range(len(trx))])
    #print(out_rdd[:5])
    
    transDataFile = sc.parallelize(out_rdd)
    #print(transDataFile.count())
    
    transData = transDataFile.map(lambda v: (v[0], v))
    #print(transData.keys().take(5))
    transData = transData.map(lambda v: v[1])
    
    transData = transData.groupBy(lambda v: int(v[0])%k).map(lambda v : (v[0], list(v[1]))).collect()#.sortByKey()
    print(transData[2][1][:5])
    print(transData[3][1][:5])
    print(transData[4][1][:5])
    print(transData[5][1][:5])
    
    #print("transaction data num of keys:", transData.count())

    #transDataList = transData.collect()
    #print(transDataList[0])
    
    # use the configuration as the number of partitions
    print("number of partitions used: {}".format(sc.defaultParallelism))
    # print(itemTidsParts.take(5))

    #phase 3: Freno from k-itemsets
    freqRange = sc.parallelize(range(0, k)).repartition(sc.defaultParallelism * 4)
    freqItemsListToRun = freqRange.map(\
        lambda v: transData[v])

    #print('freqItemsListToRun', freqItemsListToRun.take(1)[0])
    
    res = freqItemsListToRun.map(lambda t: runFreno(t[1],minsup)).collect()
    return res
