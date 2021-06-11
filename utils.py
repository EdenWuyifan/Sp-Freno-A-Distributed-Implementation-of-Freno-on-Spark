from mpi4py import MPI
import numpy as np

from tree import Tree, TreeNode
import threading

NUM_WORKER = 8

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


def calc_minsup(i,db):
    return i / 100 * len(db)


def hashing(item):
    dest_rank = int(item) % NUM_WORKER + 1
    return dest_rank

class worker():
    def __init__(self, minsup):
        self._comm = MPI.COMM_WORLD
        self._rank = self._comm.Get_rank()
        self._size = self._comm.Get_size()
        self._tree = Tree(minsup)
        #threading.Thread(target=self.listening, daemon=True).start()
        #print("NODE created. rank is: %d" % self._rank)

    def hash(self, item):
        return hashing(item)

    def insert(self, trx):
        #if trx[0] not in self._tree._root._children:
        #    newNode = self._tree._addNode(self._tree._root, trx[0])
        self._tree.insert(self._tree._root,trx)
        #print("Worker NO.%d inserted. Current size: %d" % (self._rank, self._tree.size()))

    def send(self, trx):
        return

    def bcast_finish(self):
        return
        
    def listening(self):
        return 0


def runFreno():
    # for each worker: input (transactions line, minsup) and return minsup list


def distFreno(inFile, min_sup, sc, k):

    # Phase 1: generate Frequent items; produce vertical dataset

    # tranData is ans RDD, consider using zipwithUniqueId
    # TODO: add support for diffsets

    transDataFile = sc.textFile(inFile)
    numTrans = transDataFile.count()
    minsup = min_sup * numTrans
    # print("minsup", minsup)

    transDataIndex = transDataFile.zipWithIndex()
    transData = transDataIndex.map(lambda v: (v[1], v[0].split()))
    # print("transaction data with index:", transData.take(5))

    itemTids = transData.flatMap(lambda t: [(i, t[0]) for i in t[1]])\
        .groupByKey()\
        .map(lambda t: (t[0], list(t[1])))

    # print(itemTids.take(5))

    #freqItems = itemTids.filter(lambda t: len(t[1]) >= minsup)

    # TODO: phase 2 By dist-Eclat/bigFim, use dist-Apriori (data distribution) 
    # for k-itemsets generation

    # freqItemsCnt = freqItems.map(lambda t: (t[0], len(t[1])))
    #freqItemsCnt.saveAsTextFile("FrequentItems")
    
    #sort the RDD
    #freqItemTidsList = freqItems.sortByKey()
    freqItemTidsList = itemTids.sortByKey()
    # print(freqItemTidsList.collect())

    
    # use the configuration as the number of partitions
    # print("number of partitions used: {}".format(sc.defaultParallelism))
    # itemTidsParts = itemTids.repartition(sc.defaultParallelism).glom()
    # print(itemTidsParts.take(5))


    #phase 3: EClaT from k-itemsets
    freqItemsList = freqItemTidsList.collect()
    freqAtoms = freqItemTidsList.keys().map(lambda k: [k]).collect()
    # print("frequent items", freqAtoms)
    #freqRange = sc.parallelize(range(0, len(freqItemsList) - 1))
    freqRange = sc.parallelize(range(0, k-1))
    freqItemsListToRun = freqRange.map(\
        lambda t: (freqItemsList[t], freqItemsList[t+1:]))

    # print('freqItemsListToRun', freqItemsListToRun.take(5))

    #res = freqItemsListToRun.flatMap(lambda t: runEclat([[t[0][0]], t[0][1]], t[1], 2)).collect()
    res = freqItemsListToRun.flatMap(lambda t: runFreno([[t[0][0]], t[0][1]], t[1], 2)).collect()
    res = freqAtoms + res
    return res
        
