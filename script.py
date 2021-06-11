import EClaT
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import time


testFiles = ["T20I6D100K"]
support = [0.004]

conf = SparkConf().setAppName("")
sc = SparkContext(conf = conf)

spark = SparkSession(sc)
schema = StructType([
    StructField("algorithm", StringType(), False),
    StructField("datasets", StringType(), False),
    StructField("support", FloatType(), False)
])
for i in range(1):
    schema.add("test{}".format(i+1), FloatType(), True)
experiments = []

for f in testFiles:
    for s in support:
        times = []
        for i in range(1):
            start = time.time()
            res = EClaT.distEclat("./data/{}.data".format(f), s, sc)
            end = time.time()
            print(res)
            times.append(end - start)
        experiments.append(["EClaT", f, s] + times)
    df = spark.createDataFrame(experiments, schema)
    df.coalesce(1).write.mode("overwrite").csv("./experiments/runtime{}".format(f))
    experiments = []
sc.stop()
