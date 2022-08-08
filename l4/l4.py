from pyspark import SparkContext, SparkConf

appName = "l4"
master = "local"

conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

file = "hdfs://VM-12-16-ubuntu:9000/user/ve472/input/scores.csv"
distFile = sc.textFile(file)
content = distFile.flatMap(lambda x: [tuple(x.split(",")[1:3])])
content = content.reduceByKey(lambda x, y: max(x, y))
content.collect()

