import pyspark
from pyspark.sql import SparkSession

from bfs import bfs, pbfs
from pagerank import pagerank
from convert_to_csv import convert_to_csv
from floyd import floyd


convert_to_csv('song.avro', 'song.avsc', 'res.csv')
# floyd('res.csv', 'floyd.csv')

appName = 'spark_bfs'
master = 'local[2]'

ss = SparkSession.builder.appName(appName).master(master).getOrCreate()
sc = ss.sparkContext

df = ss.read.csv('file:///root/code/res.csv', inferSchema=True, header=False)

d1 = df.rdd.map(lambda x: (x[1], list(x[2:])))

t1 = bfs(sc, d1, 'ARWVVVP1187FB52F31', 'distance1.csv', 3)
t2 = pbfs(d1, 'ARWVVVP1187FB52F31', 'distance2.txt', 3)

with open("res.txt", 'w+') as out:
    print(f"Version 1 runs with {t1}", file=out)
    print(f"Version 2 runs with {t2}", file=out)