import tqdm
import time

def bfs(sc, df, b, file, iters=3):
    t1 = time.time()
    visited = set([b])
    queue = sc.broadcast(set([b]))

    distance = {}

    for iter in range(iters):
        if queue is not None:
            tmp = set(df.filter(lambda x: x[0] in queue.value).map(lambda x: x[1]).reduce(lambda x,y: x+y))
            
            tmp -= visited
            visited |= tmp

            queue = sc.broadcast(tmp)

            distance[iter+1] = tmp
    
    t = time.time() - t1
    with open(file, 'w+') as out:
        for k, v in distance.items():
            print(k, *v, sep=',', file=out)
    
    return t

def pbfs(df, b, file, iters=3):
    
    t1 = time.time()
    
    dist = df.map(lambda x: (x[0], 0 if x[0] == b else 2*iters))

    for iter in range(iters+1):
        m1 = df.join(dist)
        m2 = m1.flatMap(lambda x: [(a, iter+1) for a in x[1][0]] if x[1][1] == iter else [])
        dist = dist.union(m2).reduceByKey(min)

    res = dist.filter(lambda x: x[1] <= iters).map(lambda x: (x[1], [x[0]])).reduceByKey(lambda x,y: x+y)

    t = time.time() - t1

    with open(file, 'w+') as out:
        for k, v in res.collect():
            print(k, *v, sep=',', file=out)
    
    # return df.join(dist).filter(lambda x: x[1][1] <= iters)
    return t

def pagerank(df, b, file, alpha=0.85, iters=5):
    ranks = df.map(lambda x: (x[0], 1.0) if x[0] == b else (x[0], 0.0))

    for _ in range(iters):
        m = df.rightOuterJoin(ranks).mapValues(lambda x: (x[0], x[1]) if x[0] is not None else ([], x[1]))
        contr = m.flatMap(lambda x: [(a, x[1][1]/(len(x[1][0])+1)) for a in [x[0]] + x[1][0]])
        cnt = contr.reduceByKey(lambda x, y: x + y).count()
        ranks = contr.reduceByKey(lambda x, y: x + y).mapValues(lambda x: (1 - alpha) / cnt + alpha * x)

    m = df.rightOuterJoin(ranks)
    with open(file, 'w+') as out:
        for k, v in m.collect():
            if v[0] is not None:
                print(k, v[1], *v[0], sep=',', file=out)
            else:
                print(k, v[1], sep=',', file=out)


# def floyd_spark(df, idx):
#     def update(prev, edges, artist):
#         if artist in prev.keys():
#             for dst, cost in edges.items():
#                 if dst in prev.keys():
#                     prev[dst] = min(prev[dst], prev[artist] + cost)
#                 else:
#                     prev[dst] = prev[artist] + cost
#         return prev

#     m = df.rdd.map(lambda row: (row[idx], {sim: 1 for sim in row[idx+1:]}))
#     artists = set(df.rdd.map(lambda row: row[idx]).collect())

#     for iter, artist in enumerate(tqdm(artists)):
#         edges = dict(m.filter(lambda x: x[0] == artist) \
#                 .map(lambda x: list(x[1].items()))\
#                 .reduce(lambda x,y: x + y))
                    
#         m = m.map(lambda row: (row[0], update(row[1], edges, artist))).cache()
