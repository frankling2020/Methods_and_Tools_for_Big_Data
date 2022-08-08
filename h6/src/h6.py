##########################################################################################
##                                    Import                                            ##
##########################################################################################

import math
import numpy as np


##########################################################################################
##                                     Utils                                            ##
##########################################################################################

def gradient(row, w):
    '''
        loss = y * log(p) + (1 - y) * log(1 - p)
        grad_l_p = y / p - (1 - y) / (1 - p)
        grad_l_z = y - p
        grad_l_w = (p - y) * x
    '''
    x = row[:-1]
    y = row[-1]
    pred = predict(x, w)

    return [(pred - y) * x_j for x_j in x]


def predict(x, w):
    return 1 / (1 + math.exp(-sum([x_i * w_i for x_i, w_i in zip(x, w)])))


def cum_gradients(x, y):
    return [x_j + y_j for x_j, y_j in zip(x, y)]


def hogwild_gradient(rdd_row, w, idx):
    x = rdd_row[:-1]
    y = rdd_row[-1]

    preds = predict(x, w)

    grad = (preds - y) * x[idx]

    return grad
    

def accurate(RDD, w):
    preds_pair = RDD.rdd.map(lambda row: (predict(row[:-1], w) > 0.5, row[-1]))
    rate = np.mean([y_true == pred for pred, y_true in preds_pair.collect()])

    return rate, preds_pair


def hessian(row, w):
    x = row[:-1]
    pred = predict(x, w)
    a = pred * (1 - pred)

    return [[a * x_j * x_i for x_j in x] for x_i in x]


def cum_hessian(x, y):
    return [cum_gradients(x_j, y_j) for x_j, y_j in zip(x, y)]


##########################################################################################
##                                     BGD                                              ##
##########################################################################################

class DistributedLRwithGD:
    def __init__(self, w, m):
        self.w = np.zeros(w, dtype=np.float32).tolist()
        self.m = m

    def validate(self, RDD):
        return accurate(RDD, self.w)[0]

    def forward(self, RDD, lr = 0.1, iters=100):

        for _ in range(iters):
            rdd_grads = RDD.rdd.map(lambda row: gradient(row, self.w)) \
                .reduce(lambda x, y: cum_gradients(x, y))

            self.w = [w_j - lr * grads_j / self.m for w_j, grads_j in zip(self.w, rdd_grads)]

        

##########################################################################################
##                                      SGD                                             ##
##########################################################################################

class DistributedLRwithSGD:
    def __init__(self, w, m, ratio):
        self.w = np.zeros(w, dtype=np.float32).tolist()
        self.m = m
        self.ratio = ratio

    def validate(self, RDD):
        return accurate(RDD, self.w)[0]

    def forward(self, RDD, lr = 0.1, iters=100):     

        for _ in range(iters):
            rdd_grads = RDD.rdd.sample(False, self.ratio) \
                .map(lambda row: gradient(row, self.w)) \
                .reduce(lambda x, y: cum_gradients(x, y))

            self.w = [w_j - lr * grads_j / self.m for w_j, grads_j in zip(self.w, rdd_grads)]
        
        


##########################################################################################
##                                SGD using Hogwild!                                    ##
##########################################################################################

class DistributedLRwithSGDHogwild:
    def __init__(self, w, m, ratio):
        self.len = w
        self.w = np.zeros(w, dtype=np.float32).tolist()
        self.m = m
        self.ratio = ratio

    def validate(self, RDD):
        return accurate(RDD, self.w)[0]

    def forward(self, RDD, lr = 0.1, iters=100): 

        for _ in range(iters):
            idx = np.random.randint(self.len)

            rdd_grad = RDD.rdd.sample(False, self.ratio) \
                .map(lambda row: hogwild_gradient(row, self.w, idx)) \
                .reduce(lambda x,y: x+y)

            self.w[idx] -= lr * rdd_grad / self.m

        
        

##########################################################################################
##                              BGD with Broadcast                                      ##
##########################################################################################

class BroadcastLRwithGD:
    def __init__(self, w, m):
        self.w = np.zeros(w, dtype=np.float32).tolist()
        self.m = m

    def validate(self, RDD):
        return accurate(RDD, self.w.value)[0]

    def forward(self, sc, RDD, lr = 0.1, iters=100):
        self.w = sc.broadcast(self.w)

        for _ in range(iters):
            rdd_grads = RDD.rdd.map(lambda row: gradient(row, self.w.value)) \
                .reduce(lambda x, y: cum_gradients(x, y))
            
            w = [w_j - lr * grads_j / self.m for w_j, grads_j in zip(self.w.value, rdd_grads)]

            self.w = sc.broadcast(w)


##########################################################################################
##                           Steepest Gradient Descent                                  ##
##########################################################################################

class LRwithSteepGD:
    def __init__(self, w, m):
        self.w = np.zeros(w, dtype=np.float32).tolist()
        self.m = m

    def validate(self, RDD):
        return accurate(RDD, self.w)[0]

    def forward(self, RDD, iters=10):

        for _ in range(iters):
            rdd_grads = np.array(
                RDD.rdd.map(lambda row: gradient(row, self.w)) \
                    .reduce(lambda x, y: cum_gradients(x, y))
            )

            rdd_hessian = np.array(
                RDD.rdd.map(lambda row: hessian(row, self.w)) \
                    .reduce(lambda x, y: cum_hessian(x, y))
            )

            direction = np.linalg.inv(rdd_hessian) @ rdd_grads.reshape(-1, 1)

            self.w = [w_j - grads_j / self.m for w_j, grads_j in zip(self.w, direction)]


##########################################################################################
##                           Steepest Gradient Descent                                  ##
##########################################################################################

class BroadcastLRwithSteepGD:
    def __init__(self, w, m):
        self.w = np.zeros(w, dtype=np.float32).tolist()
        self.m = m

    def validate(self, RDD):
        return accurate(RDD, self.w.value)[0]

    def forward(self, sc, RDD, iters=10):
        self.w = sc.broadcast(self.w)

        for _ in range(iters):
            rdd_grads = np.array(
                RDD.rdd.map(lambda row: gradient(row, self.w.value)) \
                    .reduce(lambda x, y: cum_gradients(x, y))
            )

            rdd_hessian = np.array(
                RDD.rdd.map(lambda row: hessian(row, self.w.value)) \
                    .reduce(lambda x, y: cum_hessian(x, y))
            )

            direction = np.linalg.inv(rdd_hessian) @ rdd_grads.reshape(-1, 1)

            w = [w_j - grads_j / self.m for w_j, grads_j in zip(self.w.value, direction)]

            self.w = sc.broadcast(w)