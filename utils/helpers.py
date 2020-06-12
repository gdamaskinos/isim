"""
  Copyright (c) 2020 Georgios Damaskinos
  All rights reserved.
  @author Georgios Damaskinos <georgios.damaskinos@gmail.com>
  This source code is licensed under the MIT license found in the
  LICENSE file in the root directory of this source tree.
"""

"""Various helpers for the backend
Some are left for reference and not currently in use"""

import numpy as np
import math
from pyspark import AccumulatorParam

def adjustedCosine(i, j, Pij, Qi, Qj):
    '''The adjusted cosine similarity between two items i,j
    '''
    numerator = Pij
    denominator = np.sqrt(Qi) * np.sqrt(Qj)
    if denominator:
        similarity = numerator / (float(denominator))
    else:
        similarity = 0.0

    similarity = round(similarity, 12) # numerical errors due to sqrt
    if similarity > 1: similarity = 1
    if similarity < -1: similarity = -1

    return similarity

def f(t, tr, alpha):
    '''f_{ui}^a = e^{ - \alpha*(t- t_{ui})}'''
    try:
        res = math.exp(-alpha * (t - tr))
    except OverflowError:
        raise ValueError("Overflow for t=%s\ttr=%s" % (t, tr))

    return res


def maxWeight(item1, item2):
    '''Get item with maximum weight (bigger timestamp)
    item: (iid, rating, timestamp)
    '''
    return item1 if item1[2] > item2[2] else item2


def topRecom(user_id, user_profile, topK, popular, r):
    '''Calculate predictions and create recommendations'''

    rbar = 0
    seen_items = {} # already rated items
    for value in user_profile:
        iid = value[0]
        rating = value[1]
        seen_items[iid] = rating
        rbar += rating

    rbar = rbar / len(seen_items.keys())

    predictions = [] # list for predictions
    for (n, i) in popular:
        if i in seen_items: # don't recommend already seen items
            continue

        if i in topK: # if i has neighbors
            pred = prediction(topK.get(i), seen_items, rbar)
            predictions.append([i, pred])

    # sort the scored items in ascending order
    predictions.sort(key = lambda x: x[1], reverse=True)

    return user_id, predictions[:r]

def prediction(neighbors, user_items, rbar):
    '''rating prediction for specific item by a specific user'''

    # Equation 3
    numer = 0
    denom = 0
    for (neighbor, sim) in neighbors:
        if neighbor in user_items:
            numer += sim * (user_items[neighbor] - rbar)
            denom += abs(sim)
    if denom:
        pred = rbar + numer / float(denom)
    else:
        pred = rbar

    return pred


class ListAccumulatorParam(AccumulatorParam):
    def zero(self, initialValue):
        return [0]*len(initialValue)

    def addInPlace(self, value1, value2):
        value1 += value2
        return value1

class FloatAccumulatorParam(AccumulatorParam):
    def zero(self, initialValue):
        return initialValue

    def addInPlace(self, value1, value2):
        value1 += value2
        return value1

def depth(seq):
    seq = iter(seq)
    try:
        for level in count():
            seq = chain([next(seq)], seq)
            seq = chain.from_iterable(s for s in seq if isinstance(s, Sequence))
    except StopIteration:
        return level

def combiner(x, y):
    '''Combines two values into one for reduceByKey'''
    if depth(x) == 1:
        x = [x]
    if depth(y) == 1:
        y = [y]
    return x + y

def replaceBrackets(x):
    '''Replace '[' brackets with '(' and return a string'''
    return str(list(x)).replace(']', ')').replace('[','(')

def updateRDD(name_rdd, name_temp, iteration):
    '''Update an RDD'''
    # checkpoint if needed
    if iteration % check == 0:
        eval(name_temp).checkpoint()
    # force rdd computation
    eval(name_temp).count
    # delete old rdd
    eval(name_rdd).unpersist()

def printRDD(name, addInfo = ''):
    print("%s %s RDD" % (addInfo, name))
    for line in eval(name).collect():
        print(line)
        print()


