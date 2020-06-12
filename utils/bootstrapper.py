"""
  Copyright (c) 2020 Georgios Damaskinos
  All rights reserved.
  @author Georgios Damaskinos <georgios.damaskinos@gmail.com>
  This source code is licensed under the MIT license found in the
  LICENSE file in the root directory of this source tree.
"""

"""SwIFT bootstrap helper"""

from pyspark import StorageLevel
import os
from operator import add
import math
import heapq
import numpy as np
import ast
import gc

from helpers import adjustedCosine, maxWeight, f, topRecom, prediction

def defineTables(conf, keyspace):
    """Define Cassandra tables. Equivalent of the following commands:
Cassandra:
TRUNCATE userprofiles ; TRUNCATE iteminfo ; TRUNCATE itempairinfo ; TRUNCATE topk; TRUNCATE topr;
CREATE KEYSPACE mykeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}; USE mykeyspace;
CREATE TABLE userprofiles (user_id int PRIMARY KEY, profile list<FROZEN<list<float>>>);
CREATE TABLE iteminfo (item_id int PRIMARY KEY, li float, ni int, qi float);
CREATE TABLE itempairinfo (itemi int, itemj int, lij_i float, lij_j float, pij float, PRIMARY KEY (itemi, itemj));
CREATE TABLE topk (item_id int PRIMARY KEY, neighbors list<FROZEN<list<float>>>);
CREATE TABLE topr (user_id int PRIMARY KEY, recommendations list<FROZEN<list<float>>>);
"""
    host = conf.get("spark.cassandra.connection.host")
    cmd = ("cqlsh " + host + " --request-timeout 120 -e \" drop KEYSPACE IF EXISTS " + keyspace + "; "
       "CREATE KEYSPACE mykeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}; USE  " + keyspace + ";"
    "CREATE TABLE userprofiles (user_id int PRIMARY KEY, profile list<FROZEN<list<float>>>); "
    "CREATE TABLE iteminfo (item_id int PRIMARY KEY, li float, ni int, qi float);"
    "CREATE TABLE itempairinfo (itemi int, itemj int, lij_i float, lij_j float, pij float, PRIMARY KEY (itemi, itemj));"
    "CREATE TABLE topk (item_id int PRIMARY KEY, neighbors list<FROZEN<list<float>>>);"
    "CREATE TABLE topr (user_id int PRIMARY KEY, recommendations list<FROZEN<list<float>>>);\"")
    os.system(cmd)

def truncateTables():
    '''Clear cassandra tables'''
    print("Truncating tables...")
    host = conf.get("spark.cassandra.connection.host")
    cmd = ("cqlsh " + host + " -e \" TRUNCATE " + keyspace + ".userprofiles; "
     "TRUNCATE " + keyspace + ".iteminfo; "
     "TRUNCATE " + keyspace + ".itempairinfo; "
     "TRUNCATE " + keyspace + ".topk; "
     "TRUNCATE " + keyspace + ".topr;\"")

    os.system(cmd)


def createTopR(R, batch_size, popularSize, sc, keyspace):

    UserProfiles = sc.cassandraTable(keyspace, "userprofiles") \
          .select("user_id", "profile").map(lambda p: (p.user_id,
                                                         p.profile))

    ItemInfo = sc.cassandraTable(keyspace, "iteminfo") \
          .select("item_id", "li", "ni", "qi").map(
            lambda p: (p.item_id, p.li, p.ni, p.qi))

    topK = sc.cassandraTable(keyspace, "topk") \
          .select("item_id", "neighbors").map(lambda p: (p.item_id,
                                                         p.neighbors))

    """Obtain the k+1 most popular items for creating topK neighbors for
    new_items. k+1 is to address the case that the new_item get in the most
    popular items.
    popularHeap:
        [(n1, id1), (n2, id2) ...]
        n1 >= n2 >= ...
    """
    popularHeap = ItemInfo.map(lambda p: (p[2], p[0])) \
            .takeOrdered(popularSize, key = lambda x: -x[0])
    heapq.heapify(popularHeap)


    topKd = {}
    for (item, data) in topK.collect():
        topKd[item] = data

    topKb = sc.broadcast(topKd)

    """Obtain the top-R recommendations for every user
        topR:
        user -> [ [recomm1, pred1], ... [recommR, predR] ]
    """

    # obtain R+batch recom to provide R recom in every iteration
    topR = UserProfiles.map(lambda p: topRecom(
        p[0], p[1], topKb.value, popularHeap, R + batch_size)) \
                .setName('topr').persist(StorageLevel.DISK_ONLY)

    topR.saveToCassandra(keyspace, "topr")
    print("topR created")

    UserProfiles.unpersist()
    topK.unpersist()
    topR.unpersist()

    topKb.unpersist()
    del topKb
    gc.collect()

    return popularHeap


def loadFromFiles(loadDir, popularSize, k, R,
                  sc, conf, keyspace, part):
    """Load RDDs from files"""
    print("Loading datasets from " + loadDir + " to Cassandra...")
    defineTables(conf, keyspace)
    print("Defined Cassandra Tables")
    # construct the UserProfiles rdd and use it for deriving the topR
    UserProfiles = sc.textFile(loadDir + "UserProfiles") \
            .map(lambda p: ast.literal_eval(p)) \
            .repartition(part).persist(StorageLevel.DISK_ONLY)
    UserProfiles.saveToCassandra(keyspace, "userprofiles")
    print("UserProfiles created")

    sc.textFile(loadDir + "ItemInfo") \
            .map(lambda p: ast.literal_eval(p)) \
            .saveToCassandra(keyspace, "iteminfo")
    print("ItemInfo created")

    sc.textFile(loadDir + "cross_product") \
            .map(lambda p: ast.literal_eval(p)) \
            .saveToCassandra(keyspace, "itempairinfo")
    print("ItemPairInfo initialized")

    sc.textFile(loadDir + "ItemPairInfo") \
            .map(lambda p: ast.literal_eval(p)) \
            .saveToCassandra(keyspace, "itempairinfo")
    print("ItemPairInfo created")

    # construct the topK rdd and use it for deriving the topR
    topK = sc.textFile(loadDir + "topK") \
            .map(lambda p: ast.literal_eval(p)) \
            .map(lambda p: (p[0], p[1][:k])) \
            .repartition(part).persist(StorageLevel.DISK_ONLY)
    topK.saveToCassandra(keyspace, "topk")
    print("topK created")


    topK.unpersist()
    UserProfiles.unpersist()

    return


def calcUserInfo(ratings):
    '''Calculate current timestep, average_rating and e for the user'''

    s=0.0
    lastItemTimestep = -1
    for rating in ratings:
        s += rating[1]
        if rating[2] > lastItemTimestep:
            lastItemTimestep = rating[2]
            lastItemRating = rating[1]

    rubar_t = s / float(len(ratings))
    if len(ratings) == 1: # only one rating exists
        rubar_t1 = 0
    else:
        rubar_t1 = (s - lastItemRating) / float(len(ratings) - 1)

    return lastItemTimestep, rubar_t, rubar_t - rubar_t1

def parseVectorUsers(line):
    '''Parse each line of the specified data file, assuming a "," delimiter.
    Line format: uid,iid,rating,timestamp
    Converts each rating to a float
    Converts each timestamp to an int
    Makes Uid the key
    '''
    line = line.split(",")
    return int(line[0]), [[int(line[1]), float(line[2]), int(line[3])]]

def eliminateDupl(items):
    '''Eliminate duplicates among multiple ratings
    by keeping the one with maximum weight
    '''
    dic = {}
    for item in items:
        if item[0] in dic:
            dic[item[0]] = maxWeight(dic[item[0]], item)
        else: dic[item[0]] = item

    return list(dic.values())

def timeStepCalc(items):
    '''Convert timestamp to timestep'''
    step = 0
    new_items = []
    for item in sorted(items, key = lambda x: x[2]):
        step += 1
        new_items.append([item[0], item[1], step])

    return new_items

def emitNewRatings(user, items):
    for item in items:
        iid = item[0]
        rating = item[1]
        timestep = item[2]
        yield (iid, [[user, rating, timestep]])


def calcItemInfo(item, user_ratings, users_info, alpha):
    '''Qi, Lii(i)  for adjusted cosine'''

    Q = 0
    L = 0
    n = 0 # item popularity
    for rating in user_ratings:
        user = rating[0]
        ri = rating[1]
        ti = rating[2]
        t, avg, e = users_info[user]
        Q += (f(t, ti, alpha) * (ri - avg))**2
        L += f(t, ti, alpha)**2 * (ri - avg) * e
        n += 1

    return item, L, n, Q

def getCommon(a, b):
    '''Find values with the same key in the input lists
    a: [..., (keya, vala), ... (keyx, valy)]
    b: [..., (keyb, valb), ... (keyx, valz)]
    res: [..., (keyx, (valy, valz))]
    '''
    res = []
    # create dictionaries
    dicta = {}
    for user, rating, timestep in a:
        dicta[user] = (rating, timestep)
    dictb = {}
    for user, rating, timestep in b:
        dictb[user] = (rating, timestep)

    for user in dicta.keys():
        if user in dictb:
            res.append([user, dicta[user], dictb[user]])

    return res


def calcItemPairInfo(itemi, itemj, rating_pairs, user_info, alpha):
    '''For each i-j item pair, return Pij, Lij(i), Lij(j)'''

    Pij = 0
    Lij_i = 0
    Lij_j = 0

    for (user, i, j) in rating_pairs:
        t, avg, e = user_info[user]
        ri = np.float(i[0])
        ti = i[1]

        rj = np.float(j[0])
        tj = j[1]

        Pij += f(t, ti, alpha) * (ri - avg) * f(t, tj, alpha) * (rj - avg)
        Lij_i += f(t, ti, alpha) * f(t, tj, alpha) * (ri - avg) * e
        Lij_j += f(t, ti, alpha) * f(t, tj, alpha) * (rj - avg) * e

    return itemi, itemj, Lij_i, Lij_j, Pij

def keyOnFirstItem(item_pair, item_sim_data):
    '''For each item-item pair, make the smaller item's id the key'''
    (item1_id, item2_id) = item_pair
    # yield both ways for all items to be in the item_sims
    yield item1_id, [[item2_id, item_sim_data]]
    yield item2_id, [[item1_id, item_sim_data]]

def nearestNeighbors(item_id, items_and_sims, k):
    '''Sort the predictions list by similarity and select the top-k neighbors
    '''
    items_and_sims = sorted(items_and_sims, key=lambda x: x[1], reverse=True)
    return item_id, items_and_sims[:k]


def createRDDs(trainingSetPath,
               popularSize, k, R, alpha,
               sc, conf, keyspace, part, storeDir):
    print("Creating RDDs...")
    defineTables(conf, keyspace)
    print("Defined Cassandra Tables")
    """Obtain RDD UserProfiles:
        user_id -> [[item_id_1, ratingForItem_1, timestep1],
                    [item_id_1, ratingForItem_1, timestep2],...
                    [item_id_2, ratingForItem_2, timestep1],
                    ...]
    """

    lines = sc.textFile(trainingSetPath, part)

    UserProfiles = lines.map(parseVectorUsers) \
            .reduceByKey(add, part) \
            .map(lambda p:(p[0], eliminateDupl(p[1]))) \
            .filter(lambda p: len(p[1])>0) \
            .map(lambda p: (p[0], timeStepCalc(p[1]))) \
            .setName('userprofiles') \
            .persist(StorageLevel.DISK_ONLY)

    UserProfiles.saveToCassandra(keyspace, "userprofiles")
    print("UserProfiles created")

    """Obtain ratings in the form:
        item_id -> [(user_id_1, rating1, timestep1), ...]
    """
    item_user_pairs =  UserProfiles \
            .flatMap(lambda p: emitNewRatings(p[0], p[1]), \
                    preservesPartitioning=True) \
            .reduceByKey(add, part) \
            .map(lambda p:(p[0], eliminateDupl(p[1]))) \
            .setName('itemprofiles') \
            .persist(StorageLevel.DISK_ONLY)

    # rdd with all items for choosing random neighbors + check if item is new
    #   it is needed because if an item is not in the topK rdd
    #   it will never get inside it
    allItems = item_user_pairs.keys() \
            .setName('allitems').persist(StorageLevel.DISK_ONLY)
    print("All Items size: %d" % allItems.count())
    print("All Users size: %d" % UserProfiles.count())

    """Obtain RDD userInfo:
        user_id -> (t, average_rating, e)
    """
    userInfo = UserProfiles.map(lambda p: (p[0], calcUserInfo(p[1])))

    """Convert userInfo to dictionary and broadcast it"""
    user_info = {}
    for (user, info) in userInfo.collect():
        user_info[user] = info

    uib = sc.broadcast(user_info)

    """Obtain RDD ItemInfo:
        item_i -> (Lii(i), ni, Qi)
    """
    ItemInfo = item_user_pairs.map(
            lambda p: calcItemInfo(p[0], p[1], uib.value, alpha)) \
                    .setName('iteminfo') \
                    .persist(StorageLevel.DISK_ONLY)

    # alphabetical ordering (item_id, li, qi)
    ItemInfo.saveToCassandra(keyspace, "iteminfo")

    print("ItemInfo created")

    """Convert ItemInfo to dictionary and broadcast it"""
    ItemInfod = {}
    for (item, L, n, Q) in ItemInfo.collect():
        ItemInfod[item] = (L, Q)

    iinfob = sc.broadcast(ItemInfod)


    """Obtain all item-item pair combos:  (all User ratings are max weight)
        pairwise_items:
        (item1, item2) -> [[user1, (item1_rating_byUser1, item2_rating_byUser1)],
                          [user2, (item1_rating_byUser2, item2_rating_byUser2)],
                             ...]
        ...
        (item2, item1) -> ...
        ...
    """
    cross_product = item_user_pairs.cartesian(item_user_pairs) \
            .filter(lambda p: p[0][0] < p[1][0]) \
            .map(lambda p: (p[0][0], p[1][0], getCommon(p[0][1], p[1][1]))) \
            .setName('cross_product').persist(StorageLevel.DISK_ONLY)

    keys = allItems.cartesian(allItems) \
            .filter(lambda p: p[0] < p[1]) \
            .repartition(part*part) \
            .persist(StorageLevel.DISK_ONLY)

    tempcross = keys.map(lambda p: (p[0], p[1], 0, 0, 0)) \
            .setName('tempcross').persist(StorageLevel.DISK_ONLY)

    tempcross.saveToCassandra(keyspace, "itempairinfo")

    print("Initialized item_pairs")

    """Obtain the needed information for similarity compute for each item pair
        ItemPairInfo:
        (item_i, item_j) -> (Lij(i), Lij(j), Pij)
            item_i < item_j
    """

    ItemPairInfo = cross_product.filter(lambda p: p[2] != []) \
            .map(lambda p: calcItemPairInfo(p[0], p[1], p[2], uib.value, alpha)) \
            .setName('itempairinfo') \
            .repartition(part*part)

    ItemPairInfo.persist(StorageLevel.DISK_ONLY)

    # alphabetical ordering (itemi, itemj, lij_i, lij_j, pij)
    ItemPairInfo.saveToCassandra(keyspace, "itempairinfo", batch_size=500000)
    print("ItemPairInfo created")

    """Obtain the top-k neighbors for every item
        topK:
        item -> [ [neighbor1, sim1], ... , [neighbork, simk]]
    """
    topK = ItemPairInfo.map(lambda p: (
        (p[0], p[1]), adjustedCosine(p[0], p[1], \
                p[4], ItemInfod[p[0]][1], ItemInfod[p[1]][1]))) \
                .flatMap(lambda p: keyOnFirstItem(p[0], p[1])) \
                .reduceByKey(add, part) \
                .map(lambda p: nearestNeighbors(p[0], p[1], k)) \
                .setName('topk').persist(StorageLevel.DISK_ONLY)

    topK.saveToCassandra(keyspace, "topk")
    print("topK created")

    uib.unpersist()
    iinfob.unpersist()

    del uib
    del iinfob

    if storeDir is not None:
        print("storing RDDs in " + storeDir + " ...")
        UserProfiles.coalesce(1).saveAsTextFile(storeDir + "UserProfiles")
        ItemInfo.coalesce(1).saveAsTextFile(storeDir + "ItemInfo")
        tempcross.coalesce(1).saveAsTextFile(storeDir + "cross_product")
        ItemPairInfo.coalesce(1).saveAsTextFile(storeDir + "ItemPairInfo")
        topK.coalesce(1).saveAsTextFile(storeDir + "topK")

    topK.unpersist()
    UserProfiles.unpersist()
    ItemPairInfo.unpersist()
    cross_product.unpersist()
    tempcross.unpersist()
    item_user_pairs.unpersist()
    ItemInfo.unpersist()

    return
