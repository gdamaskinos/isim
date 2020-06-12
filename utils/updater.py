"""
  Copyright (c) 2020 Georgios Damaskinos
  All rights reserved.
  @author Georgios Damaskinos <georgios.damaskinos@gmail.com>
  This source code is licensed under the MIT license found in the
  LICENSE file in the root directory of this source tree.
"""

"""SwIFT updater helper"""

import gc
import math
from itertools import chain
import heapq
import time
import random

from pyspark import StorageLevel
from helpers import adjustedCosine, maxWeight, f, topRecom, replaceBrackets

def updateItemPairInfo(i, j, prevLi, prevLj, prevP, fresh_users, user_t,
        prev_ratings, fresh_ratings, rubar, user_e, alpha):
    '''Calculate the incrementaly update value of Pij, Lij(i), Lij(j)'''

    """calculate Pij, Lij(i), Lij(j)"""
    dP = 0
    dLi = 0
    dLj = 0

    for user in fresh_users:
        ru = rubar[user]
        e = user_e[user]
        t = user_t[user]
        # u\epsilon\DeltaU_i^t \cap u\epsilon\U_j^{t-1}
        if (user, i) in fresh_ratings and (user, j) in prev_ratings:
            isRated = True
            ri, ti = fresh_ratings[(user, i)]
            rj, tj = prev_ratings[(user, j)]

            dP += (ri - ru) * f(t, tj, alpha) * (rj - ru)
            dLi += f(t, tj, alpha) * (ri - ru) * e
            dLj += f(t, tj, alpha) * (rj - ru) * e

        # u\epsilon\DeltaU_j^t \cap u\epsilon\U_i^{t-1}
        if (user, j) in fresh_ratings and (user, i) in prev_ratings:
            isRated = True
            rj, tj = fresh_ratings[(user, j)]
            ri, ti = prev_ratings[(user, i)]

            dP +=  f(t, ti, alpha) * (ri - ru) * (rj - ru)
            dLi += f(t, ti, alpha) * (ri - ru) * e
            dLj += f(t, ti, alpha) * (rj - ru) * e

        # u\epsilon\DeltaU_i^t \cap u\epsilon\DeltaU_j^t
        if (user, i) in fresh_ratings and (user, j) in fresh_ratings:
            isRated = True
            ri, ti = fresh_ratings[(user, i)]
            rj, tj = fresh_ratings[(user, j)]

            dP += (ri - ru) * (rj - ru)
            dLi += (ri - ru) * e
            dLj += (rj - ru) * e


    Li = dLi + math.exp(-2*alpha) * prevLi
    Lj = dLj + math.exp(-2*alpha) * prevLj
    P = dP + math.exp(-2*alpha) * (prevP - prevLi - prevLj)

    return i, j, Li, Lj, P

def updateItemInfo(iid, prevL, prevn, prevQ, user_t,
        prev_ratings, fresh_ratings, rubar, user_e, alpha):
    '''Calculate the incrementaly update value of Qi and Lii(i)'''

    """calculate Q, L"""
    dL = 0
    dQ = 0
    dn = 0
    for (user, item) in fresh_ratings:
        if item == iid:  # u\epsilon\DeltaU_i^t
            rating, timestep = fresh_ratings[(user, item)]
            ru = rubar[user]
            e = user_e[user]
            t = user_t[user]

            dL += (rating - ru) * e
            dQ += (rating - ru)**2
            dn += 1

            if (user, item) in prev_ratings: # u\epsilon\U_i^{t-1}
                dL += 2 * f(t, timestep, alpha) * (rating - ru) * e

    L = dL + math.exp(-2*alpha) * prevL
    Q = dQ + math.exp(-2*alpha) * (prevQ - 2 * prevL)
    n = dn + prevn

    if Q < 0:
        Q = prevQ
        L = prevL

    return iid, L, n, Q

def updateItem(user, item, fresh_ratings):
    '''Update the item in user's profile'''
    # fresh rating exists
    # FIXME for timestep
    if (user, item[0]) in fresh_ratings:
        new_rating, new_timestep = fresh_ratings[(user, item[0])]
        new_item = (item[0], new_rating, new_timestep)
        return maxWeight(item, new_item)
    else:
        return item

def updateUserProfiles(user, items, fresh_ratings, containDuplicates):
    '''Obtain the new profile for the user'''

    new = items[:] # avoid non-mutable errors
    """update current user profile """
    if containDuplicates:
        temp = map(lambda item: updateItem(user, item, fresh_ratings), items)
        l = set(map(lambda item: item[0], temp))
    else:
        l = set()

    """append the new items rated """
    for (u, item) in fresh_ratings.keys():
        if u == user and item not in l:
            rating, timestep = fresh_ratings[(u, item)]
            new.append([item, rating, timestep])

    return user, new

def updateSim(i, j, itemQ, itemP, old_sims, top):
    try:
        Qi = itemQ[i]
        Qj = itemQ[j]
        if Qi == 0 or Qj == 0:
            raise KeyError
        if i<j:
            Pij = itemP[(i, j)]
        else:
            Pij = itemP[(j, i)]
    except KeyError:
        if j in old_sims:
            return old_sims[j] # return previous similarity
        else: # if i, j are not similar return 0
            return 0
    if Qi<0 or Qj<0:
        raise ValueError("Key item: %s\nNeighbor: %s\ntopK: %s\nQi: %s\nQj: %s\n" % (i, j, top,
            Qi, Qj))
        if j in old_sims:
            return old_sims[j]
        else:
            return 0.99
    return adjustedCosine(i, j, Pij, Qi, Qj)

def updateNeighbors(key_item, kneighbors, itemQ, itemP, candNeighbors, k):
    '''Update topK similarities for key_item's neighbors'''

    # get neighbors
    old_sims = {}
    if kneighbors:
        for neighbor, sim in kneighbors:
            old_sims[neighbor] = sim

    neighbors = set(old_sims.keys())

    # union with itemsUpdated and create candidate neighbors
    # with this way neighbors converge to good quality
    candidates = neighbors.union(candNeighbors - set([key_item]))

    newN = list(map(lambda neighbor: [neighbor, updateSim(
        key_item, neighbor, itemQ, itemP, old_sims, kneighbors)], candidates))

    newN.sort(key = lambda x: x[1], reverse=True)

    return key_item, newN[:k]


def update(fresh_ratings, allItems, k, alpha, R, popularHeap,
           containDuplicates, ignoreNew,
           sc, keyspace, part):

    start = time.time()

    popularIds = set(map(lambda x: x[1], popularHeap))
    '''update1: Obtain required info for the updates'''
    fresh_users = set() # users that rated in this micro-batch
    fresh_items = set() # items that have been rated in this micro-batch
    for u, iid in fresh_ratings.keys():
        fresh_users.add(u)
        fresh_items.add(iid)

    """Obtain previous info"""
    exist_users = set() # fresh users that have profile

    # dictionary for "fresh" users' profiles
    # (user, iid) -> (rating, timestep)
    prev_ratings = {}
    # dictionaries for  users' average rating
    # user -> (sum, size)
    prev_rubar = {}
    # FIXME !!!!! MAKE t global timestep and not for each profile
    user_t = {} # timeStep for fresh users
    # FIXME O(M) -> keep a window for user profiles
    profiles = sc.cassandraTable(keyspace, "userprofiles") \
            .select("user_id", "profile") \
            .where("user_id in " + replaceBrackets(fresh_users)) \
            .map(lambda p: (p.user_id, p.profile)).collect()

    for u, profile in profiles:
        exist_users.add(u)
        user_t[u] = len(profile) # set current user timestep
        for iid, rating, timestep in profile:
            prev_ratings[(u, iid)] = (rating, timestep)
            if u in prev_rubar: # user exists
                s, num = prev_rubar[u]
                prev_rubar[u] = (s + rating, num + 1)
            else:
                prev_rubar[u] = (rating, 1)

    """Obtain fresh info"""
    fresh_rubar = {}
    # fresh user profiles: ratings in this micro-batch
    # user -> [(iid1, rating1, timestamp1), ... ]
    fresh_user_profile = {}
    for u, iid in fresh_ratings.keys():
        rating, timestep = fresh_ratings[(u, iid)]
        # change temp timestep to timeStep
        if u in user_t: timestep += user_t[u]
        # update fresh_rating
        fresh_ratings[(u, iid)] = (rating, timestep)
        if u not in fresh_rubar: # user is not inserted
            # initiliaze dict values
            fresh_user_profile[u] = [[iid, rating, timestep]]
            fresh_rubar[u] = (rating, 1)
        else: # user exists
            # update dict values
            fresh_user_profile[u].append([iid, rating, timestep])
            s, num = fresh_rubar[u]
            fresh_rubar[u] = (s + rating, num + 1)

    # update current user timeStep
    for u, iid in fresh_ratings.keys():
        if u in exist_users: user_t[u] += 1
        else: user_t[u] = 1

    """Obtain rubar, e info"""
    # FIXME small error in case of duplicates between fresh and prev
    rubar = {}
    user_e = {} # user -> e
    for u in fresh_users:
        old_sum, old_num = prev_rubar.get(u, (0, 0))
        if old_num:
            old = old_sum / float(old_num)
        else: # if all fresh users are new => prev_rubar = 0
            old = 0
        new_sum, new_num = fresh_rubar[u]
        s = old_sum + new_sum
        num = old_num + new_num
        new = s / float(num)
        rubar[u] = new
        user_e[u] = new - old

    # broadcast needed variables
    frb = sc.broadcast(fresh_ratings)
    prb = sc.broadcast(prev_ratings)
    rub = sc.broadcast(rubar)
    eb = sc.broadcast(user_e)
    utb = sc.broadcast(user_t)

    temp_start_time = start
    temp_end_time = time.time()
    print("obtain info latency (sec): %g" % (temp_end_time - temp_start_time))
    temp_start_time = temp_end_time

    '''update2: update UserProfiles'''

    """Update the previous existing profiles"""
    sc.parallelize(profiles) \
            .map(lambda p: updateUserProfiles(p[0], p[1], frb.value,
                                              containDuplicates)) \
            .saveToCassandra(keyspace, "userprofiles")

    """PUT the new ones"""
    # new users set (i.e. users that rate for first time)
    new_users = fresh_users - exist_users
    # append all new_users
    if new_users and not ignoreNew:
        print("union: UserProfiles")
        new_user_profiles = list(map(lambda u: (u, fresh_user_profile[u]),
                new_users))

        sc.parallelize(new_user_profiles) \
                .saveToCassandra(keyspace, "userprofiles")

    temp_end_time = time.time()
    print("update UserProfiles time (sec): %g" % (
        temp_end_time - temp_start_time))
    temp_start_time = temp_end_time

    '''update3: update allItems by appending the new_items'''
    if not ignoreNew:
        new_items = fresh_items - allItems
    else:
        new_items = set()

    print("new_items: ", len(new_items))

    allItems.update(new_items)

    temp_end_time = time.time()
    print("update allItems time (sec): %g" % (temp_end_time - temp_start_time))
    temp_start_time = temp_end_time

    '''update4: obtain toBeUpdated items = batch# of candidate sets'''

    """Region 1: Get the k neighbors of fresh_items: fresh_id -> kNN"""
    kN = sc.cassandraTable(keyspace, "topk") \
            .select("item_id", "neighbors") \
            .where("item_id in " + replaceBrackets(fresh_items)) \
            .map(lambda p: (p.item_id, p.neighbors))

    candidates = {}
    # candidates: fresh_id -> kN
    for item, neighbors in kN.collect():
        if neighbors != None:
            candidates[item] = set(map(lambda x: int(x[0]), neighbors))

    temp_end_time = time.time()
    print("one-hop latency (sec): %g" % (temp_end_time - temp_start_time))
    temp_start_time = temp_end_time

    """Region 2: Get the k neighbors of k neighbors of fresh_items = batch*k^2"""
    kkN = sc.parallelize(
            set(chain.from_iterable(candidates.values())), part) \
            .map(lambda p: (p, [])) \
            .joinWithCassandraTable(keyspace, "topk") \
            .on("item_id") \
            .select("neighbors") \
            .map(lambda p: (p[0][0], \
                list(map(lambda x: int(x[0]), p[1][0]))) if p[1][0] else None)

    # kkNd: kN -> neighbors
    kkNd = {}
    for item, neighbors in kkN.collect():
        if neighbors != None:
            kkNd[item] = neighbors
        else:
            kkNd[item] = []

    temp_end_time = time.time()
    print("two-hop latency (sec): %g" % (temp_end_time - temp_start_time))
    temp_start_time = temp_end_time

    """Region 3: Get k random items for every fresh_item = batch*k"""
    randomList = []
    # update candidates: fresh_id -> candidate set
    k = min(len(allItems), k)
    for item in candidates.keys():
        for neighbor in candidates[item].copy():
            candidates[item].update(kkNd[neighbor]) # + k^2
        candidates[item].update(set(random.sample(allItems, k))) # + random

    temp_end_time = time.time()
    print("fetch random latency (sec): %g" % (temp_end_time - temp_start_time))
    temp_start_time = temp_end_time

    # set of items to be updated
    toBeUpdated = set(chain.from_iterable(candidates.values())) \
            .union(fresh_items)

    updateb = sc.broadcast(toBeUpdated)

    print("items-to-be-updated: %d" % len(toBeUpdated))

    '''update5: update ItemInfo'''

    """union new_items"""
    if new_items and not ignoreNew:
        print("union: ItemInfo")
        sc.parallelize(map(lambda item:(item, 0, 0, 0), new_items)) \
                .saveToCassandra(keyspace, "iteminfo")

        temp_end_time = time.time()
        temp_start_time = temp_end_time

    """update for toBeUpdated items"""
    # GET -> UPDATE
    ItemInfoGET = sc.cassandraTable(keyspace, "iteminfo") \
            .select("item_id", "li", "ni", "qi") \
            .where("item_id in " + replaceBrackets(toBeUpdated)) \
            .map(lambda p: (p.item_id, p.li, p.ni, p.qi)) \
            .map(lambda p: updateItemInfo(p[0], p[1], p[2], p[3], \
                utb.value, prb.value, frb.value, rub.value, eb.value, alpha)) \
            .persist(StorageLevel.DISK_ONLY)

    smallQ = {} # Q info for toBeUpdated items
    for (item, L, n, Q) in ItemInfoGET.collect():
        smallQ[item] = Q
        """update mostPopular"""
        if item in popularIds: # if already popular
            # update n
            for i, (nprev, iid) in enumerate(popularHeap):
                if iid == item:
                    popularHeap[i] = (n, item)
                    break
        elif n > popularHeap[0][0]: # if more popular than the least popular
            popularIds.remove(heapq.heappop(popularHeap)[1])
            heapq.heappush(popularHeap, (n, item)) # push this
            popularIds.add(item)

    qb = sc.broadcast(smallQ)

    # PUT
    ItemInfoGET.saveToCassandra(keyspace, "iteminfo")
    ItemInfoGET.unpersist()

    temp_end_time = time.time()
    print("update iteminfo latency (sec): %g" % (temp_end_time - temp_start_time))
    temp_start_time = temp_end_time

    '''update6: update ItemPairInfo'''

    """union new_items"""
    if new_items and not ignoreNew:
        print("union: ItemPairInfo")
        # itemInfo is updated so we need a cross-product
        sc.cassandraTable(keyspace, "iteminfo") \
                .select("item_id") \
                .map(lambda p: p.item_id) \
                .cartesian(sc.parallelize(new_items, part)) \
                .filter(lambda p: p[0] != p[1]) \
                .map(lambda p: (p[0], p[1], 0, 0, 0) if p[0] < p[1] else \
                (p[1], p[0], 0, 0, 0)) \
                .saveToCassandra(keyspace, "itempairinfo")

        temp_end_time = time.time()
        temp_start_time = temp_end_time

    """update for (fresh_items, toBeUpdated) pairs"""
    # obtain toBeUpdated item_pairs
    updated_pairs = []
    for itemi in candidates.keys():
        for itemj in candidates[itemi]:
            if itemi < itemj:
                updated_pairs.append((itemi, itemj))
            elif itemj < itemi:
                updated_pairs.append((itemj, itemi))

    print("Updated_pairs size: ", len(updated_pairs))

    # GET -> UPDATE
    ItemPairInfoGET = sc.parallelize(updated_pairs, part) \
            .map(lambda x: (x[0], x[1], 0, 0, 0)) \
            .joinWithCassandraTable(keyspace, "itempairinfo") \
            .on("itemi", "itemj") \
            .select("lij_i", "lij_j", "pij") \
            .map(lambda p: updateItemPairInfo(p[0][0], p[0][1], \
                p[1][0], p[1][1], p[1][2], \
                fresh_users, utb.value, prb.value, frb.value, \
                rub.value, eb.value, alpha)) \
            .persist(StorageLevel.DISK_ONLY)

    smallP = {} # Pij info for updated_pairs
    for (i, j, Li, Lj, P) in ItemPairInfoGET.collect():
        smallP[(i, j)] = P

    pb = sc.broadcast(smallP)

    # PUT
    ItemPairInfoGET.saveToCassandra(keyspace, "itempairinfo")
    ItemPairInfoGET.unpersist()

    temp_end_time = time.time()
    print("update itempairinfo latency (sec): %g" % (temp_end_time - temp_start_time))
    temp_start_time = temp_end_time

    """update7: update topK RDD"""

    """union new_items"""
    if new_items and not ignoreNew:
        print("union: topK")
        # new top-k: k most popular items
        temp = map(lambda x: x[1], heapq.nlargest(k+1, popularHeap))
        # new top-k: k random item
        # assign 0 similarity for all new top-k neighbors
        new_topK = list(map(lambda item: (item,
                                          list(map(lambda x: [x, 0], \
                list(set(temp) - set([item]))[:k]))), new_items))

        sc.parallelize(new_topK).saveToCassandra(keyspace, "topk")

    """update for toBeUpdated items"""
    # GET -> UPDATE -> PUT
    sc.parallelize(toBeUpdated, part) \
            .map(lambda x: (x, [])) \
            .joinWithCassandraTable(keyspace, "topk") \
            .on("item_id") \
            .select("neighbors") \
            .map(lambda p: (p[0][0], p[1][0])) \
            .map(lambda p: updateNeighbors(p[0], p[1], qb.value, \
                pb.value, updateb.value, k)) \
            .saveToCassandra(keyspace, "topk")

    temp_end_time = time.time()
    print("update topK latency (sec): %g" % (temp_end_time - temp_start_time))
    temp_start_time = temp_end_time

    """Get topK info for items used for prediction"""
    # smalltopK: topk info for mostPopular items
    topKGET = sc.cassandraTable(keyspace, "topk") \
            .select("item_id", "neighbors") \
            .where("item_id in " + replaceBrackets(popularIds)) \
            .map(lambda p: (p.item_id, p.neighbors))

    topKb = sc.broadcast(dict(topKGET.collect()))

    temp_end_time = time.time()
    print("get mostPopular latency (sec): %g" % (temp_end_time - temp_start_time))
    temp_start_time = temp_end_time

    """update8: update topR RDD"""

    """union new_users"""
    # append new_users with empty recommendations
    if new_users and not ignoreNew:
        print("union: topR")
        new_user_recom = list(map(lambda u: (u, []), new_users))
        sc.parallelize(new_user_recom).saveToCassandra(keyspace, "topr")


    """update recommendations"""

    # get old users' profiles
    user_profiles = {}
    for u, i in prev_ratings:
        rating, timestamp = prev_ratings[(u, i)]
        if u not in user_profiles:
            user_profiles[u] = []
        user_profiles[u].append((i, rating))
    # get fresh users' profiles
    # maxWeight is max Timestamp
    for u, i in fresh_ratings:
        rating, timestamp = fresh_ratings[(u, i)]
        if u not in user_profiles:
            user_profiles[u] = []
        user_profiles[u].append((i, rating))

    # GET -> UPDATE -> PUT
    sc.cassandraTable(keyspace, "topr") \
            .select("user_id", "recommendations") \
            .where("user_id in " + replaceBrackets(fresh_users)) \
            .map(lambda p: (p.user_id, p.recommendations)) \
            .map(lambda p: topRecom(p[0], user_profiles.get(p[0]), \
                    topKb.value, popularHeap, R)) \
            .saveToCassandra(keyspace, "topr")

    temp_end_time = time.time()
    print("update topR latency (sec): %g" % (temp_end_time - temp_start_time))
    temp_start_time = temp_end_time

    topKb.unpersist()
    del topKb

    # unpersist broadcast variables (i.e. delete from executors)
    frb.unpersist()
    prb.unpersist()
    rub.unpersist()
    eb.unpersist()
    utb.unpersist()
    updateb.unpersist()
    qb.unpersist()
    pb.unpersist()

    # delete from driver
    del frb
    del prb
    del rub
    del eb
    del utb
    del updateb
    del qb
    del pb

    del smallP
    # force garbage collect
    gc.collect()

    return popularHeap
