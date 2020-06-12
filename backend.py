"""
  Copyright (c) 2020 Georgios Damaskinos
  All rights reserved.
  @author Georgios Damaskinos <georgios.damaskinos@gmail.com>
  This source code is licensed under the MIT license found in the
  LICENSE file in the root directory of this source tree.
 """

"""SwIFT backend implementation on PySpark with Cassandra"""

import gc
import time, datetime
import os, sys
import numpy as np
import pandas as pd
import math
import random
import argparse
from argparse import RawTextHelpFormatter

from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession
import pyspark_cassandra

sys.path.append(
    os.path.abspath(os.path.join(os.path.realpath(__file__),
            os.pardir))+'/utils/')
import comm
from bootstrapper import createRDDs, loadFromFiles, createTopR
from updater import update
from helpers import prediction

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter, \
            description='SwIFT backend\n \
            Example Usage: \n \
                spark-submit --master spark://$(hostname):7077 \n \
                --packages anguenot:pyspark-cassandra:2.4.0 \n \
                --conf spark.cassandra.connection.host=$(hostname) \n \
                --conf spark.driver.memory=100G --executor-memory 100G \n \
                --total-executor-cores 96 \n \
                fast_swift.py <args>')
    parser.add_argument("--training_set", help="file://path/to/trainingSet")
    parser.add_argument("--test_set", help="file://path/to/testSet")
    parser.add_argument("--k", default=5, help="model size for kNN")
    parser.add_argument("--alpha", default=0, help="ISIM temporal param")
    parser.add_argument("--max_mbatch_size", default=1)
    parser.add_argument("--p", default=100, help="Size of most popular items to choose from for cold-start recommendations")
    parser.add_argument('--listR', dest='listR', nargs='+', type=int, help="List of number \
            of recommendations. A separate output for each value.")
    parser.add_argument('--VIEWSIM', dest='viewsim', action='store_true', \
            help='If set => calculate average view similarity after every batch.')
    parser.add_argument('--RMSE', dest='rmse', action='store_true', \
            help='If set => calculate RMSE for the last rating in \
            every batch.')
    parser.add_argument('--no-bootstrap', dest='bootstrap', \
            action='store_false',
            help='Skip bootstrap phase. The values are obtained from cassandra')
    parser.add_argument('--loadDir', type=str, default=None, \
            help='file://folder/to/load/key-value/relations/ \n \
            k must be <= stored k! \n \
            p must be <= stored p! \n \
            bootstrap must be True to derive topR!')
    parser.add_argument('--storeDir', type=str, default=None, \
            help='file://folder/to/store/key-value/relations/ \n \
            Must empty this directory before! \n \
            bootstrap must be True')
    parser.add_argument('--listItems', dest='listI', nargs='+', type=int, \
            help="List of items to print the processing timestamps. Usefull \
            for energy consumption experiments.")
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument("--backend_port", type=int, default=9995,
                        help='Port to listen for client messages')
    parser.add_argument("--backend_ip", type=str,
                        default="127.0.0.1",
                        help='IP address to listen to')


    parser.set_defaults(bootstrap=True)
    args = parser.parse_args()

    if sys.version_info[0] != 3:
      print("Requires Python 3")
      sys.exit(1)

    random.seed(args.seed)

    assert args.storeDir is None or not os.listdir(args.storeDir[6:]), \
        "storeDir directory not empty!"

    sess = SparkSession\
        .builder\
        .appName("SwIFT")\
        .getOrCreate()
    conf = sess.sparkContext._conf
    sess.stop()

    sc = pyspark_cassandra.CassandraSparkContext(conf=conf)

    part = int(conf.get("spark.cores.max"))
    sqrtpart = int(math.ceil(math.sqrt(part)))

    # define cassandra's keyspace
    keyspace = "mykeyspace"

    # define whether to create RDDs in cassandra
        # if false => bootstrap phase is skipped
    bootstrap = args.bootstrap

    # define whether to calculate RMSE
    rmse = args.rmse

    # mostPopular size for prediction
    popularSize = int(args.p)
    print("mostPopular: %d" % popularSize)

    # define dataset length for scanrate computation
    size = 1700 # ML100K

    # define whether to include key_item in neighbors
    checkNeighbors = False

    # define if duplicate ratings (same user + same item)
    # are provided in the incremental update phase
    containDuplicates = False

    # define whether to ignore new users and new items in testPhase
    ignoreNew = False

    # top k neighbors param
    k = int(args.k)
    print("topK: %d" % k)

    # maximum number of provided recommendations param
    R = max(args.listR) # N is denoted with R
    listR = args.listR
    print("topN: %s" % R)

    # maximum number for each item's recommendations (set <= 0 to deactivate)
    maxR = -1
    print("maxR: %d" % maxR)

    # isim alpha param
    alpha = float(args.alpha)
    print("alpha: %g" % alpha)

    print("Batch size: %d" % args.max_mbatch_size)

    # total number of computed similarities
    totalSimEval = sc.accumulator(0)

    new_lines = sc.textFile(args.test_set, part)

    print("BOOTSTRAP")
    """PHASE1: BOOTSTRAP"""
    start = time.time()
    if args.loadDir is not None:
        loadFromFiles(args.loadDir, popularSize, k, R, sc, conf, keyspace, part)

    elif bootstrap:
        createRDDs(args.training_set, popularSize, k, R, alpha,
                   sc, conf, keyspace, part, args.storeDir)
    else:
        print("Data will be read from the previous state of Cassandra.")

    # derive topR (that depends on config parameters and not just on the dataset)
    # and fetch most popular items
    popularHeap = createTopR(R, args.max_mbatch_size, popularSize, sc, keyspace)

    allItems = set(sc.cassandraTable(keyspace, "topk") \
            .select("item_id").map(lambda p: p.item_id).collect())

    end = time.time()
    print("Total training latency (sec): %g" % (end-start))

    print("INCREMENTAL UPDATES")
    """PHASE2: INCREMENTAL UPDATES"""

    # dictionary for batching observations
    # (user, iid) -> (rating, timestep)
    fresh_ratings = {}

    # temp timestep for fresh_ratings
    # user -> count
    tempTimestep = {}

    firstIteration = True

    recommendations = {} # R -> (user_id, recommended_items_list)
    for val in args.listR:
        recommendations[val] = []
    rec = {}
    batch_latency = [] # contain the latency for each batch event
    update_latency = [] # contain the latency for each update
    RMSEcount = 0
    MAEsum = 0
    RMSEsum = 0
    RMSErbarsum = 0
    it = 0

    conn = comm.openServerConn(args.backend_port, args.backend_ip)

    batch_id = 0
    while True:
        batch_id += 1
        print("Waiting for new batch...")
        try:
            comm.sendMessage(conn, "ACK")
        except BrokenPipeError:
            print("Connection with frontend terminated!")
            break
        msg = comm.getMessage(conn, isJSON=True) # fetch data from frontend
        mbatch = pd.read_json(msg)

        """Batching phase"""
        start = time.time()
        for index, row in mbatch.iterrows():

            '''batch1: Get new observation (user, item, rating, timestamp)'''
            user = int(row['user'])
            item_id = int(row['item'])
            rating = float(row['rating'])
            timestamp = int(row['unixtimestamp'])

            '''batch2: Add it to fresh_ratings dictionary'''
            if user in tempTimestep: tempTimestep[user] += 1
            else: tempTimestep[user] = 1
            fresh_ratings[(user, item_id)] = (rating, tempTimestep[user])

            if rmse:
                '''batch3a: Predict'''
                # get user_items
                profile = sc.cassandraTable(keyspace, "userprofiles") \
                        .select("profile").where("user_id=" + str(user)) \
                        .map(lambda p: p.profile).collect()

                if profile:
                    profile = profile[0]
                    # derive rbar
                    rbar = 0
                    user_items = {} # already rated items
                    for value in profile:
                        iid = value[0]
                        rating = value[1]
                        user_items[iid] = rating
                        rbar += rating

                    rbar = rbar / len(user_items.keys())

                    # get topk
                    knn = sc.cassandraTable(keyspace, "topk") \
                            .select("neighbors").where("item_id=" + str(item_id)) \
                            .map(lambda p: p.neighbors).collect()

                    if knn:
                        knn = knn[0]

                    # calculate prediction and rmse
                    pred = prediction(knn, user_items, rbar)
                    if pred:
                        RMSEsum += (pred - rating)**2
                        RMSErbarsum += (rbar - rating)**2
                        MAEsum += abs(pred - rating)
                        RMSEcount += 1

            '''batch3b: Recommend'''
            recom = sc.cassandraTable(keyspace, "topr").select("recommendations") \
                    .where("user_id=" + str(user)) \
                    .map(lambda p: p.recommendations).collect()

            new_recom = {} # R -> new recommendations list
            for val in listR:
                new_recom[val] = []
            if recom != []: # if user exists
                if recom != [None]: # if recommendations for the user exist
                    recom = recom[0]
                    count = 0 # counter to contain the number of recoms
                    for x in recom:
                        # if x is recommended more than maxR times => skip
                        if maxR > 0:
                            if user in rec:
                                if rec[user].count(x[0]) == maxR:
                                    continue
                        # if x is not-yet-rated item
                        if (user, x[0]) not in fresh_ratings:
                            for val in listR:
                                if count < val:
                                    new_recom[val].append(int(x[0]))
                            count += 1
                            if count == R: break
                    for val in listR:
                        # first int is user and the rest are recoms
                        msg = [user] + new_recom[val]
                        comm.sendMessage(conn, msg, isJSON=False)
                        recommendations[val].append((user, new_recom[val]))


                    # TODO make rec update more efficient
                    if maxR > 0 :
                        if user in rec:
                            old = rec[user]
                            rec[user] = old + new_recom
                        else:
                            rec[user] = new_recom

        end = time.time()
        batch_latency.append(end-start)
        print("Batching latency for batch %d: %g" % (batch_id, end-start))
        """End of batching phase"""

        start = time.time()
        """Update phase"""
        popularHeap = update(fresh_ratings, allItems, k, alpha, R + len(mbatch),
                             popularHeap, containDuplicates, ignoreNew,
                             sc, keyspace, part)
        # reset for next micro-batch
        fresh_ratings = {}
        tempTimestep = {}

        """End of Update phase"""
        end = time.time()
        update_latency.append(end - start)
        print("update latency for batch %d (sec): %g" % (batch_id, end-start))

        if args.viewsim:
            """Calculate average view similarity
            get results: cat out | awk -F: '/Average view similarity:/ {getline; print $0}'
            """
            simAvg = sc.cassandraTable(keyspace, "topk") \
                    .select("item_id", "neighbors") \
                    .map(lambda p: np.average(list(map(lambda x: x[1], p.neighbors)))) \
                    .reduce(add)
            print("Average view similarity: %g" % (simAvg / float(len(allItems))))

        # popularity energy consumption
        if args.listI and item_id in args.listI:
            print("start and end datetime for item: %s" % item_id)
            print(datetime.datetime.fromtimestamp(start)).strftime(
                    '%Y-%m-%d %H:%M:%S')
            print(start)
            print(datetime.datetime.fromtimestamp(end)).strftime(
                    '%Y-%m-%d %H:%M:%S')
            print(end)

    if rmse:
        print("phase3a: Calculate RMSE")
        print("RMSE count: %g" % RMSEcount)
        RMSEfinal = np.sqrt(RMSEsum / RMSEcount)
        RMSErbarfinal = np.sqrt(RMSErbarsum / RMSEcount)
        MAEfinal = MAEsum / RMSEcount
        print("RMSE rbar: %g" % RMSErbarfinal)
        print("MAE: %g" % MAEfinal)
        print("RMSE: %g" % RMSEfinal)


    print("Iterations: %d" % batch_id)
    print("Average update latency: %g" % np.average(update_latency))
    print("Min update latency: %g" % min(update_latency))
    print("50th percentile update latency: %g" % np.percentile(update_latency, 50))
    print("90th percentile update latency: %g" % np.percentile(update_latency, 90))
    print("99th percentile update latency: %g" % np.percentile(update_latency, 99))
    print("Max update latency: %g" % max(update_latency))

    print("Average batch latency: %g" % np.average(batch_latency))
    print("Min batch latency: %g" % min(batch_latency))
    print("50th percentile batch latency: %g" % np.percentile(batch_latency, 50))
    print("90th percentile batch latency: %g" % np.percentile(batch_latency, 90))
    print("99th percentile batch latency: %g" % np.percentile(batch_latency, 99))
    print("Max batch latency: %g" % max(batch_latency))

    sc.stop()
