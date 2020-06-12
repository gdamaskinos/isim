"""
  Copyright (c) 2020 Georgios Damaskinos
  All rights reserved.
  @author Georgios Damaskinos <georgios.damaskinos@gmail.com>
  This source code is licensed under the MIT license found in the
  LICENSE file in the root directory of this source tree.
"""

"""Test program for performing simple get-put operations on cassandra with Spark"""

import time
from pyspark import SparkContext, StorageLevel, SparkConf
from pyspark.sql import SparkSession
import random
from datetime import datetime
import pyspark_cassandra
from datetime import timedelta
import collections
import sys, os
import argparse
from argparse import RawTextHelpFormatter

"""
CREATE KEYSPACE mykeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};
USE mykeyspace;
CREATE TABLE test (key int PRIMARY KEY, val double);
"""
def defineTables(keyspace):
    '''Define cassandra tables'''
    host = conf.get("spark.cassandra.connection.host")
    cmd = ("cqlsh " + host + " --request-timeout 120 -e \" drop KEYSPACE IF EXISTS " + keyspace + "; "
       "CREATE KEYSPACE mykeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}; USE  " + keyspace + ";"
    "CREATE TABLE test (key int PRIMARY KEY, val double);\"")

    os.system(cmd)

def compare(x, y):
    '''Compare two lists'''
    return collections.Counter(x) == collections.Counter(y)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter, \
             description="Perfoms get/put test operations on Cassandra \n \
             Example usage: \n \
             spark-submit --master spark://$(hostname):7077 \n \
                          --conf spark.driver.memory=6G \n \
                          --executor-memory 6G \n \
                          --packages anguenot:pyspark-cassandra:2.4.0 \n \
                          --conf spark.cassandra.connection.host=$(hostname) \n \
                          cassandraSparkTest.py <args>")
    parser.add_argument("--num_tests", type=int, default=5,
                        help="Number of tests")
    parser.add_argument("--test_get_size", type=int, default=10,
                        help="Number of random get keys per test")
    parser.add_argument("--test_put_size", type=int, default=10,
                        help="Number of random put keys per test")
    parser.add_argument("--key_range", type=int, default=1000,
                        help="Range of keys to uniformly sample from")
    parser.add_argument("--spark_partitions", type=int, default=100)
    parser.add_argument('--skip_check', dest='skip_check',
                        action='store_true',
                        help='Skip verification that puts are successful \n \
                        Example: set this flag for measuring throughput')
    args = parser.parse_args()

    sess = SparkSession\
        .builder\
        .appName("Spark-Cassandra Test")\
        .getOrCreate()
    conf = sess.sparkContext._conf
    sess.stop()
    sc = pyspark_cassandra.CassandraSparkContext(conf=conf)

    keyspace = "mykeyspace"
    defineTables(keyspace)

    key_range = range(0, args.key_range)

    start = time.time()

    # !! ATTENTION if rdd contains tuples instead of dicts with the col names
    #   => the mapping to cols is done in alphabetical ordering
    rdd = sc.parallelize(map(lambda x: (x, 0), key_range),
                         args.spark_partitions).persist(
                           StorageLevel.MEMORY_AND_DISK)
    print("RDD created")
    rdd.saveToCassandra(keyspace, "test")
    end = time.time()
    print("Table creation time: %g" %  (end - start))

    failed = 0
    for i in range(1, args.num_tests + 1):
        start = time.time()

        """Get values"""

        # pick random keys for get operation
        getKeys = random.sample(key_range, args.test_get_size)

        # get toBeBroadcasted values
        rddGET = sc.cassandraTable(keyspace, "test") \
                .select("key", "val").where("key in " + str(tuple(getKeys))) \
                .map(lambda p: (p.key, p.val))

        rddGET.collect()

        end = time.time()
        print("GET1 time in iteration %d: %g" % (i, (end - start)))

        start = time.time()
        rddGET = sc.parallelize(getKeys, args.spark_partitions) \
                .map(lambda x: (x, 3)) \
                .joinWithCassandraTable(keyspace, "test") \
                .on("key") \
                .select("val") \
                .map(lambda l: (l[0][0], l[1][0]))

        rddGET.collect()

        end = time.time()
        print("GET2 time in iteration %d: %g" % (i, (end - start)))

        """Put values"""
        start = time.time()

        putlist = []
        putKeys = random.sample(key_range, args.test_put_size)
        for key in putKeys:
            putlist.append((key, 42))

        rddPUT = sc.parallelize(putlist, args.spark_partitions)
        rddPUT.saveToCassandra(keyspace, "test")

        if args.skip_check:
            rddCHECK = sc.cassandraTable(keyspace, "test").select(
                "key","val").where("key in " + str(tuple(putKeys)))

            checklist = map(lambda x: (x.key, x.val), rddCHECK.collect())
            if not compare(checklist, putlist):
                print("NOT EQUAL!")
                failed = 1
                for (key, val) in putlist:
                    if (key, val) not in checklist:
                        print("Missing: (%s, %s)" % (key, val))

                for (key, val) in checklist:
                    if (key, val) not in putlist:
                        print("Additional: (%s, %s)" % (key, val))

                print("put:")
                print(putlist)
                print("got:")
                print(checklist)

        end = time.time()
        print("PUT time in iteration %d: %g" % (i, (end - start)))


    sc.stop()
    sys.exit(failed)
