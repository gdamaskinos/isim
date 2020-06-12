#!/bin/bash

if [ "$#" -ne 1 ]; then
  echo "usage: <FLAGS> bash $0 <'STANDALONE' || 'YARN'>"
  echo "<FLAGS>: CLEANSPARKWORK=1 CLEANSPARKLOGS=1 CLEANCASSANDRA=1"  
  exit 1
fi

echo "Stopping SPARK cluster..."

#### STANDALONE SPARK ####
if [ "$1" = STANDALONE ]; then

  stop-all.sh
  stop-history-server.sh

fi

#### SPARK on YARN with EXECO ####
if [ "$1" = YARN ]; then

  #cd $DERBY_HOME/data/ 
  #$HIVE_HOME/hcatalog/sbin/webhcat_server.sh stop
  #
  #$HIVE_HOME/hcatalog/sbin/hcat_server.sh stop
  #rm $HIVE_HOME/hcatalog/sbin/../var/log/hcat.pid
  #
  #$DERBY_HOME/bin/stopNetworkServer
  #rm -r metastore_db
  
  /tmp/spark/sbin/stop-history-server.sh
  spark_g5k --delete
  hg5k --delete                   

fi

echo "Stopping CASSANDRA..."
kill `cat $CASSANDRA_HOME/cassandra.pid`

if [ "$CLEANSPARKWORK" = 1 ]; then
  echo "Cleaning up spark working dir..."
  rm -rf $SPARK_HOME/work/*
fi

if [ "$CLEANSPARKLOGS" = 1 ]; then
 echo "Cleaning up spark logs dir..."
  rm -rf $SPARK_HOME/logs/*
  rm -rf $SPARK_HOME/work/*
fi

if [ "$CLEANCASSANDRA" = 1 ]; then
  echo "Cleaning up cassandra..."
  rm -rf $CASSANDRA_HOME/data/data/*
  rm -rf $CASSANDRA_HOME/data/commitlog/*
  rm -rf $CASSANDRA_HOME/data/saved_caches/*
fi
