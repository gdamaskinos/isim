#!/bin/bash

if [ "$#" -ne 2 ]; then
  echo "usage: RUN_TESTS=1 bash $0 <'STANDALONE' || 'YARN'> <temp_dir>"
  exit 1
fi

temp_dir=$(readlink -f $2)
host=`hostname -I | awk '{print $1}'`

SCRIPT=`realpath -s $0`
SCRIPTPATH=`dirname $SCRIPT`

#### STANDALONE SPARK ####
if [ "$1" = STANDALONE ]; then

  cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
  echo "export JAVA_HOME=$JAVA_HOME" >> $SPARK_HOME/conf/spark-env.sh
  echo "export SPARK_LOCAL_DIRS=$temp_dir" >> $SPARK_HOME/conf/spark-env.sh
  echo "export SPARK_LOG_DIR=$temp_dir/logs" >> $SPARK_HOME/conf/spark-env.sh
  echo "export SPARK_MASTER_HOST=$host" >> $SPARK_HOME/conf/spark-env.sh
  echo "export SPARK_PUBLIC_DNS=$host" >> $SPARK_HOME/conf/spark-env.sh
  
  cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
  freeMem=$(free -m | awk '/Mem/ {print $4;}') #if memory below 32GB make pointers 4B
  if [ $freeMem -lt 32768 ]; then
  	echo spark.executor.extraJavaOptions	-XX:+UseCompressedOops >> $SPARK_HOME/conf/spark-defaults.conf
  fi
  
  #cp $SPARK_HOME/conf/log4j.properties.template $SPARK_HOME/conf/log4j.properties
  echo 'log4j.rootCategory=WARN, console' > $SPARK_HOME/conf/log4j.properties
  echo 'log4j.appender.console=org.apache.log4j.ConsoleAppender' >> $SPARK_HOME/conf/log4j.properties
  echo 'log4j.appender.console.target=System.err'  >> $SPARK_HOME/conf/log4j.properties
  echo 'log4j.appender.console.layout=org.apache.log4j.PatternLayout'  >> $SPARK_HOME/conf/log4j.properties
  echo 'log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n'  >> $SPARK_HOME/conf/log4j.properties
  
  #uniq $OAR_NODEFILE | sed "/$HOSTNAME/d" > $SPARK_HOME/conf/slaves
  #uniq $OAR_NODEFILE | head -n $2 > $SPARK_HOME/conf/slaves
  echo "Getting configuration from $SPARK_HOME/conf/slaves. Must have set it before."
  echo "Starting Spark cluster..."
  start-all.sh
  #start-history-server.sh 

fi

#### SPARK on YARN with EXECO ####
if [ "$1" = YARN ]; then
#hadoop
hg5k --create $OAR_FILE_NODES --version 2
hg5k --bootstrap ~/opt/tarballs/hadoop-2.6.0.tar.gz
hg5k --initialize 

#fix problem
#hg5k --changeconf yarn.resourcemanager.scheduler.address=$(hostname):8030
#hg5k --changeconf yarn.resourcemanager.resource-tracker.address=$(hostname):8031

hg5k --start


#spark
spark_g5k --create YARN --hid 1 
spark_g5k --bootstrap ~/opt/tarballs/spark-1.6.0-bin-hadoop2.6.tgz
spark_g5k --initialize --start

#spark conf
echo spark.eventLog.enabled          true >> /tmp/spark/conf/spark-defaults.conf
#echo spark.eventLog.dir              file:/tmp/spark_events >> /tmp/spark/conf/spark-defaults.conf
echo spark.eventLog.dir              hdfs:///spark_events >> /tmp/spark/conf/spark-defaults.conf
echo spark.history.fs.logDirectory	hdfs:///spark_events >> /tmp/spark/conf/spark-defaults.conf

# kryoserializer causes bad gc in driver for broadcast variables
#echo spark.serializer	org.apache.spark.serializer.KryoSerializer >> /tmp/spark/conf/spark-defaults.conf
#echo spark.kryoserializer.buffer.max	2047m >> /tmp/spark/conf/spark-defaults.conf

freeMem=$(free -m | awk '/Mem/ {print $4;}') #if memory below 32GB make pointers 4B
if [ $freeMem -lt 32768 ]; then
	echo spark.executor.extraJavaOptions	-XX:+UseCompressedOops >> /tmp/spark/conf/spark-defaults.conf
fi

echo 'log4j.rootCategory=WARN, console' >> /tmp/spark/conf/log4j.properties
echo 'log4j.appender.console=org.apache.log4j.ConsoleAppender' >> /tmp/spark/conf/log4j.properties
echo 'log4j.appender.console.target=System.err'  >> /tmp/spark/conf/log4j.properties
echo 'log4j.appender.console.layout=org.apache.log4j.PatternLayout'  >> /tmp/spark/conf/log4j.properties
echo 'log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n'  >> /tmp/spark/conf/log4j.properties

# python verion
#echo 'PYSPARK_PYTHON=/home/gdamaskinos/Python-3.5.0/python' >> /tmp/spark/conf/spark-env.sh

#spark_history_server
#mkdir /tmp/spark_events
hg5k --execute 'fs -mkdir /spark_events'
/tmp/spark/sbin/start-history-server.sh

export SPARK_HOME=/tmp/spark
export PATH=$SPARK_HOME/bin:$PATH
export PATH=$SPARK_HOME/sbin:$PATH

#hive

##change the xml files
#cp $HIVE_HOME/conf/hive-site.template.xml $HIVE_HOME/conf/hive-site.xml
#sed -i 's/master/'$HOSTNAME'/g' $HIVE_HOME/conf/hive-site.xml
#
##copy to spark
#cp $HIVE_HOME/conf/hive-site.xml /tmp/spark/conf/
#
##start hive
#cd $DERBY_HOME/data/
#rm -r metastore_db
#
#nohup $DERBY_HOME/bin/startNetworkServer -h 0.0.0.0 &
#
#schematool -dbType derby -initSchema
#schematool -dbType derby -info 
#
#rm $HIVE_HOME/hcatalog/sbin/../var/log/hcat.pid
#$HIVE_HOME/hcatalog/sbin/hcat_server.sh start 
#$HIVE_HOME/hcatalog/sbin/webhcat_server.sh start

fi

echo "Starting CASSANDRA..."
cassandra -p $CASSANDRA_HOME/cassandra.pid > /dev/null
i=0
while ! cqlsh $host -e 'describe cluster' 2> /dev/null ; do     
  sleep 1; 
  i=$((i+1))
  if [ $((i%10)) = 0 ]; then
    echo "CASSANDRA takes too long to start...consider checking $CASSANDRA_HOME/logs/system.log for issues"
  fi
done

if [ "$RUN_TESTS" = 1 ]; then
  echo "Running Cassandra-Spark Tests ..."
  spark-submit --master spark://$host:7077 \
    --conf spark.driver.memory=6G --executor-memory 6G \
    --packages anguenot:pyspark-cassandra:2.4.0 \
    --conf spark.cassandra.connection.host=$host \
    $SCRIPTPATH/cassandraSparkTest.py

  if [ "$?" = 0 ]; then
    echo -e "CASSANDRA-SPARK TEST: \e[32mSUCCESSFUL\e[39m" 
  else
    echo -e "CASSANDRA-SPARK TEST: \e[31mFAILED\e[39m" 
  fi
fi


