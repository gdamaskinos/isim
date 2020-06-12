#!/bin/bash

handler()
{
  echo "Killing SwIFT ..."
  kill $backendPID
  kill $frontendPID
  exit
}

trap handler SIGINT

if [ "$#" -ne 9 ]; then
  echo "usage: $0 <trainSet.csv> <testSet.csv> <topK> <topN> <alpha> <mostPopular> <batch_size> <backend_port> <log_file>"
  exit 1
fi

cmd="$0 $@"
train_set=$(readlink -f $1)
test_set=$(readlink -f $2)
topK=$3
topN=$4
alpha=$5
mostPopular=$6
batch_size=$7
backend_port=$8
log_file=$(readlink -f $9)

SCRIPT=`realpath -s $0`
SCRIPTPATH=`dirname $SCRIPT`

echo "This script deploys SwIFT and waits till the execution finishes. Press ctrl+c to stop the execution. Output of both the frontend and the backend is logged in ${log_file}"

echo "Reproduce with: $cmd" > $log_file
cd $SCRIPTPATH
echo "Git commit: $(git rev-parse HEAD)" >> $log_file
echo "Git status: $(git status)" >> $log_file

# deploy backend
freeMem=$(free -m | awk '/Mem/ {print $4;}')
freeMem=$((freeMem/1024)) # in GBs
driverMem=$(echo $freeMem*0.1 | bc)
driverMem=${driverMem%.*}G
executorMem=$(echo $freeMem*0.6 | bc)
executorMem=${executorMem%.*}G

spark-submit --master spark://$(hostname -I | awk '{print $1}'):7077 \
  --packages anguenot:pyspark-cassandra:2.4.0 \
  --py-files $SCRIPTPATH/utils/bootstrapper.py,$SCRIPTPATH/utils/updater.py,$SCRIPTPATH/utils/helpers.py \
  --conf spark.cassandra.connection.host=$(hostname -I | awk '{print $1}') \
  --conf spark.driver.memory=$driverMem \
  --executor-memory $executorMem \
  --conf spark.executor.heartbeatInterval=3600s \
  --conf spark.network.timeout=7200s \
  --conf spark.driver.maxResultSize="900G" \
  --conf spark.cassandra.output.consistency.level=ANY \
  --conf spark.app.name='SwIFT' \
  --total-executor-cores 3 \
  --supervise \
  $SCRIPTPATH/backend.py \
  --training_set file://$train_set \
  --test_set file://$test_set \
  --k $topK --alpha $alpha --p $mostPopular --listR $topN \
  --backend_ip $(hostname -I | awk '{print $1}') \
  --backend_port $backend_port \
  --RMSE >> $log_file 2>&1 &

backendPID=$!

# deploy frontend
python $SCRIPTPATH/frontend.py --backend_port $backend_port --backend_ip $(hostname -I | awk '{print $1}') --mbatch_size $batch_size --test_set $test_set >> $log_file 2>&1 &

frontendPID=$!

wait
