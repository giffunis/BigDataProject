#!/usr/bin/env bash
#
#  Author: José Saúl Giffuni
#  Date: 2025-07-26 13:17:00
#

export JAVA_HOME="${JAVA_HOME:-/usr}"

echo "================================================================================"
echo "                              HBase Docker Container For BigData"
echo "================================================================================"
echo

# shell breaks and doesn't run zookeeper without this
mkdir -pv /hbase/logs

# kill any pre-existing rest or thrift instances before starting new ones
#pgrep -f proc_rest && pkill -9 -f proc_rest
#pgrep -f proc_thrift && pkill -9 -f proc_thrift

echo "Starting local Zookeeper"
/hbase/bin/hbase zookeeper &>/hbase/logs/zookeeper.log &
echo

echo "Starting HBase"
/hbase/bin/start-hbase.sh
echo

#echo "Starting HBase Stargate Rest API server"
#/hbase/bin/hbase-daemon.sh start rest


#echo "Starting HBase Thrift API server"
#/hbase/bin/hbase-daemon.sh start thrift
#/hbase/bin/hbase-daemon.sh start thrift2

echo "Starting 3 HBase region servers"
/hbase/bin/local-regionservers.sh start 1 2 3

echo "Aquí llamaremos al programa..."
if [ -t 0 ]; then                                                                                     
    /hbase/bin/hbase shell                                                                            
else                                                                                                  
    echo " Running non-interactively, will not open HBase shell for HBase shell start this image with 'docker run -t -i' switches"
    
    tail -f /hbase/logs/* &                                                                           
    # this shuts down from Control-C but exits prematurely, even when +euo pipefail and doesn't shut do
    # so I rely on the sig trap handler above                                                          
    wait || :                                                                                          
fi
