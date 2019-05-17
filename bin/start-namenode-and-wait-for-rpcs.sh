#!/bin/sh

# Params:
#   log directory for stdout / stderr and pid file
#
# Assumes the correct hdfs command is on the path and
# the hdfs config is correctly picked up

if (( $# != 1 )); then
    echo "Illegal number of parameters - expecting path/to/log/dir"
    exit 1
fi


nohup hdfs namenode > "$1/stdouterr" 2>&1 < /dev/null &
disown
echo $! > "$1/nn_pid"

hdfs dfsadmin -safemode get
while [ $? -ne 0 ]; do
  echo "not ready yet ... retrying"
  sleep 1
  hdfs dfsadmin -safemode get
done

hdfs dfsadmin -safemode leave
