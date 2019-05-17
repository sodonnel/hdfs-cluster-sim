#!/bin/sh

# Params:
#  1. log directory for stdout / stderr / log4j and pid file
#  2.  number of datanodes to start
#  3.  Java Heap heap in MB 
#
# Assumes the correct hdfs command is on the path and
# the hdfs config is correctly picked up

if (( $# != 3 )); then
    echo "Illegal number of parameters - expecting path/to/log/dir and count of DNs to start"
    exit 1
fi

export CLASSPATH=`hadoop classpath`
export CLASSPATH="$CLASSPATH:../target/*"
nohup java "-Xmx$3m" -Dlog4j.configuration="file:../conf/log4j.properties" -Dlog.dir="$1" sodonnell.MultipleDatanode $2 >"$1/dnstdouterr" 2>&1 < /dev/null &
disown
echo $! > "$1/dn_pid"
