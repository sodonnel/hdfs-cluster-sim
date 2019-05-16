#!/bin/sh

# Params:
#   log directory for stdout / stderr / log4j and pid file
#   number of datanodes to start
#
# Assumes the correct hdfs command is on the path and
# the hdfs config is correctly picked up

if (( $# != 2 )); then
    echo "Illegal number of parameters - expecting path/to/log/dir and count of DNs to start"
    exit 1
fi

export CLASSPATH=`hadoop classpath`
export CLASSPATH="$CLASSPATH:../target/*"
java -Dlog4j.configuration="file:../conf/log4j.properties" -Dlog.dir="$1" sodonnell.MultipleDatanode $2 >"$1/dnstdouterr" 2>&1 < /dev/null &
echo $! > "$1/dn_pid"
