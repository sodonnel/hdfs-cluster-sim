set -e
export CLASSPATH=`hadoop classpath`:target/DnSim-1.0-SNAPSHOT.jar
java -Dlog4j.configuration="file:./conf/log4j.properties" sodonnell.CommandShell
