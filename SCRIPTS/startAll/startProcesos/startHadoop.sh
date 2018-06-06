cd /opt/hadoop/sbin/
#Se arrancan los servicios del HDFS 
./start-dfs.sh 
#jps -> comprueba los servicios java arrancados
#Se arrancan los servicios de YARN 
./start-yarn.sh 
#Se puede arrancar todo con start-all.sh
#Se arranca el job-history-server 
./mr-jobhistory-daemon.sh start historyserver 
