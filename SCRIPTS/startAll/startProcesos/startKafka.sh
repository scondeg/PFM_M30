cd /opt/kafka/

##Para ejecutar Kafka utilizaremos uno de los scripts que proporciona:
#bin/kafka-server-start.sh config/server.properties
#Si se desea ejecutar en segundo plano se puede añadir la opción -daemon:
bin/kafka-server-start.sh -daemon config/server.properties

##Desde zookeper
# /opt/zookeeper/bin/zkCli.sh
#[zk: localhost:2181(CONNECTED) 5] ls /brokers/ids
#[0]
#Si hacemos un get del zNode podemos ver el contenido que el broker ha almacenado dentro.
#> [zk: localhost:2181(CONNECTED) 7] get /brokers/ids/0	
#get /brokers/ids/0
