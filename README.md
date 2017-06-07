## Description
Openstack-logProcessor is a Flink program to read, process and dump data.

## Introduction
In this prototype, developed by Red Hat and KEEDIO, we managed to address the above challenges with the use of Big Data tools like Apache Flink and Apache Kafka, that enable us to process the constant stream of syslog messages (RFC5424) produced by the Infrastructure as a Service (IaaS). Log multiplexing and complex pattern detection has been possible using Apache Flink 1.3 CEP.  Apache Kafka is also used to manage the detected events and enable an external alert management.  
 
## Main goal
Read data from kafka, process, generate alerts and dump Cassandra.
 
## Available properties
|   propiedad	|   default value	|   mandatory	|   Description and example	|   connector	|
|:-:	|---	|---	|---	|---	|
|cassandra.port 	|   9042	|  no 	|  33105 	| flink-cassandra  	|
|cassandra.host 	|   disabled	|no   	| cassandra-openstack-poc-shuriken.apps.keedio.lab  	| flink-cassandra  	|
|restart.attempts	|  3 	|no   	|5 (intentos de reinicio del job si alguna tarea falla)   	| flink  	|
|restart.delay  	|   10	|no   	| 10 (minutos de espera entre cada x intentos de reinicio)  	|   flink	|
|checkpointinterval 	|  10000 	|no   	| ms entre puntos de chequeo  	|   flink	|
|parseBody  	    | true  	|  no 	| el cuerpo del mensaje syslog contiene los atributos necesarios (específico para la poc)  	|  flink 	|
|source.topic   	|   	|  yes 	| nombre del topic para el componente source  	| flink-kafka  	|
|target.topic   	|   	|  yes 	| nombre del topic para componente sink  	|  flink-kafka 	|
|bootstrap.servers	|   127.0.0.1:9092	|no   	| servidor y puerto para kafka   	|  flink-kafka 	|
|zookeeper.connect	|  127.0.0.1:2181 	|no 	| servidor y puerto para zookeeper  	| flink-kafka  	|
|group.id        	|  myGroup 	|no   	| grupo de consumidores al que pertenece la source kafka  	|  flink-kafka 	|
|auto.offset.reset 	|  latest 	|no   	| if nothing is commited yet automatically reset the offset to the latest offset   	| flink-kafka  	|
|broker         	|  bootstrap.servers 	|yes   	|kafka producer   	|  flink-kafka 	|
|enable.cep     	|  true 	|no   	| enable disable complex event processing  	| flink-cep  	|
|maxOutOfOrdeness  	| 0  	|yes   	|10 segundos máximo de espera entre eventos que llegan desordenados en el tiempo.   	|  flink-cep 	|



