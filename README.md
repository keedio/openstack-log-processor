## Description
Openstack-logProcessor is a Flink program to read, process and dump data.

## Introduction
In this prototype, developed by Red Hat and KEEDIO, we managed to address the above challenges with the use of Big Data tools like Apache Flink and Apache Kafka, that enable us to process the constant stream of syslog messages (RFC5424) produced by the Infrastructure as a Service (IaaS). Log multiplexing and complex pattern detection has been possible using Apache Flink 1.3 CEP.  Apache Kafka is also used to manage the detected events and enable an external alert management.  
 
## Main goal
Read data from kafka, process, generate alerts and dump Cassandra. 
