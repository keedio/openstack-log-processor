CREATE KEYSPACE IF NOT EXISTS redhatpoc WITH replication={'class' : 'SimpleStrategy', 'replication_factor':1};

USE redhatpoc;

CREATE TABLE redhatpoc.cassandraconnectorexample(
    id varchar,
    text varchar,
    PRIMARY KEY(id));