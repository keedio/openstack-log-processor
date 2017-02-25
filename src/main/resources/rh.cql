CREATE KEYSPACE IF NOT EXISTS rh WITH replication={'class' : 'SimpleStrategy', 'replication_factor':1};
USE rh;
CREATE TABLE rh.stack_services (
    id varchar,
     region varchar,
      loglevel varchar,
       service varchar,
        timeframe int,
         ts timeuuid,
          PRIMARY KEY((id), service, loglevel, region, ts));