CREATE KEYSPACE IF NOT EXISTS redhatpoc WITH replication={'class' : 'SimpleStrategy', 'replication_factor':1};

USE redhatpoc;

CREATE TABLE redhatpoc.cassandraconnectorexample(
    id varchar,
    text varchar,
    PRIMARY KEY(id));

    CREATE TABLE redhatpoc.stack_services (
        id text,
         region text,
          loglevel text,
           service text,
            timeframe int,
             ts timeuuid,
             tfhours text,
              PRIMARY KEY((id), service, loglevel, region, ts));

    CREATE TABLE redhatpoc.raw_logs (
        date text,
         region text,
          loglevel text,
           service text,
            node_type text,
             log_ts timestamp,
              payload text,
              PRIMARY KEY((date), service, loglevel, region));

    CREATE TABLE redhatpoc.counters_nodes (
    id text,
     region text,
      az text,
       loglevel text,
        node_type text,
         ts timeuuid,
          primary key ( (id), loglevel, region,az, ts) );

    CREATE TABLE counters_services (
    id text,
     region text,
      az text, loglevel text, service text, ts timeuuid, primary key ((id), service, loglevel, region,az, ts) );