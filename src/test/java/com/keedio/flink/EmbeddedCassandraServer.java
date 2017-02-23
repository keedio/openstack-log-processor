package com.keedio.flink;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import java.io.IOException;

/**
 * Created by luislazaro on 23/2/17.
 * lalazaro@keedio.com
 * Keedio
 */

/**
 * Instanciar clase y arrancar.
 * Exceptions at the end of the class with cassandra-unit-spring witll be launched
 * ERROR com.datastax.driver.core.ControlConnection - [Control connection] Cannot
 * connect to any host, scheduling retry in 2000 milliseconds
 * The error has no impact on the test results.
 *
 * @see(http://github.com/jsevellec/cassandra-unit/issues/98)
 */
public class EmbeddedCassandraServer {
    private Session session;
    private String dataSetLocation;
    private String keySpace;

    public EmbeddedCassandraServer(String dataSetLocation, String keySpace)
            throws InterruptedException, TTransportException, ConfigurationException, IOException {
        this.dataSetLocation = dataSetLocation;
        this.keySpace = keySpace;
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        Cluster cluster = new Cluster.Builder().addContactPoints("127.0.0.1").withPort(9142).withProtocolVersion(ProtocolVersion.V3).build();
        this.session = cluster.connect();
        CQLDataLoader dataLoader = new CQLDataLoader(session);
        dataLoader.load(new ClassPathCQLDataSet(dataSetLocation, true, keySpace));

    }

    public void cleanupServer() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    public Session getSession() {
        return session;
    }

}
