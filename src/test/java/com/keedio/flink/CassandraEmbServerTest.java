package com.keedio.flink;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import org.cassandraunit.DataLoader;
import org.cassandraunit.dataset.json.ClassPathJsonDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Created by luislazaro on 22/2/17.
 * lalazaro@keedio.com
 * Keedio
 */
public class CassandraEmbServerTest {
    @Before
    public void before() throws Exception{
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        DataLoader dataLoader = new DataLoader("TestCluster", "localhost:9171");
        dataLoader.load(new ClassPathJsonDataSet("./src/test/resources/config/simpleDataSet.json"));
    }

    @After
    public void after() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    @Test
    public void shouldWork() throws Exception {
        String clusterName = "TestCluster";
        String host = "localhost:9771";
        Cluster cluster = HFactory.getOrCreateCluster(clusterName, host);
        Keyspace keyspace = HFactory.createKeyspace("beautifulKeyspaceName", cluster);
        assertThat(keyspace, notNullValue());
		/* and query all what you want */
    }
}
