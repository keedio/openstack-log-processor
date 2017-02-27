package com.keedio.flink.integration

import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.{Cluster, ProtocolVersion, ResultSet}
import com.keedio.flink.EmbeddedCassandraServer
import com.keedio.flink.dbmodels.PojoSampleJava
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, ClusterBuilder}
import org.hamcrest.MatcherAssert._
import org.hamcrest.Matchers._
import org.junit.{After, Assert, Test}

/**
  * Created by luislazaro on 23/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
class CassandraConnectorWithJavaPojoTest {
  val embeddedCassandraServer = new EmbeddedCassandraServer("redhatpoc.cql", "redhatpoc")
  val session = embeddedCassandraServer.getSession

  @After
  def after() = {
    embeddedCassandraServer.cleanupServer()
  }

  /**
    * Execution environment is scala api.
    *
    * @return
    */
  @Test
  def cassandraConntectWithJavaPojoSinkTest = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val list: List[PojoSampleJava] = List(new PojoSampleJava("1", "a"), new PojoSampleJava("2", "b"), new PojoSampleJava("3", "c"))
    val source: DataStream[PojoSampleJava] = env.fromCollection(list)

    CassandraSink.addSink(source.javaStream)
      .setClusterBuilder(new ClusterBuilder() {
        override def buildCluster(builder: Builder): Cluster = {
          builder.addContactPoint("127.0.0.1")
            .withPort(9142)
            .withProtocolVersion(ProtocolVersion.V3)
            .build()
        }
      })
      .build()
    env.execute()

    val result: ResultSet = session.execute("select * from redhatpoc.cassandraconnectorexample WHERE id='2';")
    assertThat(result.iterator.next.getString("text"), is("b"))
    val result1: ResultSet = embeddedCassandraServer.getSession.execute("select * from redhatpoc.cassandraconnectorexample WHERE id='3';")
    Assert.assertEquals(result1.iterator.next.getString("text"), "c")
  }
}
