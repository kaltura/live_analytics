package com.kaltura.Live.infra

import com.datastax.driver.core.{SocketOptions, ConsistencyLevel, QueryOptions, Cluster}
import com.kaltura.Live.model.Consts

/**
 * Created by didi on 3/24/15.
 */
object SerializedSession extends Serializable
{
     val socketOptions = new SocketOptions().setReadTimeoutMillis(100000);
     lazy val cluster = Cluster.builder().addContactPoint(ConfigurationManager.get("cassandra.node_name")).withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM)).withSocketOptions(socketOptions).build()
     lazy val session = cluster.connect(Consts.KalturaKeySpace)
}

