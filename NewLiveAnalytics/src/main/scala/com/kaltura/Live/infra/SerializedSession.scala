package com.kaltura.Live.infra

import com.datastax.driver.core.Cluster
import com.kaltura.Live.model.Consts

/**
 * Created by didi on 3/24/15.
 */
object SerializedSession extends Serializable
{
     val cluster = Cluster.builder().addContactPoint(ConfigurationManager.get("cassandra.node_name")).build()
     val session = cluster.connect(Consts.KalturaKeySpace)
}
