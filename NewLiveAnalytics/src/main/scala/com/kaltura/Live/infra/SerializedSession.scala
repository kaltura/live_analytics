package com.kaltura.Live.infra

import com.datastax.driver.core.Cluster
import com.kaltura.Live.env.EnvParams

/**
 * Created by didi on 3/24/15.
 */
object SerializedSession extends Serializable
{
     val cluster = Cluster.builder().addContactPoint(EnvParams.cassandraAddress).build()
     val session = cluster.connect(EnvParams.kalturaKeySpace)
}
