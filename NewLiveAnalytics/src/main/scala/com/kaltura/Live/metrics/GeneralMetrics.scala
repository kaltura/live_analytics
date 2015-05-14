package com.kaltura.Live.metrics

import com.codahale.metrics.{Gauge, MetricRegistry}
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory.T
import com.kaltura.ip2location.Coordinate

import scala.collection.mutable.Queue

/**
 * Created by didi on 5/14/15.
 */


public class GeneralMetrics( metrics: MetricRegistry, name: String )
{
     metrics.register(MetricRegistry.name(classOf[GeneralMetrics], name, "size"), new TimeSinceLastCycleGauge)
}

class TimeSinceLastCycleGauge extends Gauge
{
     override def getValue() : Int =
     {
          0
     }
}