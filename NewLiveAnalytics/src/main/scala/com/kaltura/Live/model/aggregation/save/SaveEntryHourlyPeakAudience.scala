package com.kaltura.Live.model.aggregation.save

import com.datastax.driver.core.Cluster
import com.kaltura.Live.env.EnvParams
import com.kaltura.Live.infra.SerializedSession
import com.kaltura.Live.model.LiveEvent
import com.kaltura.Live.model.dao.EntryHourlyPeakAudienceCF

import scala.concurrent.Future

/**
 * Created by didi on 4/2/15.
 */
//class SaveEntryHourlyPeakAudience extends Serializable
//{
////     val cluster = Cluster.builder().addContactPoint(EnvParams.cassandraAddress).build()
////     val session = cluster.connect(EnvParams.kalturaKeySpace)
//
//
//
//     def update( event: LiveEvent ): (Future[Unit], Future[Unit]) =
//     {
//          val entryHourlyPeakAudienceCF = new EntryHourlyPeakAudienceCF(SerializedSession.session) // take it out into the class variables
//
//          val updateAudienceResult = entryHourlyPeakAudienceCF.updateAudience(event.entryId, event.eventTime, event.alive)
//          val updateDVRAudienceResult = entryHourlyPeakAudienceCF.updateDVRAudience(event.entryId, event.eventTime, event.alive)
//
//          val ret = (updateAudienceResult, updateDVRAudienceResult)
//          ret
//     }
//}
