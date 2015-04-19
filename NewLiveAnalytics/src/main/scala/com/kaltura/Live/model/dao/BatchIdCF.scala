package com.kaltura.Live.model.dao

import eu.inn.binders._
import eu.inn.binders.cassandra._
import eu.inn.binders.naming.PlainConverter
import scala.concurrent.{Future, ExecutionContext}

case class BatchId( batch_id: Long ) extends Serializable

class BatchIdCF( session: com.datastax.driver.core.Session )
{
     import ExecutionContext.Implicits.global

     implicit val cache = new SessionQueryCache[PlainConverter](session)

     def getBatchIdIfFound() = cql"select * from current_batch WHERE batch_code=0".one[BatchId]

     def updateBatchId( batchId: Long) = cql"insert into current_batch (batch_code, batch_id) values (0, $batchId)".execute()
}
