package com.kaltura.Live.model.dao

import eu.inn.binders._
import eu.inn.binders.cassandra._
import eu.inn.binders.naming.PlainConverter
import scala.concurrent.{Future, ExecutionContext}

case class LoggedData( file_id: String, data: java.nio.ByteBuffer ) extends Serializable

class LoggedDataCF( session: com.datastax.driver.core.Session ) extends Serializable
{
     import ExecutionContext.Implicits.global

     implicit val cache = new SessionQueryCache[PlainConverter](session)

     def selectFile( fileId: String ) = cql"select * from log_data where file_id = $fileId".one[LoggedData]
}
