package com.kaltura.Live.model.dao

import eu.inn.binders._
import eu.inn.binders.cassandra._
import eu.inn.binders.naming.PlainConverter
import scala.concurrent.{Future, ExecutionContext}

case class LoggedFile( file_id: String, insert_time: java.util.Date, var batch_id: Long ) extends Serializable
{
     def setBatchId( batchId: Long ) = this.copy(batch_id = batchId)
}

class LoggedFilesCF( session: com.datastax.driver.core.Session ) extends Serializable
{
     import ExecutionContext.Implicits.global

     implicit val cache = new SessionQueryCache[PlainConverter](session)

     def selectLoggedFiles( batchId: Long ) : Future[Iterator[LoggedFile]] = cql"select * from log_files where batch_id=$batchId".all[LoggedFile]

     def update( loggedFile: LoggedFile ) = cql"insert into log_files (file_id, insert_time, batch_id) values (?, ?, ?)".bind(loggedFile).execute()
}

