package com.kaltura.Live.infra

import java.io.{IOException, InputStreamReader, BufferedReader, ByteArrayInputStream}
import java.util.zip.GZIPInputStream

import com.kaltura.Live.utils.{BaseLog, MetaLog}

object BlobExtractor extends Serializable with MetaLog[BaseLog]
{
     def metaLog = BlobExtractor

     def blobToLines( blob: java.nio.ByteBuffer ): List[String] =
     {
          var lines:List[String] = Nil

          if ( blob == null )
               return lines

          if ( blob.remaining() == 0 )
               return lines

          val blobBytes = new Array[Byte](blob.remaining() )
          blob.get(blobBytes)

          val bStream: ByteArrayInputStream = new ByteArrayInputStream(blobBytes)
          var gzipStream: GZIPInputStream = null
          var in: BufferedReader = null

          try
          {
               gzipStream = new GZIPInputStream(bStream)
               val reader: InputStreamReader = new InputStreamReader(gzipStream)
               in = new BufferedReader(reader)

               var continue: Boolean = true
               while (continue)
               {
                    val newLine = in.readLine
                    if ( newLine != null )
                         lines = lines :+ newLine
                    else
                         continue = false
               }

               lines
          }
          catch
          {
               case ex: IOException =>
               {
                    logger.error("Failed to read GZipInputStream" + ex.getMessage)
               }

               lines
          }
          finally
          {
               try
               {
                    if ( bStream != null )
                         bStream.close
                    if ( gzipStream != null )
                         gzipStream.close
                    if ( in != null )
                         in.close
               }
               catch
               {
                    case ex: IOException =>
                    {
                         logger.error("Failed to close GZipInputStream" + ex.getMessage)
                    }
               }
          }
     }
}
