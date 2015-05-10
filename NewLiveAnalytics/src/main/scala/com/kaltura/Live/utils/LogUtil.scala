package com.kaltura.Live.utils

import org.slf4j.LoggerFactory

/**
 * Created by didi on 3/23/15.
 */
trait MetaLog[BaseLog] {
     val logger = LoggerFactory.getLogger(getClass)
}

trait BaseLog {
     def metaLog: MetaLog[BaseLog]
     def logger = metaLog.logger
}