package com.kaltura.Live.env

object EnvParams
{
     val sparkMaxCores: String = "40"

     val sparkExecutorMem: String = "8g"

     val sparkParallelism: String = "20"

     val sparkAddress = "spark://pa-ana1:7077"
     val cassandraAddress = "192.168.31.91"
     val kalturaKeySpace = "kaltura_live"

     val ip2locationFileName = "/opt/kaltura/data/geoip/IP-COUNTRY-CITY-ISP.BIN"

     val maxProcessFilesPerCycle = 50

     val bufferTimeResolution = 100L

     val repositoryHome = "/opt/kaltura/lib/"
//     var _partner = ""
//
//     var _cassandraAddress = "192.168.31.91:9160"
//
//     var _checkConsistency = false
//
//     var _ipsListFileName = "/opt/ips.txt"// "/home/didi/Workspace/Data/ips.txt"
//
//     var _nginxURL = "http://192.168.11.133/api_v3/index.php"
//
//     def _simTimeCycle = 3600000
//
//     def _minTimeResolution = 10000
//
//     def _partnerStartId = 100
//
//     var _entries: Vector[String] = Vector.empty
//
//     var _nPartners = 10 //10
//
//     var _nEntries = 10 //10
//
//     var _nReferrers = 5 //5
//
//     var _nLocations = 40 //40
//
//     var _nBaseSessions = 4 //5
//
//     var _bitRate = 100
//
//     var _bufferTime = 0
//
//     var _nMaxDeltaSessions = 0 // 3
//
//     def _timeReSyncThreshold = 3000
//
//     def _timeReSyncRemainMargin = 1000
//
//     var _resetSim: Boolean = true


}
