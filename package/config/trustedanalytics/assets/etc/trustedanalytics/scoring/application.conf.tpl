# Archive declaration
trustedanalytics.atk.component.archives {
  scoring-engine {
    parent = "scoring-interfaces"
    class = "org.trustedanalytics.atk.scoring.ScoringServiceApplication"
   }
}

trustedanalytics.scoring-engine {
  archive-tar = "hdfs://spbathi-ws.eg.intel.com:8020/user/taproot/test1.tar"
}

trustedanalytics.atk {
  scoring {
    identifier = "ia"
    host = "127.0.0.1"
    port = 9100
    default-count = 20
    default-timeout = 30s
    request-timeout = 29s
    logging {
      raw = false
      profile = false
    }
  }
}



