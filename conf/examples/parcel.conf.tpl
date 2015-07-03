intel.taproot.analytics.metastore.connection-postgresql.url="jdbc:postgresql://"${intel.taproot.analytics.metastore.connection-postgresql.host}":"${intel.taproot.analytics.metastore.connection-postgresql.port}"/"${intel.taproot.analytics.metastore.connection-postgresql.database}
intel.taproot.analytics.metastore.connection=${intel.taproot.analytics.metastore.connection-postgresql}
intel.taproot.analytics.engine.titan.query {
storage {
  # query does use the batch load settings in titan.load
  backend = ${intel.taproot.analytics.engine.titan.load.storage.backend}
  hostname =  ${intel.taproot.analytics.engine.titan.load.storage.hostname}
  port =  ${intel.taproot.analytics.engine.titan.load.storage.port}
}
}