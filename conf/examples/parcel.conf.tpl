intel.analytics.metastore.connection-postgresql.url="jdbc:postgresql://"${intel.analytics.metastore.connection-postgresql.host}":"${intel.analytics.metastore.connection-postgresql.port}"/"${intel.analytics.metastore.connection-postgresql.database}
intel.analytics.metastore.connection=${intel.analytics.metastore.connection-postgresql}
intel.analytics.engine.titan.query {
storage {
  # query does use the batch load settings in titan.load
  backend = ${intel.analytics.engine.titan.load.storage.backend}
  hostname =  ${intel.analytics.engine.titan.load.storage.hostname}
  port =  ${intel.analytics.engine.titan.load.storage.port}
}
}