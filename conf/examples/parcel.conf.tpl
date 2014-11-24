intel.analytics.api.port
intel.analytics.metastore.connection-postgresql.host="invalid-postgresql-host"
intel.analytics.metastore.connection-postgresql.port=5432
intel.analyticsmetastore.connection-postgresql.database="ia-metastore"
intel.analytics.metastore.connection-postgresql.username="iauser"
intel.analytics.metastore.connection-postgresql.password="myPassword"
intel.analytics.metastore.connection-postgresql.url="jdbc:postgresql://"${intel.analytics.metastore.connection-postgresql.host}":"${intel.analytics.metastore.connection-postgresql.port}"/"${intel.analytics.metastore.connection-postgresql.database}
intel.analytics.metastore.connection=${intel.analytics.metastore.connection-postgresql}