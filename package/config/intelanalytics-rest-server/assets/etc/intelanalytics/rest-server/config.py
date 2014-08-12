from cm_api.api_client import ApiResource
from cm_api.endpoints import hosts
from cm_api.endpoints import role_config_groups
import re

clouderaManagerHost = None
clouderaAgentConfig = None
try:
    #/etc/cloudera-scm-agent
    clouderaAgentConfig = open("/etc/cloudera-scm-agent/config.ini", "r")
    clouderaManagerHost = re.search('(?<=server_host=).*',clouderaAgentConfig.read()).group(0)
    clouderaAgentConfig.close()
except IOError:
    clouderaManagerHost = raw_input("What the hostname of your cloudera manager instance? ")


clouderaManagerPort = raw_input("What port is Cloudera manager listening on? defaults to 7180 if nothing is entered")
if clouderaManagerPort == "" or clouderaManagerPort == None:
    clouderaManagerPort = 7180

api = ApiResource(clouderaManagerHost, server_port=clouderaManagerPort, username="admin", password="admin")

#the user picked cluster or the only cluster managed by cloudera manager
cluster = None
# Get a list of all clusters
clusters=api.get_all_clusters()
#hold all hdfs configs

def select_cluster(clusters):
    count = 1
    for c in clusters:
      print(str(count) + ": Cluster Name: {0:20} Version: {1}".format(c.name, c.version))
      count += 1
    cluster = input("Enter the clusters index number: ")
    print ("you picked cluster " + str(cluster))
    return clusters[(cluster-1)]

def find_service(services, type):
    for service in services:
        if service.type == type:
            return service
    return None

def find_hdfs_service(services):
    return find_service(services, "HDFS")

def find_zookeeper_service(services):
    return find_service(services, "ZOOKEEPER")

def find_spark_service(services):
    return find_service(services, "SPARK")

def find_service_roles(roles, type):
    foundRoles = []
    for role in roles:
        if role.type == type:
            foundRoles.append(role)
    return foundRoles

def get_role_hostnames(api, roles):
    hostnames = []
    for role in roles:
        hostnames.append(hosts.get_host(api, role.hostRef.hostId).hostname)
    return hostnames

def find_config(groups, groupName, configName):
    for configGroup in groups:
        if configGroup.name == groupName:
            for name, config in configGroup.get_config(view='full').items():
                if config.name == configName:
                    if config.value == None:
                        return config.default
                    else:
                        return config.value

def get_hdfs_details(services):
    #get hdfs service details
    hdfs_service = find_hdfs_service(services)

    hdfs_roles = hdfs_service.get_all_roles()

    hdfs_namenode_roles = find_service_roles(hdfs_roles, "NAMENODE")

    hdfs_namenode_role_hostnames = get_role_hostnames(api, hdfs_namenode_roles)

    return hdfs_service, hdfs_roles, hdfs_namenode_roles, hdfs_namenode_role_hostnames

def getZookeeperDetails(services):
    zookeeper_service = find_zookeeper_service(services)

    zookeeper_roles = zookeeper_service.get_all_roles()

    zookeeper_server_roles = find_service_roles(zookeeper_roles, "SERVER")

    zookeeper_server_role_hostnames = get_role_hostnames(api, zookeeper_server_roles)

    return zookeeper_service, zookeeper_roles, zookeeper_server_roles, zookeeper_server_role_hostnames

def getSparkDetails(services):
    spark_service = find_spark_service(services)

    spark_roles = spark_service.get_all_roles()

    spark_master_roles = find_service_roles(spark_roles, "SPARK_MASTER")

    spark_master_role_hostnames = get_role_hostnames(api, spark_master_roles)

    spark_config_groups = role_config_groups.get_all_role_config_groups(api, spark_service.name, cluster.name)

#    for configGroup in spark_config_groups:
#        print configGroup
#        for name, config in configGroup.get_config(view='full').items():
#            print name


    spark_config_executor_total_max_heapsize = find_config(spark_config_groups, "spark-SPARK_WORKER-BASE",
                                                           "executor_total_max_heapsize")

    spark_config_master_port = find_config(spark_config_groups, "spark-SPARK_MASTER-BASE", "master_port")

    sparkConfigEnvSh = find_config(spark_config_groups, "spark-GATEWAY-BASE",
                                   "spark-conf/spark-env.sh_client_config_safety_valve")

    print sparkConfigEnvSh
    return spark_service, spark_roles, spark_master_roles, spark_master_role_hostnames, spark_config_groups, \
           spark_config_executor_total_max_heapsize, spark_config_master_port

def updateIntelAnalyticsConfig( hdfsHost, zookeeperHost, sparkHost, sparkPort, sparkMemory):
    IAUSER = "iauser"
    configFileTplPath = "application.conf.tpl"
    configFilePath = "application.conf"

    configTpl = open( configFileTplPath, "r")
    configTplText = configTpl.read()
    configTpl.close()

    #set fs.root
    configTplText = re.sub(r'fs.root = .*', 'fs.root = "hdfs://' + hdfsHost[0] + '/user/iauser"', configTplText)
    #set titan zookeeper list titan.load.storage.hostname
    configTplText = re.sub(r'titan.load.storage.hostname = .*',
                       'titan.load.storage.hostname = "' + ','.join(zookeeperHost) + '"', configTplText)
    #set spark master
    configTplText = re.sub(r'spark.master = .*',
                       'spark.master = "spark://' + sparkHost[0] + ':' + sparkPort + '"', configTplText)
    #set spark executor memory
    configTplText = re.sub(r'spark.executor.memory = .*', 'spark.executor.memory = "' + sparkMemory + '"', configTplText)

    config = open(configFilePath, "w")
    config.write(configTplText)
    config.close()

#if we have more than one cluster prompt the user to pick a cluster
if len(clusters) > 1:
    cluster = select_cluster(clusters)

    services = cluster.get_all_services()

    #get hdfs service details
    hdfsService, hdfsRoles, hdfsNamenodeRoles, hdfsNamenodeRoleHostnames = get_hdfs_details(services)

    #get zookeeper service details
    zookeeperService, zookeeperRoles, zookeeperServerRoles, zookeeperServerRoleHostnames = getZookeeperDetails(services)

    #get spark service details
    sparkService, sparkRoles, sparkMasterRoles, sparkMasterRoleHostnames, sparkConfigGroups, \
    sparkConfigExecutorTotalMaxHeapsize, sparkConfigMasterPort = getSparkDetails(services)

    #write changes to our config
    updateIntelAnalyticsConfig(hdfsNamenodeRoleHostnames, zookeeperServerRoleHostnames, sparkMasterRoleHostnames,
                               sparkConfigMasterPort, sparkConfigExecutorTotalMaxHeapsize)

elif len(clusters) == 1:
    #we only have a single cluster in Cloudera manager
    cluster = clusters[0]

    services = cluster.get_all_services()

    hdfsService, hdfsRoles, hdfsNamenodeRoles, hdfsNamenodeRoleHostnames = get_hdfs_details(services)

    zookeeperService, zookeeperRoles, zookeeperServerRoles, zookeeperServerRoleHostnames = getZookeeperDetails(services)

    sparkService, sparkRoles, sparkMasterRoles, sparkMasterRoleHostnames, sparkConfigGroups, \
                                sparkConfigExecutorTotalMaxHeapsize, sparkConfigMasterPort = getSparkDetails(services)

    updateIntelAnalyticsConfig(hdfsNamenodeRoleHostnames, zookeeperServerRoleHostnames, sparkMasterRoleHostnames,
                               sparkConfigMasterPort, sparkConfigExecutorTotalMaxHeapsize)



