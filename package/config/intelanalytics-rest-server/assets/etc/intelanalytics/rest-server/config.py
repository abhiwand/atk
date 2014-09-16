##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
"""
Requirements:
    Clouderas python cm-api http://cloudera.github.io/cm_api/
    working Cloudera manager with at least a single cluster
    Intel Analytics installation

This script queries Cloudera manager to get the host names of the machines running the following roles.
 -ZOOKEEPER server(the zookeeper role is actually called 'server')
 -HDFS name node
 -SPARK master
It also updates the spark-env.sh config in Cloudera manager with a necessary export of SPARK_CLASSPATH
needed for graph processing. The spark service config is re-deployed and the service is restarted. If the Intel Analytics
class path is already present no updates are done, the config is not deployed and the spark service is not restarted.

Command Line Arguments
    Every command line argument has a corresponding user prompt. If the command line argument is given the prompt will
    be skipped.
--host the cloudera manager host address. If this script is run on host managed by Cloudera manager we will try to get
    the host name from /etc/cloudera-scm-agent/config.ini

--port the Cloudera manager port. The port used to access the Cloudera manager ui. Defaults to 7180 if nothing is
    entered when prompted

--username The Cloudera manager user name. The user name for loging into Cloudera manager

--pasword The Cloudera manager pass word. Thye user name for loging into Cloudera manager

--cluster The Cloudera cluster we will pull and update config for. If Cloudera manager manages more than one cluster
    we need to know what cluster we will be updating and pulling our config for. Can give the display name of the
    cluster
--python The python exec you would like to use. Intel Analytics defaults to python. This is usually changed to the
    python 2.7 exec to allow users the ability to use ipython. The server and the client must be runnig the same
    python version
--restart Weather or not we will restart the spark service after setting the spark classpath. After the SPARK_CLASSPATH
    gets updated we deploy the new config but we also need to restart the spark service for the changes to take effect
    on all the master and worker nodes. This is left to the user to decide in case spark is currently busy running some
    jobs
--db-port
--db-username
--db-password

    TODO: Configure database when the configuration script is done.
"""

from cm_api.api_client import ApiResource
from cm_api.endpoints import hosts
from cm_api.endpoints import role_config_groups
from subprocess import call
import re, time, argparse

parser = argparse.ArgumentParser(description="Process cl arguments to avoid prompts in automation")
parser.add_argument("--host", type=str, help="Clouder Manager Host")
parser.add_argument("--port", type=int, help="Cloudera Manager Port")
parser.add_argument("--username", type=str, help="Cloudera Manager User Name")
parser.add_argument("--password", type=str, help="Cloudera Manager Password")
parser.add_argument("--cluster", type=str, help="Cloudera Manager Cluster Name if more than one cluster is managed by Cloudera Manager.")
parser.add_argument("--python", type=str, help="The name of the python executable to use. It must be in the path")
parser.add_argument("--restart", type=str, help="Weather or not to restart spark service after config changes")
parser.add_argument("--db-host", type=str, help="Database host name")
parser.add_argument("--db-port", type=str, help="Database port number")
parser.add_argument("--db", type=str, help="Database name")
parser.add_argument("--db-username", type=str, help="Database username")
parser.add_argument("--db-password", type=str, help="Database password")
args = parser.parse_args()

LIB_PATH = "/usr/lib/intelanalytics/graphbuilder/lib/ispark-deps.jar"
IAUSER = "iauser"


def user_info_prompt(message, default):
    """
    prompt the user for info if nothing is entered we default to the given default

    :param message: message to be displayed to the user
    :param default: the fallback value if the user doesn't give any input
    :return: either the default or whatever the user entered
    """
    response = raw_input(message)
    if response == "" or response is None:
        response = default
    return response

def get_arg(question, default, arg):
    """
    see if we value in the parsed argument arg if not prompt for the user for the required info. If an parsed
    argument is present that will returned and no prompt will be displayed

    :param question: question to display when prompting for input
    :param default: the dafault value if nothing is entered
    :param arg: the parsed argument from the command line
    :return: argument if it exist, user input, or the default value
    """
    return user_info_prompt(question + " defaults to '" + str(default) + "' if nothing is entered: ", default) \
        if arg is None else arg

def get_python_exec():
    """
    Get ask the user for the python exec the would like to use.

    :return: string with the python path exec name
    """
    return get_arg("\nWhat python executable would you like to use? It must be in the path. ", "python", args.python)


def select_cluster(clusters, command_line_cluster):
    """
    Prompt the user for cluster selection. The user will be displayed an indexed table of all the clusters managed
    by Cloudera manager. The user will select clusters index we will configure against

    if command_line_cluster is given we will try to find that cluster in the Cloudera manager instead of prompting

    :param clusters: List of all clusters in Cloudera manager
    :param command_line_cluster: the cluster name parsed from the command line
    :return: The cluster selected by the user or the cluster that maches the name parsed from the command line
    """
    cluster = None
    if command_line_cluster:
        for c in clusters:
            if c.displayName == command_line_cluster or c.name == command_line_cluster:
                cluster = c
    else:
        count = 1
        for c in clusters:
          print(str(count) + ": Cluster Name: {0:20} Version: {1}".format(c.name, c.version))
          count += 1
        cluster_index = input("Enter the clusters index number: ")
        print ("You picked cluster " + str(cluster_index))
        cluster = clusters[(cluster_index-1)]
    return cluster

def find_service(services, type):
    """
    Find a service handle, right now we only look for HDFS, ZOOKEEPER, and SPARK

    :param services: The list of services on the cluster
    :param type: the service we are looking for
    :return: service handle or None if the service is not found
    """
    for service in services:
        if service.type == type:
            return service
    return None

def get_service_names(roles):
    """
    Get all the role names. this is used when deploying configurations after updates. The role names are very long and
    look like this 'spark-SPARK_WORKER-207e4bfb96a401eb77c8a78f55810d31'. Used by the Cloudera api to know where the
    config is going to get deployed

    :param roles: list of roles from a service. example SPARK_WORKER, SPARK_MASTER
    :return: only the service names. will list of something like this 'spark-SPARK_WORKER-207e4bfb96a401eb77c8a78f'
    """
    return [role.name for role in roles]

def find_service_roles(roles, type):
    """
    Look for roles like spark_worker, spark_master, namenode, ... We will have duplicate roles depending on how many
    host are running the same role

    :param roles: list of service roles
    :param type: the type of role we are looking for, ie "SPARK_MASTER"
    :return: list of all roles matching the type
    """
    return [role for role in roles if role.type == type]

def get_role_host_names(api, roles):
    """
        get the host name for the all for the roles

    :param api: rest service handle
    :param roles: the list of service roles
    :return: list of machine host names
    """
    return [hosts.get_host(api, role.hostRef.hostId).hostname for role in roles]

def find_config(groups, group_name, config_name):
    """
    find a config among all the configs in Cloudera manager for the given group

    :param groups: list of configuration groups for a service
    :param group_name: The group name we are going to be searching in
    :param config_name: the configuration we will look for
    :return: The configuration value from Cloudera manager and the corresponding configuration group
    """
    for config_group in groups:
        if config_group.name == group_name:
            for name, config in config_group.get_config(view='full').items():
                if config.name == config_name:
                    if config.value is None:
                        return config.default, config_group
                    else:
                        return config.value, config_group
    return None, None

def find_ia_class_path(class_path):
    """
    find any current ia class path
    :param class_path: the full class path value from Cloudera manager
    :return: found intel analytics class path
    """
    return re.search('.*' + LIB_PATH + '.*', class_path)


def find_exported_class_path(spark_config_env_sh):
    """
    find any current class path
    :param spark_config_env_sh: all the text from the cloudera manager spark_env.sh
    :return: the entire line containing the exported class path
    """
    return re.search('SPARK_CLASSPATH=.*', spark_config_env_sh) if spark_config_env_sh else None

def find_class_path_value(spark_config_env_sh):
    """
    find the only the class path value nothing else

    :param spark_config_env_sh: all the text from the cloudera manager spark_env.sh
    :return: found class path value
    """

    #i search all groups after the match to find the one that only has the value
    find_class_path = re.search('SPARK_CLASSPATH=(\".*\"|[^\r\n]*).*', spark_config_env_sh)
    class_path = None
    #get only the value not the 'export SPARK_CLASSPATH' chaff. find the group that only has the export value
    if find_class_path is not None:
        for g in find_class_path.groups():
            find_only_exported_value = re.search('SPARK_CLASSPATH', g)
            if find_only_exported_value is not None:
                continue
            else:
                class_path = g.strip('"')
                break
    return class_path


def create_updated_class_path(current_class_path, spark_env):
    """
    create a string with our class path addition and any other class paths that currently existed

    :param current_class_path: the current class path value
    :param spark_env: the entire spark-env.sh config text from Cloudera manager
    :return:
    """

    if current_class_path is None:
        #if no class path exist append it to the end of the spark_env.sh config
        spark_class_path="SPARK_CLASSPATH=\"" + LIB_PATH + "\""
        return spark_env + "\n" + spark_class_path
    else:
        #if a class path already exist search and replace the current class path plus our class path in spark_env.sh
        #config
        spark_class_path="SPARK_CLASSPATH=\"" + current_class_path + ":" + LIB_PATH + "\""
        return re.sub('.*SPARK_CLASSPATH=(\".*\"|[^\r\n]*).*', spark_class_path, spark_env)


def poll_commands(service, command_name):
    """
    poll the currently running commands to find out when the config deployment and restart have finished

    :param service: service to pool commands for
    :param command_name: the command we will be looking for, ie 'Restart'
    """
    active = True
    while active:
        time.sleep(1)
        print " . ",
        commands = service.get_commands(view="full")
        if commands:
            for c in commands:
                if c.name == command_name:
                    active = c.active
                    break
        else:
            break


def deploy_config(service, roles):
    """
    deploy configuration for the given service and roles

    :param service: Service that is going to have it's configuration deployed
    :param roles: the roles that are going to have their configuration deployed
    :return:
    """
    print "Deploying config ",
    service.deploy_client_config(*get_service_names(roles))
    poll_commands(service, "deployClientConfig")

def restart_service(service):
    """
    restart the service
    :param service: service we are going to restart

    """
    print "\nYou need to restart " + service.name + " service for the config changes to take affect."
    service_restart = get_arg("Would you like to restart spark now? Type 'yes' to restart.", "no", args.restart)
    if service_restart is not None and service_restart.strip().lower() == "yes":
        print "Restarting " + service.name,
        service.restart()
        poll_commands(service, "Restart")


def update_spark_env(group, spark_config_env_sh):
    """
    update the park env configuration in Cloudera manager

    :param group: the group that spark_env.sh belongs too
    :param spark_config_env_sh: the current spark_env.sh value
    :return:
    """


    if spark_config_env_sh is None:
        spark_config_env_sh = ""

    #look for any current SPARK_CLASSPATH
    found_class_path = find_exported_class_path(spark_config_env_sh)

    if found_class_path is None:
        #no existing class path found
        print "No current SPARK_CLASSPATH set."

        updated_class_path = create_updated_class_path(found_class_path, spark_config_env_sh)

        print "Setting to: " + updated_class_path

        #update the spark-env.sh with our exported class path appended to whatever whas already present in spark-env.sh
        group.update_config({"SPARK_WORKER_role_env_safety_valve": updated_class_path})
        return True
    else:
        #found existing classpath
        found_class_path_value = find_class_path_value(spark_config_env_sh)
        print "Found existing SPARK_CLASSPATH: " + found_class_path_value

        #see if we our LIB_PATH is set in the classpath
        found_ia_class_path = find_ia_class_path(found_class_path_value)
        if found_ia_class_path is None:
            #no existing ia classpath
            print "No existing Intel Analytics class path found."
            updated_class_path = create_updated_class_path(found_class_path_value, spark_config_env_sh)
            print "Updating to: " + updated_class_path
            group.update_config({"SPARK_WORKER_role_env_safety_valve" : updated_class_path})
            return True
        else:
            #existing ia classpath
            print "Found existing Intel Analytics class path no update needed."
            return False
    return False

def get_hdfs_details(services):
    """
    We need various hdfs details to eventually get to the name node host name

    :param services: all the cluster services
    :return: name node host name
    """
    #get hdfs service details
    hdfs_service = find_service(services, "HDFS")
    if hdfs_service is None:
        print "no hdfs service found"
        exit(1)

    hdfs_roles = hdfs_service.get_all_roles()

    hdfs_namenode_roles = find_service_roles(hdfs_roles, "NAMENODE")

    hdfs_namenode_role_hostnames = get_role_host_names(api, hdfs_namenode_roles)

    hdfs_config_groups = role_config_groups.get_all_role_config_groups(api, hdfs_service.name, cluster.name)

    hdfs_namenode_port, _ = find_config(hdfs_config_groups, "hdfs-NAMENODE-BASE", "namenode_port")

    return hdfs_namenode_role_hostnames, hdfs_namenode_port

def get_zookeeper_details(services):
    """
    get the various zookeeper service details and eventually return the zookeeper host names

    :param services: all the cluster services
    :return: list of zookeeper host names
    """
    zookeeper_service = find_service(services, "ZOOKEEPER")
    if zookeeper_service is None:
        print "no zookeeper service found"
        exit(1)

    zookeeper_roles = zookeeper_service.get_all_roles()

    zookeeper_server_roles = find_service_roles(zookeeper_roles, "SERVER")

    zookeeper_server_role_hostnames = get_role_host_names(api, zookeeper_server_roles)

    zookeeper_config_groups = role_config_groups.get_all_role_config_groups(api, zookeeper_service.name, cluster.name)

    zookeeper_client_port, _ = find_config(zookeeper_config_groups, "zookeeper-SERVER-BASE", "clientPort")

    return zookeeper_server_role_hostnames, zookeeper_client_port

def get_spark_details(services):
    """
    Look for the spark master host name, spark master port, executor memory and update the spark_env.sh with the
    necessary class path to build graphs
    :param services: all the cluster services
    :return: spark master host name, port and executor max memory
    """
    spark_service = find_service(services, "SPARK")
    if spark_service is None:
       print "no spark service found"
       exit(1)

    spark_roles = spark_service.get_all_roles()

    spark_master_roles = find_service_roles(spark_roles, "SPARK_MASTER")

    spark_master_role_hostnames = get_role_host_names(api, spark_master_roles)

    spark_config_groups = role_config_groups.get_all_role_config_groups(api, spark_service.name, cluster.name)

    spark_config_executor_total_max_heapsize, _ = find_config(spark_config_groups, "spark-SPARK_WORKER-BASE",
                                                           "executor_total_max_heapsize")

    spark_config_master_port, _ = find_config(spark_config_groups, "spark-SPARK_MASTER-BASE", "master_port")

    #spark_config_env_sh, group = find_config(spark_config_groups, "spark-GATEWAY-BASE",
    #                               "spark-conf/spark-env.sh_client_config_safety_valve")
    spark_config_env_sh, group = find_config(spark_config_groups, "spark-SPARK_WORKER-BASE", "SPARK_WORKER_role_env_safety_valve")
    print spark_config_env_sh
    updated = update_spark_env(group, spark_config_env_sh)

    if updated and True:
        deploy_config(spark_service, spark_roles)
        restart_service(spark_service)

    return spark_master_role_hostnames, spark_config_executor_total_max_heapsize, spark_config_master_port

def get_db_details():

    print args
    db_host = get_arg("What is the database hostname?", "localhost", args.db_host)
    db_port = get_arg("What is the database hostname?", "5432", args.db_port)
    db = get_arg("What is the database name?", "ia-metastore", args.db)
    db_username = get_arg("What is the database name?", "ia-metastore", args.db_username)
    db_password = get_arg("What is the database name?", "somerandomtext", args.db_password)

    print os.urandom(32)
    #create postgres user with password
    #create postgres database
    #update /var/lib/pgsql/data/pg_hba.conf
    print call(["ls", "-l"])



def create_intel_analytics_config( hdfs_host_name, hdfs_namenode_port, zookeeper_host_names, zookeeper_client_port,
                                   spark_master_host, spark_master_port, spark_worker_memory, python_exec):
    """
    create a new application.conf file from the tempalte

    :param hdfs_host_name: hdfs host name
    :param zookeeper_host_names: zookeeper host names
    :param spark_master_host: spark master host
    :param spark_master_port: spakr master port
    :param spark_worker_memory: spark worker executor max memory
    :return:
    """
    print "\nCreating application.conf file from application.conf.tpl"
    config_file_tpl_path = "application.conf.tpl"
    config_file_path = "application.conf"

    print "Reading application.conf.tpl"
    config_tpl = open( config_file_tpl_path, "r")
    config_tpl_text = config_tpl.read()
    config_tpl.close()

    print "Updating configuration"
    #set fs.root
    config_tpl_text = re.sub(r'fs.root = .*', 'fs.root = "hdfs://' + hdfs_host_name[0] + ":" + hdfs_namenode_port + '/user/' + IAUSER + '"', config_tpl_text)
    #set titan zookeeper list titan.load.storage.hostname
    config_tpl_text = re.sub(r'titan.load.storage.hostname = .*',
                       'titan.load.storage.hostname = "' + ','.join(zookeeper_host_names) + '"', config_tpl_text)
    config_tpl_text = re.sub(r'[/]*titan.load.storage.port = .*',
                       'titan.load.storage.port = "' + zookeeper_client_port  + '"', config_tpl_text)
    #set spark master
    config_tpl_text = re.sub(r'spark.master = .*',
                       'spark.master = "spark://' + spark_master_host[0] + ':' + spark_master_port + '"', config_tpl_text)
    #set spark executor memory
    config_tpl_text = re.sub(r'spark.executor.memory = .*', 'spark.executor.memory = "' + spark_worker_memory + '"', config_tpl_text)

    #set python exec
    config_tpl_text = re.sub(r'[/]*python-worker-exec = .*', 'python-worker-exec = "' + python_exec + '"', config_tpl_text)

    print "Writing application.conf"
    config = open(config_file_path, "w")
    config.write(config_tpl_text)
    config.close()

#get the Cloudera manager host
cloudera_manager_host = args.host if args.host else None

if cloudera_manager_host is None:
    try:
        #look for in the Cloudera agent config.ini file before prompting the user
        #config dir for Cloudera agent /etc/cloudera-scm-agent
        cloudera_agent_config = open("/etc/cloudera-scm-agent/config.ini", "r")
        cloudera_manager_host = re.search('(?<=server_host=).*',cloudera_agent_config.read()).group(0)
        cloudera_agent_config.close()
    except IOError:
        cloudera_manager_host = user_info_prompt("What the hostname of your Cloudera Manager instance? ","localhost")


cloudera_manager_port = cloudera_manager_username = get_arg("What port is Cloudera manager listening on?", 7180,
                                                            args.port)

cloudera_manager_username = get_arg("What is the Cloudera manager username?", "admin", args.username)

cloudera_manager_password = get_arg("What is the Cloudera manager password?", "admin", args.password)


#rest service handle
api = ApiResource(cloudera_manager_host, server_port=cloudera_manager_port, username=cloudera_manager_username,
                  password=cloudera_manager_password)

#the user picked cluster or the only cluster managed by cloudera manager
cluster = None
# Get a list of all clusters
clusters=api.get_all_clusters()

#if we have more than one cluster prompt the user to pick a cluster
if len(clusters) > 1:
    cluster = select_cluster(clusters, args.cluster)
elif len(clusters) == 1:
    cluster = clusters[0]

if cluster:

    #get a list of the services running on the this cluster
    services = cluster.get_all_services()

    #get hdfs name node host name
    hdfs_namenode_role_host_names, hdfs_namenode_port = get_hdfs_details(services)

    #get zookeeper host names
    zookeeper_server_role_host_names, zookeeper_client_port = get_zookeeper_details(services)

    #get spark service details
    spark_master_role_host_names, spark_config_executor_total_max_heapsize, spark_config_master_port = get_spark_details(services)

    #get python exec
    python_exec = get_python_exec()

    get_db_details()

    #write changes to our config
    create_intel_analytics_config(hdfs_namenode_role_host_names, hdfs_namenode_port, zookeeper_server_role_host_names,
                                  zookeeper_client_port, spark_master_role_host_names, spark_config_master_port,
                                  spark_config_executor_total_max_heapsize, python_exec)
else:
    print "No cluster selected"
    exit(1)


