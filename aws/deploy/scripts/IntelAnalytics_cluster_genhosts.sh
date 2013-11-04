#!/bin/bash
# Description: Used for preparing hosts file for the target cluster
source IntelAnalytics_cluster_env.sh

IA_generate_hosts_file ${1} ${2} ${IA_CLUSTERS}
