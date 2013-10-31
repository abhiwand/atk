#!/bin/bash
#
# Big WIP: this is only the workflow description
#
# Implementation script will follow
#

# Input is a unique cluster id from frontend user registration

# Prefix=IntelAnalytics-${id}

# Retrieve cluster master node AMI image id

# Retrieve cluster slave  node AMI image id

# Retrieve cluster vpc id

# Retrieve cluster Group IAM

# Retrieve cluster User IAM

# Create cluster security groups

# Create cluster placement group

# Create cluster subnet

# Associate cluster subnet to VPC router

# Prepare ssh keypair for password less login

# Prepare cluster hosts file password less login

# Launch 4 instances into the placement group

# Execute post launch script: should leverage the User Data metadata
# upon instance launching
# - format IS disks
# - mount FS
# - update cluster config
# - bring up the cluster
