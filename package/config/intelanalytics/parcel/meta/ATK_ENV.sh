#!/bin/bash

echo "ATK env sh"



export ATK_PARCEL_HOME="$PARCELS_ROOT/INTELANALYTICS"

export ATK_LAUNCHER_DIR="$ATK_PARCEL_HOME/usr/lib/intelanalytics/rest-server"

export ATK_LAUNCHER_LIB_DIR="$ATK_PARCEL_HOME/usr/lib/intelanalytics/rest-server/lib"

export ATK_SPARK_DEPS_DIR="$ATK_PARCEL_HOME/usr/lib/intelanalytics/graphbuilder/lib"

export ATK_SPARK_DEPS_JAR="ispark-deps.jar"

export ATK_CONFIG_DIR="$ATK_PARCEL_HOME/etc/intelanalytics/rest-server"

export ATK_LOGBACK_JARS="$ATK_LAUNCHER_LIB_DIR/logback-classic-1.1.1.jar:$ATK_LAUNCHER_LIB_DIR/logback-core-1.1.1.jar"

alias python="$PARCELS_ROOT/INTELANALYTICS/usr/bin/python2.7"

export ATK_CLASSPATH="$CONF_DIR:$ATK_CONFIG_DIR:$ATK_LOGBACK_JARS:$ATK_LAUNCHER_DIR/launcher.jar:/etc/hbase/conf:/etc/hadoop/conf"

export ATK_TEMP="$PARCELS_ROOT/INTELANALYTICS/tmp"