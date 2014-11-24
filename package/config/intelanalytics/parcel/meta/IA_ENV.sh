#!/bin/bash

echo "IA env sh"

export ATK_ENV="BLEH"

export ATK_LAUNCHER_DIR="$PARCELS_ROOT/$PARCEL_DIRNAMES/usr/lib/intelanalytics/rest-server"
export ATK_LAUNCHER_LIB_DIR="$PARCELS_ROOT/$PARCEL_DIRNAMES/usr/lib/intelanalytics/rest-server/lib"
export ATK_SPARK_DEPS_DIR="$PARCELS_ROOT/$PARCEL_DIRNAMES/usr/lib/intelanalytics/graphbuilder/lib"
export ATK_CONFIG_DIR="$PARCELS_ROOT/$PARCEL_DIRNAMES/etc/intelanalytics/rest-server"
export ATK_LOGBACK_JARS="$ATK_LAUNCHER_LIB_DIR/logback-classic-1.1.1.jar:$ATK_LAUNCHER_LIB_DIR/logback-core-1.1.1.jar"
export ATK_CLASSPATH="$ATK_CONFIG_DIR:$ATK_LOGBACK_JARS:$ATK_LAUNCHER_DIR/launcher.jar:/etc/hbase/conf:/etc/hadoop/conf"
