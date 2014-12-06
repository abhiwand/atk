#!/bin/bash
#This script will package a tar into a deb and rpm package.
#The tar has to be built with the entire directory structrue of the linux file system
#if the file needs to be installed in /usr/lib/intelanalytics/myfiles
#the tar should be created with that  directory structure
#The tar will be extracted to both deb and rpm dir wich have all the boiler plate files 
#necessary for packing.
#Arguments
#	--package-name the name of the package that we will be creating. the given package must have a config folder
#	--build any build identifier
source common.sh 
pwd

TEMP=`getopt -o p:b:t:v:m: --long make-package:,package-name:,build:,tar:,version: -n 'package.sh' -- "$@"`

if [ $? != 0 ]; then echo "Terminating .." >&2 ; exit 1; fi

eval set -- "$TEMP"
echo "$@"
config="config"
packages="deb rpm pypi csd parcel"
#version="0.8.0"
build="1"
buildDir=${SCRIPTPATH}/tarballs

while true; do
        case "$1" in
                -m|--make-package)
                        echo "make-page: '$2'"
                        makePackage=$2
                        shift 2;;
                -p|--package-name)
                        echo "package-name: '$2'"
                        packageName=$2
                        shift 2;;
                -b|--build)
                        echo "build: '$2'"
                        build=$2
                        shift 2;;
                -t|--tar)
                        echo "tar file: '$2'"
                        tarFile=$2
                        shift 2;;
		        -v|--version)
                        echo "version: '$2'"
                        version=$2
                        shift 2;;
                --) shift; break;;
                *) echo "error"; exit 1;;
        esac
done

function usage()
{
        echo "Usage: package -p or --package-name <the name of the packge to build> -b or --build <some build id> -t or --tar <path to tar file> "
        exit 1;
}

if [ "$packageName" == "" ]; then
       	log "no package name specified"
        usage
fi

if [ "$build" == "" ]; then
        log "no build id specified default id: $build"
        usage
fi

if [ "$tarFile" == "" ]; then
        log "no tar file specified"
       # usage
fi

if [ "$version" == "" ]; then
	log "not version specified"
	usage
fi

configDir="$config/$packageName"

export BUILD_NUMBER=$build
export TIMESTAMP=$(date)
export VERSION=$version
export PACKAGE_NAME=$packageName
export LICENSE=Apache
export GROUP="Intel Analytics"
export BUILD_DIR=$buildDir/$PACKAGE_NAME
#do a verbose extract of the tar file to get a list of all the files in the tar file


if [ $makePackage == "yes" ]; then
    log "make package $PACKAGE_NAME"
    if [ -f $configDir/package.sh ]; then
        cleanBuild $PACKAGE_NAME
        #make build directory
        mkdir -p $BUILD_DIR/$PACKAGE_NAME
        $configDir/package.sh ${PACKAGE_NAME}

    fi
    tarFile=$BUILD_DIR/../$PACKAGE_NAME-source.tar.gz
fi
tarFile=$BUILD_DIR/../$PACKAGE_NAME-source.tar.gz
echo $tarFile
tarFiles $tarFile
for package in $packages
do 
	if [ -f $configDir/$package.sh  ]; then
			log "found $package config"
			$configDir/$package.sh $packageName $tarFile $version
		else
			log "no package config found for: $package"
	fi
done

#cleanBuild $PACKAGE_NAME
