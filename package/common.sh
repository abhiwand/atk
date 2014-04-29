#!/bin/bash

#get the script path
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

URL="graphtrial.intel.com"
MAINTAINER="BDA <BDA@intel.com>"

#deb build defaults
#some sensible defaults for some of the fields in all these control files
BUILD_DEPENDS="debhelper (>= 9.0.0)"
DEPENDS="\${misc:Depends}"
STANDARDS_VERSION="3.9.3"
ARCH="any"
SECTION="libs"
PRIORITY="extra"
COMPAT=9

#rpm build defaults
PROVIDES=$PACKAGE_NAME
RELEASE=$BUILD_NUMBER
SOURCE="$PACKAGE_NAME-$VERSION.tar.gz"


function log()
{
 	echo "-##LOG##-$1"
}

function deleteOldBuildDirs()
{
	echo "delete old build dirs"
	echo "rm -rf $SCRIPTPATH/$PACKAGE_NAME-$VERSION"
	rm -rf $SCRIPTPATH/$PACKAGE_NAME-$VERSION
}

function tarFiles()
{
	tar -xvf $1 > TAR.LOG
	rm FILES.LOG
	for path in `cat TAR.LOG`;
	do
		fullPath=$path
		fileName=${path##*/}
		if [ "$fileName" != "" ]; then
			echo $fullPath | sed 's/^.\//\//g' >> FILES.LOG
		fi
	done
	export TAR_FILES=FILES.LOG
}

function expandTarDeb()
{
	packageTar=${packageName}_${version}.orig.tar.gz
	rm -rf $packageTar
	
	rm -rf $SCRIPTPATH/repack/
	log "repack source tar gz with packageName-version parent file"
	mkdir -p $SCRIPTPATH/repack/${packageName}-${version}
	
	tar -xvf $tarFile -C $SCRIPTPATH/repack/${packageName}-${version}

	pushd $SCRIPTPATH/repack/
	tar -pczf ../$packageTar  ${packageName}-${version}/
	popd
	
	log "untar"
	tar -xvf $SCRIPTPATH/${packageTar} -C $SCRIPTPATH
}

#the deb control is the deb packages meta data file it's kind of like the rpm spec file
function debControl()
{
	echo "Source: $SOURCE"
	echo "Priority: $PRIORITY"
	echo "Maintainer: $MAINTAINER"
	echo "Build-Depends: $BUILD_DEPENDS"
	echo "Standards-Version: $STANDARDS_VERSION"
	echo "Section: $SECTION"
	echo ""
	echo "Package: $PACKAGE_NAME"
	echo "Architecture: $ARCH"
	if [ ! -z "$DEPENDS" ]; then
		echo "Depends: $DEPENDS, \${misc:Depends}"
	fi
	if [ ! -z "$RECOMMENDS" ];then
		echo "Recommends: $RECOMMENDS"
	fi
	echo "Description: $SUMMARY"
	echo " $DESCRIPTION"
}

function debCopyright()
{
	echo "TC package $BUILD_NUMBER $TIMESTAMP $MAINTAINER"
	echo ""
	echo "Copyright:"
	echo ""
	echo "Copyright (C) 2014 Intel Corporation"
	echo ""
	echo "License:"
	echo ""
	echo "All Rights reserved."
}

function debChangeLog()
{
	dch --create -M -v $version --package $packageName "Initial release. (Closes: #XXXXXX)"
}

#not much explanation is given for this file with a magical number for the time being it's  defaulted to 9
function debCompat()
{
	echo $COMPAT
}

#list of files that
function debInstall()
{
	for file in `cat $TAR_FILES`;
	do
#		bleh="/usr/lib/intelanalytics/rest-server/api-server.jar"
		local fileName=${file##*/}

		installDir=$(echo $file | sed "s/$fileName/ /g")

		echo "$file $installDir"
	done
}

function debRules()
{
	if [ -z "$RULEOPT" ]; then
		RULEOPT=""
	fi
	echo "#!/usr/bin/make -f"
	echo "# Uncomment this to turn on verbose mode."
	if [ ! -z "$RULESSETUP" ]; then
		echo $RULESSETUP
	fi
	echo "#export DH_VERBOSE=1"
	echo "%:"
	echo -e "\tdh \$@ $RULEOPT"
}

function rpmSpec()
{

echo "Name: $PACKAGE_NAME"
echo "Provides: $PROVIDES"
echo "Summary: $SUMMARY"
echo "License: $LICENSE"
echo "Version: $VERSION"
echo "Group: $GROUP"
if [ ! -z "$REQUIRES" ];then
	echo "Requires: $REQUIRES"
fi
if [ ! -z "$PREFIX" ];then
	echo "Prefix: $PREFIX"
fi
echo "Release: $RELEASE"
echo "Source: $SOURCE"
if [ ! -z "$URL" ]; then
	echo "URL: $URL"
fi
echo "%description"
echo $DESCRIPTION
echo "%define TIMESTAMP %(echo $TIMESTAMP)"
echo "%define TAR_FILE %(echo $TAR_FILE)"
echo "%build"
echo " cp %{TAR_FILE} %{_builddir}/files.tar.gz"

echo "%install"

echo " rm -rf %{buildroot}"

echo " mkdir -p %{buildroot}"

echo " mv files.tar.gz %{buildroot}/files.tar.gz"

echo " tar -xvf %{buildroot}/files.tar.gz -C %{buildroot}"

echo " rm %{buildroot}/files.tar.gz"

echo "%clean"

echo "%post"

echo "%postun"

echo "%files"
cat $TAR_FILES


}

