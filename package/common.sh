#!/bin/bash

#get the script path
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

#some sensible defaults for some of the fields in all these control files
BUILD_DEPENDS="debhelper (>= 8.0.0)"
MAINTAINER="BDA <BDA@intel.com>"
STANDARDS_VERSION="3.9.2"
ARCH="any"
SECTION="libs"
PRIORITY="extra"
COMPAT=9

function log ()
{
 	echo "-##LOG##-$1"
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
			echo "/$fullPath" >> FILES.LOG
		fi
	done
	export TAR_FILES=FILES.LOG
}

function expandTarDeb()
{
	cp $tarFile $SCRIPTPATH/${packageName}_${version}.orig.tar.gz
	tar -xvf $tarFile -C $SCRIPTPATH/deb
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
		echo "Depends: $DEPENDS"
	fi
	echo "Description: $DESCRIPTION"
}

function debChangeLog()
{
#	echo "$PACKAGE_NAME ($VERSION) UNRELEASED; urgency=low"
#	echo "SUBJECT: $SUBJECT"
#	echo ""
#	echo "  * Initial release (Closes: #nnnn)"
#	for file in `cat $TAR_FILES`;
#	do
		#echo "  * $file: ribet"
#	done#
#	echo ""
#	echo " -- $MAINTAINER  Tue, 08 Apr 2014 18:47.41 +0000"
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
		echo "$file $file"
	done
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
