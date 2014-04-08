#!/bin/bash
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
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
function spec()
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
