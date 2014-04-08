#!/bin/bash

function spec()
{

echo "Name: $NAME"
echo "Provides: $NAME"
echo "Summary: $SUMMARY"
echo "License: $LICENSE"
echo "Version: $VERSION"
echo "Group: $GROUP"
echo "Requires: $REQUIRES"
echo "Prefix: /usr/lib/intelanalytics"
echo "Release: %{?BUILD_NUMBER}"
echo "Source: intelanalytics-0.8.0.tar.gz"
#URL: <TODO>
echo "%description"
echo $DESCRIPTION
echo "%define TIMESTAMP %(echo $TIMESTAMP)"
echo "%define TAR_FILE %(echo $TAR_FILE)"
echo "%build"
echo " cp %{TAR_FILE} %{_builddir}/files.tar.gz"

echo "%install"

echo "rm -rf %{buildroot}"

echo "mkdir -p %{buildroot}"

echo "mv files.tar.gz %{buildroot}/files.tar.gz"

echo "tar -xvf %{buildroot}/files.tar.gz -C %{buildroot}"

echo "rm %{buildroot}/files.tar.gz"

echo "%clean"

echo "%post"

echo "%postun"

echo "%files"
cat $TAR_FILES


}
