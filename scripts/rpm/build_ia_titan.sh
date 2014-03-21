#!/bin/bash +x

. ./versions.sh

set +u
if [ -z "${TITAN_VERSION}" ]; then
    TITAN_VERSION=0.4.2
fi
if [ -z "${TITAN_USER}" ]; then
    TITAN_USER=hadoop
fi
if [ -z "${TITAN_GROUP}" ]; then
    TITAN_GROUP=hadoop
fi
if [ -z "${TRIBECA_VERSION}" ]; then
    TRIBECA_VERSION=0.8.0
fi
if [ -z "${BUILD_NUMBER}" ]; then
    BUILD_NUMBER=0000
fi

rpmbuild --clean --define "_topdir ${PWD}" \
    --define "BUILD_NUMBER $BUILD_NUMBER" \
    --define "TITAN_USER $TITAN_USER" \
    --define "TITAN_GROUP $TITAN_GROUP" \
    --define "TITAN_VERSION $TITAN_VERSION" \
    --define "TRIBECA_VERSION $TRIBECA_VERSION" \
    -bb SPECS/titan-server.spec
