. ./versions.sh
rpmbuild --clean --define "_topdir ${PWD}" --define "BUILD_NUMBER $BUILD_NUMBER" --define "TIMESTAMP $TIMESTAMP" -bb SPECS/intelanalytics.spec
rpmbuild --clean --define "_topdir ${PWD}" --define "BUILD_NUMBER $BUILD_NUMBER" --define "TIMESTAMP $TIMESTAMP" -bb SPECS/python-intelanalytics-dependencies.spec
rpmbuild --clean --define "_topdir ${PWD}" --define "BUILD_NUMBER $BUILD_NUMBER" --define "TIMESTAMP $TIMESTAMP" -bb SPECS/python-intelanalytics.spec
