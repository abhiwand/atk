. ./versions.sh
rpmbuild --clean --define "_topdir ${PWD}" --define "BUILD_NUMBER $BUILD_NUMBER" --define "TIMESTAMP $TIMESTAMP" -bb SPECS/intelanalytics.spec
rpmbuild --clean --define "_topdir ${PWD}" --define "BUILD_NUMBER $BUILD_NUMBER" --define "TIMESTAMP $TIMESTAMP" -bb SPECS/intelanalytics-python-deps-localbuild.spec
rpmbuild --clean --define "_topdir ${PWD}" --define "BUILD_NUMBER $BUILD_NUMBER" --define "TIMESTAMP $TIMESTAMP" -bb SPECS/intelanalytics-python.spec
