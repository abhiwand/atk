. ./versions.sh
rpmbuild --clean --define "_topdir ${PWD}" --define "BUILD_NUMBER $TIMESTAMP" -bb SPECS/intelanalytics.spec
rpmbuild --clean --define "_topdir ${PWD}" --define "BUILD_NUMBER $TIMESTAMP" -bb SPECS/python-intelanalytics-dependencies.spec
rpmbuild --clean --define "_topdir ${PWD}" --define "BUILD_NUMBER $TIMESTAMP" -bb SPECS/python-intelanalytics.spec
