#!/bin/bash
#
# Generate code coverage numbers for Scala
#
# Scoverage ONLY works with one module at a time.  It was NOT designed to work with multi-module projects so we
# have to go through extra lengths to get correct output.
#
# The broken behavior was coverage would be reported but it was inaccurate because part of the measurements for
# graphbuilder-3 were written to engine-spark or vice-versa. This was noticed especially with any test extending
# TestingSparkContextWordSpec or other test clases in testutils.
#
# Assumes you are running from source_code and that "mvn install" has already been done
#

# maven profiles we want active
mvn_profiles="-Pcompile,test,modules-all,scala-coverage "

# maven command for generating coverage
mvn_scala_coverage="mvn $mvn_profiles clean test scoverage:report"

# list of modules we want coverage for
scala_coverage_modules="interfaces shared engine-spark graphbuilder-3 graphon api-server launcher imllib"

# target directory to generate report
report_target=scala-coverage/target/scala-coverage-report

# make sure old folder is gone
rm -rf ${report_target}

# re-create target folder
mkdir -p ${report_target}

# copy resources into report
cp -r scala-coverage/src/main/resources/* ${report_target}

# build all of the modules without scoverage
mvn -DskipTests -Pcompile,test,modules-all install

for module in `echo $scala_coverage_modules`
do
  if [ -e $module ]
  then
    # only one module at a time can be ran with scoverage otherwise you get bad numbers
    cd $module
    $mvn_scala_coverage

    # fix issue with links in overview.html
    sed -i 's:a href=".*com/intel/:a href="com/intel/:g' target/scoverage-report/overview.html

    # save coverage report to code-coverage project
    cp -r target/scoverage-report ../${report_target}/${module}-scoverage-report

    # make doubly sure instrumented scoverage classes are gone
    rm -rf target
    cd ..
  fi
done
