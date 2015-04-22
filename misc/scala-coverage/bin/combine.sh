#!/bin/bash
#
# Generate code coverage numbers for Scala
#
# Scoverage does NOT aggregate reports for multi-module projects so we aggregate ourselves here.
#

echo "assuming combine.sh is being ran from source_code"

# list of modules we want coverage for
scala_coverage_modules="interfaces shared engine-spark graphbuilder-3 graphon api-server launcher imllib"

# target directory to generate report
report_target=scala-coverage/target/scala-coverage-report

# make sure old folder is gone
rm -rf ${report_target}

# re-create target folder
mkdir -p ${report_target}

# copy resources into report
cp -r misc/scala-coverage/src/main/resources/* ${report_target}

for module in `echo $scala_coverage_modules`
do
  if [ -e $module ]
  then
    # only one module at a time can be ran with scoverage otherwise you get bad numbers
    cd $module

    # fix issue with links in overview.html
    sed -i 's:a href=".*com/intel/:a href="com/intel/:g' target/site/scoverage/overview.html

    # save coverage report to code-coverage project
    cp -r target/site/scoverage ../${report_target}/${module}-scoverage-report

    cd ..
  fi
done
