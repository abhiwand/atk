#!/bin/bash
#
# Generate code coverage numbers for Scala
#
# Scoverage does NOT aggregate reports for multi-module projects so we aggregate ourselves here.
#

echo "assuming combine.sh is being ran from source_code"

# list of modules we want coverage for
<<<<<<< HEAD
scala_coverage_modules="engine-interfaces meta-store engine-spark graphbuilder graphon rest-server launcher IB876
=======
scala_coverage_modules="engine-interfaces meta-store engine graphbuilder graphon rest-server launcher IB876
>>>>>>> 83fb11195ff6bab3bf0874fa161dea075d0335a8
"

# target directory to generate report
report_target=misc/scala-coverage/target/scala-coverage-report

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
