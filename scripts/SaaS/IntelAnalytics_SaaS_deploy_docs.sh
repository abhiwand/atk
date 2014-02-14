#!/bin/sh
#will clone the repo, build the root project, generate docs and copy them to S3
#Prerequisites:
# java sdk
# maven
# GAO git repo access through ssh key
# installed aws cli (http://docs.aws.amazon.com/cli/latest/index.html) tools and configured with our default region
# installed epydoc for build python docs

#validate the command line options
TEMP=`getopt -o d:b: --long dir:,branch: -n 'IntelAnalytics_Saas_deploy_docs.sh' -- "$@"`

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

eval set -- "$TEMP"

#our s3 documentation bucket
DOC_BUCKET=s3://docs.graphtrial.intel.com

REPO="ssh://git@stash-fm-prod.devtools.intel.com:7999/trib/source_code.git"

while true; do
    case "$1" in
              -d|--dir)
                echo "Option d/dir, argument '$2'"
                OUTPUT=$2
                shift 2 ;;
              -b|--branch)
                echo "Option b/branch, argument '$2'"
                BRANCH=$2
                shift 2 ;;
              --) shift ; break ;;
              *) echo "Internal error!" ; exit 1 ;;
    esac
done

if [ $OUTPUT == ""] ; then echo "no ouput directory"; exit 1; fi

if [ $BRANCH == ""] ; then echo "no branch"; exit 1; fi

echo $OUTPUT
echo $BRANCH

echo "clean up old output directory"
rm -rf $OUTPUT

echo "clone repo source_code:"
git clone $REPO $OUTPUT

echo "checkout branch"
cd $OUTPUT; git checkout $BRANCH

echo "build source code skip tests, generate java docs "
cd $OUTPUT; mvn package -DskipTests; mvn javadoc:jar

echo "extract GB api docs"
unzip $OUTPUT/graphbuilder/target/graphbuilder-2.0-javadoc.jar -d $OUTPUT/javaapi

echo "remove old java api files"
aws s3 rm $DOC_BUCKET/javaapi/ --recursive

echo "copy GB java api docs to S3"
aws s3 mv $OUTPUT/javaapi $DOC_BUCKET/javaapi/ --recursive

echo "build python docs"
cd $OUTPUT/IntelAnalytics; epydoc --html -o $OUTPUT/pythonapi intel_analytics --no-sourcecode --no-private --parse-only

echo "remove old python api docs"
aws s3 rm $DOC_BUCKET/pythonapi/ --recursive

echo "copy python api docs to s3"
aws s3 mv $OUTPUT/pythonapi $DOC_BUCKET/pythonapi/ --recursive

echo "clean up old output directory"
rm -rf $OUTPUT
