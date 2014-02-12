export TIMESTAMP=$(date --utc +%Y%m%d%H%M%SZ)
if [ "$BUILD_NUMBER" == "" ]
then
    export BUILD_NUMBER=$TIMESTAMP
fi

./fetch_sources.sh
./python.sh
./build_rpms.sh
