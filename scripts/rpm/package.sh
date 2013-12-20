export BUILD_NUMBER=$(date --utc +%Y%m%d%H%M%SZ)
./fetch_sources.sh
./python.sh
./build_rpms.sh
