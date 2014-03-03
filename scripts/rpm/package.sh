export TIMESTAMP=$(date --utc +%Y%m%d%H%M%SZ)
set -e
set -u
./build_ia.sh
./build_ia_python.sh
./build_precompiled_deps.sh
# We do NOT build this one normally, only if there
# is a customer for whom the precompiled one doesn't work
# for some reason.
#./build_localbuild_deps.sh
