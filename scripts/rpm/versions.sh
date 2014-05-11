export TRIBECA_VERSION="0.8.0"
export IAUSER=hadoop

set -e


# Find the absolute path for the given (possibly relative) path
# http://www.linuxquestions.org/questions/programming-9/bash-script-return-full-path-and-filename-680368/page3.html
function abspath {
    if [[ -d "$1" ]]
    then
        pushd "$1" >/dev/null
        pwd
        popd >/dev/null
    elif [[ -e $1 ]]
    then
        pushd $(dirname "$1") >/dev/null
        echo "$(pwd)/$(basename "$1")"
        popd >/dev/null
    else
        echo "$1" does not exist! >&2
        return 127
    fi
}
