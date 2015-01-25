#! /bin/bash

if [ "$1" == "" ] ; then
    echo \ 
    echo 'Usage: '$0' <term to search for>'
    echo \ 
else
    grep -ril --include=*.scala --exclude-dir=doc "$1" ../
    if [ "$?" == "1" ] ; then echo "$1 not found"; fi
fi
