#!/bin/bash - 
#===============================================================================
#
#          FILE: check_rst.sh
# 
#         USAGE: ./check_rst.sh 
# 
#   DESCRIPTION: 
# 
#       OPTIONS: ---
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: Mark Aldrich (rma), robertx.m.aldrich@intel.com
#  ORGANIZATION: 
#       CREATED: 02/15/2015 14:24
#      REVISION:  ---
#
#  This script is to examine/edit .rst files
#  If run without parameters, it should process all files
#  If run with a string, it should process only those files containing that string
#  If run with the flag "-resume" it should continue from the last place it was
#===============================================================================
if [[ $# -gt 0 ]]
then
    FLAG1=$1
else
    FLAG1=""
fi
CONTINUE=True
START_AT=""
if [ "$FLAG1" == "-resume" ]
then
    if [[ -f ~/check_rst_progress.txt ]]
    then
        START_AT=$(cat ~/check_rst_progress.txt)
    else
        read -p "File ~/check_rst_progress.txt not found. Continue from begining? [Y|n]" USER_RESPONSE
        if [ "$USER_RESPONSE" == "" ]
        then
            USER_RESPONSE='Y'
        fi
        if [ "$USER_RESPONSE" != "y" -a "$USER_RESPONSE" != "Y" ]
        then
            CONTINUE=False
        fi
    fi
fi

if [ "$CONTINUE" == "True" ]
then
    for FILE in $(find /home/work/source_code/api-doc/src/main/resources/python -name "*.rst")
    do
        if [ "$START_AT" == "" -o "$START_AT" == "$FILE" ]
        then
            if [ "$START_AT" != "" ]
            then
                START_AT=""
            fi
            if [ "$CONTINUE" == "True" ]
            then
                vim $FILE
                if [ "$?" != "0" ]
                then
                    CONTINUE=False
                    LAST_FILE=$FILE
                    echo $FILE > ~/check_rst_progress.txt
                fi
            fi
        fi
    done


    if [ "$CONTINUE" == "True" ]
    then
        echo "All files processed."
    else
      echo "Last file processed: $LAST_FILE. Use $0 -resume to resume with this file."
    fi
fi
