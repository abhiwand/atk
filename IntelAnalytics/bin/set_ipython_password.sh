#!/bin/bash
PASSWORD1=""
PASSWORD2=""
HASH="sha512"
if [ "$PASSWORD1" == "" ]; then 
	read -p "Enter IPython Password:" -s PASSWORD1
	echo ""
fi

if [ "$PASSWORD2" == "" ]; then
	read -p "Verify IPython Password:" -s PASSWORD2
	echo ""
fi

if [ "$PASSWORD1" == "$PASSWORD2" ]; then
	sha=$(echo "$PASSWORD1" | sha512sum | tr -d " -")
	echo "$HASH:$sha"
	#sed -i "s/^passwd = .*/passwd = '$HASH:$sha'/g" /usr/lib/IntelAnalytics/conf/ipython_notebook_config.py
	else
	echo "Passwords don't match"
fi


