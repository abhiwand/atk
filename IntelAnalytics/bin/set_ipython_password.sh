#!/bin/bash
INPUT=""
PASSWORD1=""
PASSWORD2=""
HASH="sha512"
HASHED=""

function hash()
{
	local hashing=$1
	sha=$(echo "$hashing" | sha512sum | tr -d " -")
        HASHED="$HASH:$sha"
	echo $HASHED
	return 0;
}

function setHash()
{
	sed -i "s/^passwd = .*/passwd = '$HASHED'/g" /usr/lib/IntelAnalytics/conf/ipython_notebook_config.py
	return 0;
}

if read -t 0; then
	read input
	hash $input
	setHash 
else

	if [ "$PASSWORD1" == "" ]; then 
		read -p "Enter IPython Password:" -s PASSWORD1
		echo ""
	fi

	if [ "$PASSWORD2" == "" ]; then
		read -p "Verify IPython Password:" -s PASSWORD2
		echo ""
	fi

	if [ "$PASSWORD1" == "$PASSWORD2" ]; then
		hash $PASSWORD1
		setHash
	else
		echo "Passwords don't match"
		exit 1
	fi

fi
