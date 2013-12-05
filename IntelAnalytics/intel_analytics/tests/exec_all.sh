#!/bin/bash

DIR="$( cd "$( dirname "$BASH_SOURCE[0]}" )" && pwd )"

source /usr/lib/IntelAnalytics/virtpy/bin/activate
python -m unittest discover $DIR test_*.py
deactivate