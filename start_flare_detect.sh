#!/bin/bash
# Wrapper to run flare detection in a named screen session
export PYTHONPATH=/home/user/test_svn/python:/common/python/current:/common/python:/common/python/packages/pipeline

SESSION="flare_detection"
SCRIPT="/common/python/current/find_flare4date.py"
LOG="/tmp/flare_detection.log"

# Check if session already exists
if screen -list | grep -q "$SESSION"; then
    echo "Screen session $SESSION already running"
    exit 0
fi

# Start screen session detached
screen -dmS $SESSION bash -c "/common/anaconda2/bin/python $SCRIPT >> $LOG 2>&1"