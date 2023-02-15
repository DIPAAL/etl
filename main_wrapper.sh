#!/bin/bash

# This is a shell script that should wrap the python script to avoid memory leaks

# find the argument --from_date and --to_date of the format yyyy-mm-dd. If not set exit
from_date=$(echo "$@" | grep -o -- '--from_date [0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}' | cut -d ' ' -f 2)
to_date=$(echo "$@" | grep -o -- '--to_date [0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}' | cut -d ' ' -f 2)

if [ -z "$from_date" ] || [ -z "$to_date" ]; then
    echo "Please provide --from_date and --to_date arguments"
    exit 1
fi

# create argument list without the --from_date and --to_date arguments
args=$(echo "$@" | sed -e 's/--from_date [0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}//g' -e 's/--to_date [0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}//g')

# loop through the dates, and run the python script with the rest of the given arguments
while [ "$from_date" != "$to_date" ]; do
    echo "Running for $from_date"

    # run python and redirect stdout and stderr to stdout and stderr of this script
    python3 -u main.py $args --from_date $from_date --to_date $from_date 1>&1 2>&1
    from_date=$(date -d "$from_date + 1 day" +%Y-%m-%d)
done
