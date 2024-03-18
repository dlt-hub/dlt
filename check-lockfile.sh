#!/bin/bash

# searching for the string "hash = " in poetry.lock file will
# help us to verify if the lockfile is in the right order
LOCKFILENAME="poetry.lock"
HASH_STRING="hash = "

# Check if the file exists
if [ ! -f "$LOCKFILENAME" ]; then
    echo "Error: File $LOCKFILENAME does not exist."
    exit 1
fi

# Count the occurrences of the string
COUNT=$(grep -o "$HASH_STRING" "$LOCKFILENAME" | wc -l)

# Check if the string appears less than 100 times, if so, then error
if [ $COUNT -lt 100 ]; then
    echo "Error: The string '$HASH_STRING' does not appear in $LOCKFILENAME, please make sure you are using an up to date poetry version."
    exit 1
else
    echo "The string '$HASH_STRING' appears $COUNT times in $LOCKFILENAME. All good!"
fi

# Exit normally
exit 0