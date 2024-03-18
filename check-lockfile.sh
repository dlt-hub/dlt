#!/bin/bash

# searching for the string "hash = " in poetry.lock file will
# help us to verify if the lockfile is in the right order
FILENAME="poetry.lock"
SEARCH_STRING="hash = "

# Check if the file exists
if [ ! -f "$FILENAME" ]; then
    echo "Error: File $FILENAME does not exist."
    exit 1
fi

# Count the occurrences of the string
COUNT=$(grep -o "$SEARCH_STRING" "$FILENAME" | wc -l)

# Check if the string appears less than 10 times
if [ $COUNT -lt 100 ]; then
    echo "Error: The string '$SEARCH_STRING' does not appear in $FILENAME, please make sure you are using an up to date poetry version."
    exit 1
else
    echo "The string '$SEARCH_STRING' appears $COUNT times in $FILENAME."
fi

# Exit normally
exit 0