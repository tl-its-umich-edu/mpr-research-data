#!/bin/sh --
echo "Starting dbToBucketScript.py..."

python dbToBucketScript.py

echo "🏁 - Python Script Execution Complete."

# for debugging: keep container running after program exits
# remove when no longer needed
#sleep infinity
