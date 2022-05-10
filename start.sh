#!/bin/sh --
echo "Starting mpr-research-data.py..."
date --iso-8601=seconds

python mpr-research-data.py

echo "🏁 - Python Script Execution Complete."
date --iso-8601=seconds

# for debugging: keep container running after program exits
# remove when no longer needed
#sleep infinity
