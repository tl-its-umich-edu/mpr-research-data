#!/bin/sh --
echo "mpr-research-data…"
python mpr-research-data.py

echo "🏁 DONE"

# for debugging: keep container running after program exits
# remove when no longer needed
sleep infinity
