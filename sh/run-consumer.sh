#!/bin/bash

# Need to run `chmod +x ./run-consumer.sh` to use this script.

# find abs path
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# run sensors
nohup python3 "$DIR/../src/stream_4.py" & > "$DIR/../log/stream_4.log"
nohup python3 "$DIR/../src/stream_7.py" & > "$DIR/../log/stream_7.log"
nohup python3 "$DIR/../src/stream_8.py" & > "$DIR/../log/stream_8.log"
