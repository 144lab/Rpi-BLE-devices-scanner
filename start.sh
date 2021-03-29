#!/bin/bash

sleep 5

# stop script on error
set -e

. /home/pi/env/bin/activate
python main.py
