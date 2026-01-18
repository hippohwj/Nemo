#!/bin/sh

if [ -z "$1" ]; then
  echo "Usage: $0 <output_filename>"
  exit 1
fi

progstr="ExpYCSB"
#progstr="LoadYCSB"
progpid=`pgrep -o $progstr`
while [ “$progpid” = “” ]; do
  progpid=`pgrep -o $progstr`
done
sudo gdb -p $progpid -batch -ex "thread apply all bt" > thread_dump$1.txt
