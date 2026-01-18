#!/bin/sh
progstr="ExpYCSB"
#progstr="LoadYCSB"
progpid=`pgrep -o $progstr`
while [ “$progpid” = “” ]; do
  progpid=`pgrep -o $progstr`
done
sudo gdb -ex continue -p $progpid
