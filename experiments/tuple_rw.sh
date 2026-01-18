#!/bin/bash

python3 ExpPointTupleVaryBufRW.py > tuple_rw_buf.out

echo " "
echo "================="
echo " "


python3 ExpPointTupleUniHotspotRW.py > tuple_rw_skew.out

echo " "
echo "================="
echo " "

