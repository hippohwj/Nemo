#!/bin/bash

python3 ExpScanMixPartitionLockVaryPSize.py > ExpScanMixPartitionLockVaryPSize.out

echo " "
echo "================="
echo " "


#python3 ExpScanMixPartitionLockVaryBuf.py > ExpScanMixPartitionLockVaryBuf.out

echo " "
echo "================="
echo " "

#python3 ExpScanMixVaryBuf.py > ExpScanMixVaryBuf.out

echo " "
echo "================="
echo " "

python3 ExpScanMixPartitionLockVaryZipf.py > ExpScanMixPartitionLockVaryZipf.out

echo " "
echo "================="
echo " "

python3 ExpScanMixVaryZipf.py > ExpScanMixVaryZipf.out

echo " "
echo "================="
echo " "
