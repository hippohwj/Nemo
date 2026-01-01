#!/bin/bash
rm -rf build
mkdir -p build && cd build
#cmake .. -DCMAKE_BUILD_TYPE=Release
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_TOOLCHAIN_FILE=/home/hippo/mydata/vcpkg/scripts/buildsystems/vcpkg.cmake ../
make -j ExpYCSB > exp_out  2>&1
#make -j LoadYCSB > load_out  2>&1
