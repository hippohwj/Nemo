#!/bin/bash
rm -rf build
mkdir -p build && cd build
#cmake .. -DCMAKE_BUILD_TYPE=Release
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_TOOLCHAIN_FILE=/home/hippo/mydata/vcpkg/scripts/buildsystems/vcpkg.cmake ../
make -j8 LoadYCSB > load_out  2>&1
