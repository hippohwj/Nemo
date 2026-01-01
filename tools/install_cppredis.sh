# cpp redis
cd ../include/cpp_redis/
mkdir -p build && cd build
# Generate the Makefile using CMake
cmake .. -DCMAKE_BUILD_TYPE=Release
# Build the library
make -j
# Install the library
sudo make install