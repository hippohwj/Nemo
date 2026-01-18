mkdir -p ../third_party
cd ../third_party || exit
if [ ! -d "grpc" ]
then
  git clone -b v1.54.x https://github.com/grpc/grpc
  cd grpc || exit
  git submodule update --init
  cd ..
fi
cp ../tools/run_distrib_test_cmake.sh grpc/test/distrib/cpp || exit
cd grpc/test/distrib/cpp || exit
./run_distrib_test_cmake.sh
