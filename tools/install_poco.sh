mkdir -p ../third_party
cd ../third_party || exit
if [ ! -d "poco" ]
then
  git clone -b master https://github.com/pocoproject/poco.git
  cd poco || exit
  mkdir cmake-build
  cd cmake-build
  cmake ..
  sudo $(which cmake) --build . --target install
fi
