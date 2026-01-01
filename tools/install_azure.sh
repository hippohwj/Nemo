pip install azure-storage-blob
pip install azure-data-tables
# install boost
export MY_INSTALL_DIR=$1
cd $MY_INSTALL_DIR || exit
wget https://boostorg.jfrog.io/artifactory/main/release/1.82.0/source/boost_1_82_0.tar.bz2
tar --bzip2 -xf boost_1_82_0.tar.bz2
cd boost_1_82_0 || exit
## install boost into /usr/local
./bootstrap.sh
sudo ./b2 install

# install vcpkg
cd $MY_INSTALL_DIR || exit
git clone https://github.com/microsoft/vcpkg
./vcpkg/bootstrap-vcpkg.sh
sudo ln -s $MY_INSTALL_DIR/vcpkg /usr/local/bin/vcpkg

# install azure storage client
./vcpkg/vcpkg install azure-storage-cpp
