
sudo apt-get update
sudo apt-get install -y libssl-dev openssl
sudo apt-get install -y software-properties-common
sudo apt-get install -y python-software-properties
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
wget -O - http://llvm.org/apt/llvm-snapshot.gpg.key | sudo apt-key add -
sudo add-apt-repository -y 'deb http://apt.llvm.org/xenial/ llvm-toolchain-xenial-4.0 main'
sudo apt install -y build-essential gcc g++ clang clang++ lldb lld gdb
sudo apt -y install clang-5.0 libc++-dev
sudo apt install -y git
sudo apt install -y vim htop
sudo apt install -y vagrant curl
sudo apt install -y libjemalloc-dev
sudo apt install -y openjdk-8-jre-headless
sudo apt install -y cgroup-tools
sudo apt install -y python3-pip
sudo apt install -y numactl
sudo apt install -y libgtest-dev
sudo apt install -y clang-format
pip3 install --upgrade pip
pip3 install pandas
sudo apt -y install build-essential autoconf libtool pkg-config
sudo apt -y install libgflags-dev
sudo apt install -y numactl
#sudo apt-get install libprotobuf-dev protobuf-compiler
echo "set number" > ~/.vimrc

## allocate more disk space
#sudo mkdir /mydata
#sudo /usr/local/etc/emulab/mkextrafs.pl /mydata
#sudo chmod 777 /mydata

# set installation path
## cwd -- path to Arboretum root
cwd=$(pwd)
export MY_INSTALL_DIR=/home/hippo/mydata
cd $MY_INSTALL_DIR

# update cmake
mkdir -p $MY_INSTALL_DIR/cmake
cd $MY_INSTALL_DIR/
wget -q -O cmake-linux.sh https://github.com/Kitware/CMake/releases/download/v3.19.6/cmake-3.19.6-Linux-x86_64.sh
sudo sh cmake-linux.sh -- --skip-license --prefix=${MY_INSTALL_DIR}/cmake
rm cmake-linux.sh
export PATH=${MY_INSTALL_DIR}/cmake/bin:$PATH
echo "export PATH=${MY_INSTALL_DIR}/cmake/bin:$PATH" >> ${HOME}/.bashrc

# setup a version of jemalloc with profiling enabled
cd ${MY_INSTALL_DIR}
git clone https://github.com/jemalloc/jemalloc.git
cd jemalloc
git checkout master
./autogen.sh --enable-prof
make -j
sudo make install

# set up redis
cd ${MY_INSTALL_DIR}
git clone https://github.com/redis/redis.git
cd redis
make -j
cp ${cwd}/tools/redis*.conf ./
cd
mkdir ${MY_INSTALL_DIR}/redis_data/

## set ssh key permission (for cloudlab set up)
#cd ${cwd}/tools
#chmod +x install_cppredis.sh
#chmod +x install_grpc.sh
#chmod +x install_poco.sh
#chmod +x install_tikv.sh
#chmod +x setup_sshkey.sh
#./setup_sshkey.sh $HOME
#for ((i=1; i<${CLUSTER_SZ}; i++))
#do
#    sudo scp setup_sshkey.sh node-${i}:${HOME}
#    sudo ssh node-${i} ${HOME}/setup_sshkey.sh $HOME
#done

# set up cpu governor to performance
sudo apt install -y cpufrequtils
echo 'GOVERNOR="performance"' | sudo tee /etc/default/cpufrequtils
sudo systemctl disable ondemand

# install azure apis
chmod +x tools/install_azure.sh
./tools/install_azure.sh ${MY_INSTALL_DIR}

