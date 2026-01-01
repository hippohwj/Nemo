pwd=$(pwd)
export MY_INSTALL_DIR=/mydata
mkdir -p ${MY_INSTALL_DIR}/tikv/
# install protoc
#PROTOC_ZIP=protoc-3.14.0-linux-x86_64.zip
#curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v3.14.0/$PROTOC_ZIP
#sudo unzip -o $PROTOC_ZIP -d /usr/local bin/protoc
#sudo unzip -o $PROTOC_ZIP -d /usr/local 'include/*'
#rm -f $PROTOC_ZIP
# which protoc: /usr/local/bin/
# install tiup
cd ${MY_INSTALL_DIR}/tikv/ || exit
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
# install cluster
source ${HOME}/.bashrc
tiup cluster
tiup update --self && tiup update cluster
tiup --binary cluster
# Prepare the TiUP offline component package
version=v6.5.0
osv=linux  # only {darwin, linux} are supported
archv=amd64 # only {amd64, arm64} are supported
tiup mirror clone tidb-community-server-${version}-${osv}-${archv} ${version} --os=${osv} --arch=${archv}
tar czvf tidb-community-server-${version}-${osv}-${archv}.tar.gz tidb-community-server-${version}-${osv}-${archv}
# Deploy the offline TiUP component
tar xzvf tidb-community-server-${version}-${osv}-${archv}.tar.gz && \
sh tidb-community-server-${version}-${osv}-${archv}/local_install.sh && \
source ${HOME}/.bashrc
# check for risk
tiup cluster check ${pwd}/topology.yaml -i ~/.ssh/id_rsa
tiup cluster check ${pwd}/topology.yaml --apply -i ~/.ssh/id_rsa
tiup cluster check ${pwd}/topology.yaml -i ~/.ssh/id_rsa
# deploy and start
tiup cluster deploy ab-datastore ${version} ${pwd}/topology.yaml -i ~/.ssh/id_rsa
tiup cluster start ab-datastore --init
# display status
tiup cluster display ab-datastore
# destroy cluster instance
# tiup cluster stop ab-datastore
# tiup cluster destroy ab-datastore

