# Nemo

Nemo: Efficient Caching with Index Pushdown for Disaggregated OLTP Databases


### on machine to execute
For all storage types:
```
./BUILD
```
If using Azure Storage Client Library for C++ (7.5.0)
```
cd tools; ./install_azure.sh
```
## Configurations
Nemo's storage engine can be any kv-stores which satisfy the capabilies defined in our paper. 
Connecting urls should be configured to guide Nemo to correctly access the storage service. We use CosmosDB as an example. You can change the configure file "configs/ifconfig\_azure.txt" to your own storage service.


## Compile and Run
generate makefile
```
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
```
expected output
```
-- Generating benchmark tests: 
-- [benchmark test] executable: bench_ycsb
-- Generating unit tests: 
-- [unit test] executable: AllUnitTests
-- [unit test] executable: CCTest
-- [unit test] executable: IndexObjBtreeTest
-- Generating experiments: 
-- [experiment] executable: ExpYCSB
-- Configuring done
-- Generating done
-- Build files have been written to: /users/username/Nemo-test/
```
compile using makefile
```
make -j <desire executable>
```
