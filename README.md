# Nemo

Nemo: Efficient Caching with Index Pushdown for Disaggregated OLTP Databases

## System Requirements
- OS: Ubuntu 20.04 LTS or higher
- Toolchain: CMake ≥ 3.10, GCC/G++ ≥ 9, Python 3.8+
- Azure / Cosmos DB (Table API):
  - C++: 
    - `azure-storage-cpp` 7.5.0#6
    - azure-storage-blobs-cpp: 12.8.0
    - azure-storage-common-cpp: 12.3.3
  - Python helpers: `azure-storage-blob` ≥ 12.x and `azure-data-tables` ≥ 12.x (PyPI)
- Other dependencies: Boost, jemalloc, OpenSSL, etc.


## Install Dependencies

Install Azure Dependencies
```
cd tools
./install_azure.sh
```

## Configurations
Nemo's data store can be any remote storage services which can satisfy the APIs defined in our paper. 
Connecting urls should be configured to guide Nemo to correctly access the storage service. We use Azure CosmosDB by default. If you want to extend to other storage services, please change the configure file "configs/ifconfig\_azure.txt" to your own storage service and implement interfaces in IDataStore.h accordingly.

## Compile and Run
compile data loading exec
```
./compile_dataload.sh
```

compile experiment exec
```
./compile_exp_release.sh
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
-- Build files have been written to: /users/username/Nemo/
```

## Load Data

After compiling data loading exec, we can generate and load data. Here is an example script to load 10G data. Parameters within cfg file can be changed to load different sizes of data.

```bash
cd build
./LoadYCSB configs/load_data_10g.cfg
```

## End-to-end Experiments
We support multiple types of end-to-end experiments for different scenarios (i.e., positive search, negative search, and phantom protection workloads). 

All scripts for experiments are in this directory: `experiments/`. Please change host IPs and storage connection keys within the scripts to run experiments in your environments. 

Here are example commands to run each type of end-to-end experiments:

### 1. Positive Search

Run an read-only experiment while varying the buffer size
```
cd experiments/
python3 ExpPointTupleVaryBuf.py > logs/ExpPointTupleVaryBuf.log
```

Run an read-write experiment while varying the buffer size
```
cd experiments/
python3 ExpPointTupleVaryBufRW.py > logs/ExpPointTupleVaryBufRW.log
```

Run an read-only experiment while varying skewness
```
cd experiments/
python3 ExpPointTupleUniHotspot.py > logs/ExpPointTupleUniHotspot.log
```

Run an read-write experiment while varying skewness
```
cd experiments/
python3 ExpPointTupleUniHotspotRW.py > logs/ExpPointTupleUniHotspotRW.log
```

### 2. Negative Search

#### (1) Negative Point Search

Run negative point search experiment while varying skewness
```
cd experiements/negative_search
python3 PointVarySkewnessD2g.py > logs/PointVarySkewnessD2g.log
```
Run negative point search experiment while varying negative ratio
```
cd experiements/negative_search
python3 PointVaryDomain03BufferSkewTuple.py > logs/PointVaryDomain03BufferSkewTuple.log
```

#### (2) Range Search

Run (partial negative) range search experiment while varying skewness
```
cd experiements/negative_search
python3 RangeVarySkewnessD2gScatter.py > logs/RangeVarySkewnessD2gScatter.log
```

Run (partial negative) range search experiment while varying range size
```
cd experiements/negative_search
python3 RangeVaryScanLenD2gScatter.py > logs/RangeVaryScanLenD2gScatter.log
```

### 3. Phantom Protection

Run phantom protection experiments (mix workloads of range and insert queries) while varying domain size of the key
```
cd experiements/phantom_protection
python3 PPVaryDomainFixLockTblSize.py > logs/PPVaryDomainFixLockTblSize.log
```

## Code Structure
```
├── README.md                  # Project README file
├── benchmark                  # Code for classic benchmarks (such as YCSB), including data loading, query generation, and benchmark configurations
├── config                     # Configuration files for setting parameters (mainly used for data loading)
├── debug                      # GDB debug tools
├── output                     # Output results of all experiments
├── plots/VLDB26               # Scripts to generate all figures in the paper based on output results 
├── experiments                # End-to-end experiments for typical scenarios
│   ├── ExpYCSB.cpp                # Main entrance of experiments
│   ├── LoadYCSB.cpp               # Main entrance of data loading
│   ├── negative_search            # python scripts to run negative search experiments
│   ├── phantom_protection         # python scripts to run phantom protection experiments  
├── src                        # Core logic for the OLTP testbed 
│   ├── common                 # Common utilities such as global variables, basic data structures, basic types, etc.
│   ├── db                     # Core logic for OLTP-related features, such as data storage, transaction management, and remote storage communication
│   │   ├── access                     # Index interfaces and implementations
│   │   │   ├── IndexBTreeObject.cpp                   # In-memory index for tree-structured record buffer (phantom locks are acquired here). 
│   │   │   ├── IndexBTreePage.cpp                     # Page-based B+tree
│   │   ├── buffer                     # Buffer management (i.e., strategies for cache admission and eviction). Both record-based and page-based managements are implemented
│   │   ├── log                        # Log-related code including writing to WAL, group commit, and log replaying 
│   │   ├── ARDB.cpp                   # DB engine 
│   │   ├── CCManager.cpp              # Responsible for concurrency control (2PL by default). Phantom locks are also implemented here.
│   │   ├── ITxn.cpp                   # Core logic for transaction execution and atomic commit
│   │   │   ...
│   ├── local                  # Compute-layer integration and basic units
│   │   ├── ITable.cpp                 # The entrance for DB-level CRUD. It coordinates core components to produce results
│   │   ├── ITuple.cpp                 # Record Object
│   │   ├── IPage.cpp                  # Page Object
│   ├── remote                 # Codes for accessing remote storage, such as writing logs to the log store, reading from / writing updates to the data store (e.g., CosmosDB)
│   │   ├── ILogStore.cpp                  # Access remote log store
│   │   ├── IDataStore.cpp                 # Access remote data store
│   │   ...
├── tools                      # Scripts for installing dependencies
├── unit_tests                 # Some unit tests
├── compile_exp_release.sh     # Scripts for compiling experiment exec codes
├── compile_dataload.sh        # Scripts for compiling data loading exec codes
├── compile_exp_debug.sh       # Scripts for compiling experiment exec codes (debug version)
```
