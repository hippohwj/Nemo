#ifndef ARBORETUM_SRC_COMMON_UTILS_H_
#define ARBORETUM_SRC_COMMON_UTILS_H_

#include <iostream>
#include <string>

namespace arboretum {

static bool StrEndsWith(const std::string& str, const std::string& suffix) {
    if (str.length() < suffix.length()) {
        return false;
    }
    size_t pos = str.rfind(suffix);
    if (pos == std::string::npos) {
        return false; // Not found
    }
    return pos == (str.length() - suffix.length());
}

static int GetPartitionID(uint64_t key) {
    return key/g_partition_covering_lock_unit_sz;
}


}

#endif //ARBORETUM_SRC_COMMON_UTILS_H_
