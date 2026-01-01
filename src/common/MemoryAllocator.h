#ifndef ARBORETUM_SRC_COMMON_MEMORYALLOCATOR_H_
#define ARBORETUM_SRC_COMMON_MEMORYALLOCATOR_H_

#include <jemalloc/jemalloc.h>

namespace arboretum {
class MemoryAllocator {
 public:
  static void *Alloc(size_t size, size_t alignment = 0) {
    void * addr;
    if (alignment == 0) {
      addr = malloc(size);
    } else {
      addr = aligned_alloc(alignment, size);
    }
    if (!addr) LOG_ERROR("Out of memory!");
    return addr;
  };
  static void Dealloc(void *ptr) { 
    if (ptr) {
       free(ptr); 
    }
    };
};
}

#endif //ARBORETUM_SRC_COMMON_MEMORYALLOCATOR_H_
