#ifndef ARBORETUM_SRC_COMMON_COMMON_H_
#define ARBORETUM_SRC_COMMON_COMMON_H_

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <thread>
#include <unistd.h>
#include <random>
#include <execinfo.h>
#include <unistd.h>


#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "GlobalData.h"
#include "SharedPtr.h"

using namespace std;

namespace arboretum {
inline uint64_t GetSystemClock() {
  // in nanosecond
#if defined(__i386__)
  uint64_t ret;
  __asm__ __volatile__("rdtsc" : "=A" (ret));
  return ret;
#elif defined(__x86_64__)
  unsigned hi, lo;
  __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
  uint64_t ret = ((uint64_t) lo) | (((uint64_t) hi) << 32);
  ret = (uint64_t) ((double) ret / g_cpu_freq); // nano second
  return ret;
#else
  LOG_ERROR("Instruction set architecture is not supported yet.");
#endif
}

inline void CalculateCPUFreq() {
  // measure CPU Freqency
  auto tp = NEW(timespec);
  clock_gettime(CLOCK_REALTIME, tp);
  uint64_t start_t = tp->tv_sec * 1000000000 + tp->tv_nsec;
  auto starttime = GetSystemClock();
  sleep(1);
  auto endtime = GetSystemClock();
  clock_gettime(CLOCK_REALTIME, tp);
  auto end_t = tp->tv_sec * 1000000000 + tp->tv_nsec;
  auto runtime = end_t - start_t;
  g_cpu_freq = 1.0 * (endtime - starttime) * g_cpu_freq / runtime;
  LOG_INFO("CPU freqency is %.2f", g_cpu_freq);
}


inline void printStackTrace() {
    void *array[100];
    size_t size;
    char **strings;
    size_t i;

    size = backtrace(array, 100);
    strings = backtrace_symbols(array, size);

    LOG_INFO("Obtained %zd stack frames.\n", size);

    for (i = 0; i < size; i++)
        LOG_INFO("%s\n", strings[i]);

    free(strings);
}

}

#endif //ARBORETUM_SRC_COMMON_COMMON_H_
