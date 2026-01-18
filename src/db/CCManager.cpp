#include "CCManager.h"
#include <iostream>  
#include <thread>
#include "common/GlobalData.h"

namespace arboretum {

static constexpr uint32_t PH_S_MASK     = 0x0000FFFFu;  // S count
static constexpr uint32_t PH_IX_MASK    = 0x7FFF0000u;  // IX count (15 bits)
static constexpr uint32_t PH_SWAIT_BIT  = 0x80000000u;  // S waiter flag



const uint64_t MAX_S_LOCKS = 1;  // Example maximum
const uint64_t IX_SHIFT = ((uint64_t)1) << 32;
std::atomic<uint64_t> waitingIXCount{0};


RC CCManager::AcquireLock(std::atomic<uint32_t> &state, bool is_exclusive) {
  if (g_negative_point_wl) {
    return OK;
  }
  // NO_WAIT impl.
  uint32_t unlocked = 0;
  uint32_t wr_locked = 1;
  if (is_exclusive) {
    return state.compare_exchange_strong(unlocked, wr_locked) ? OK : ABORT;
  }
  // Acquire read lock
  auto current = state.load(std::memory_order_acquire);
  uint32_t rd_locked = current + (1 << 1);
  if (current & 1) {
    return ABORT; // write locked
  }
  // not write locked
  while (!state.compare_exchange_strong(current, rd_locked)) {
    if (current & 1) return ABORT;
    rd_locked = current + (1 << 1);
  }
  return OK;
}

void CCManager::ReleaseLock(std::atomic<uint32_t> &state, bool is_exclusive) {
  if (g_negative_point_wl) {
    return;
  }
  if (is_exclusive) {
    state = 0;
  } else {
    auto prev = state.fetch_sub(1 << 1);
    M_ASSERT(prev > 0, "Releasing a read lock that is not held");
  }
}


RC CCManager::CheckLocked(std::atomic<uint32_t> &state) {
  // if (state > 0) {
  //   // already locked (either in read or write mode)
  //   return ABORT;
  // } else {
  //   // not locked
  //   return OK;
  // }
  return OK;
}

RC CCManager::TryEXLock(std::atomic<uint32_t> &state) {
  if (state > 0) return ABORT;
  uint32_t unlocked = 0;
  uint32_t wr_locked = 1;
  return state.compare_exchange_strong(unlocked, wr_locked) ? OK : ABORT;
}

RC CCManager::AcquirePhantomIntentionLock(std::atomic<uint32_t> &state, bool is_exclusive) {
  ILockType lock_type = is_exclusive ? ILockType::IX : ILockType::S;

  if (lock_type == ILockType::IX) {
    while (true) {
      uint32_t current = state.load(std::memory_order_acquire);

      // If any S is waiting for THIS lock, or S is held, don't allow IX.
      if ((current & PH_SWAIT_BIT) != 0) return ABORT;
      if ((current & PH_S_MASK) != 0) return ABORT;

      // Try to add one IX (ensure we don't overflow 15-bit IX count).
      if ((current & PH_IX_MASK) == PH_IX_MASK) return ABORT;
      uint32_t desired = current + (1u << 16);
      if (state.compare_exchange_strong(current, desired)) return OK;
    }
  } else if (lock_type == ILockType::S) {
    while (true) {
      uint32_t current = state.load(std::memory_order_acquire);
      // If IX is held, mark S-waiting on THIS lock and keep trying (to enforce priority).
      if ((current & PH_IX_MASK) != 0) {
        if ((current & PH_SWAIT_BIT) == 0) {
          uint32_t desired = current | PH_SWAIT_BIT;
          if (!state.compare_exchange_strong(current, desired)) continue;
        }
        std::this_thread::yield();
        continue;
      }

      // No IX held: acquire S and clear S-wait flag if it was set.
      uint32_t desired = (current & ~PH_SWAIT_BIT) + 1u;
      if (state.compare_exchange_strong(current, desired)) return OK;
    }
  } else {
    M_ASSERT(false, "unsupported intention lock type");
    return ABORT;
  }
}



void CCManager::ReleasePhantomIntentionLock(std::atomic<uint32_t> &state, bool is_exclusive) {
  ILockType lock_type = is_exclusive ? ILockType::IX : ILockType::S;
  if (lock_type == ILockType::IX) {
    auto prev = state.fetch_sub(((uint32_t)1) << 16);
    M_ASSERT(prev >= (1u << 16), "Releasing a IX lock that is not held");
    // uint32_t newv = prev - (1u << 16);
    // // If no IX remains, clear the S-wait flag to prevent stale blocking.
    // if ((newv >> 16) == 0) {
    //   state.fetch_and(~PH_SWAIT_BIT);
    // }
  } else if (lock_type == ILockType::S) {
    auto prev = state.fetch_sub(1);
    M_ASSERT(((prev << 16) >> 16) > 0, "Releasing a S lock that is not held");
    // No flag change needed; S clears PH_SWAIT_BIT upon successful acquire.
  } else {
    M_ASSERT(false, "unsupported intention lock type");
  }
}

RC CCManager::AcquireIntentionLock(std::atomic<uint64_t> &state, ILockType lock_type, std::atomic<uint32_t> &wait_state) {
  if (lock_type == ILockType::IX) {
    wait_state.fetch_add(1, std::memory_order_relaxed);
    uint64_t current = state.load(std::memory_order_acquire);

      while (true) {

        if (((current << 32) >> 32) > 0) {  // Check if S lock is held
          // continue;
          // current = state.load(std::memory_order_acquire);
          return ABORT;
        } else {
          uint64_t ix_locked = current + (((uint64_t)1) << 32);
          if (state.compare_exchange_strong(current, ix_locked)) {
            // successfully acquired IX lock
             wait_state.fetch_sub(1, std::memory_order_relaxed);
            return OK;
          }
        }
      }


  } else if (lock_type == ILockType::S) {
     // Acquire S lock
        uint64_t current = state.load(std::memory_order_acquire);

        while (true) {
            
            if ((current >> 32) > 0) {  // Check if IX lock is held
                return ABORT;
            }
            
            uint64_t s_lock_count = (current << 32) >> 32;
            uint64_t s_locked = current + 1;
            if (state.compare_exchange_strong(current, s_locked)) {
                return OK;
            }
        }

  } else {
    M_ASSERT(false, "unsupported intention lock type");
    return ABORT;
  }
}

void CCManager::ReleaseIntentionLock(std::atomic<uint64_t> &state, ILockType lock_type) {
    //   auto prev = state.fetch_sub(1 << 1);
    // M_ASSERT(prev > 0, "Releasing a read lock that is not held");
  if (lock_type == ILockType::IX) {
    auto prev = state.fetch_sub(((uint64_t)1) << 32);
    M_ASSERT(prev >= ( 1<<32 ), "Releasing a IX lock that is not held");
  } else if (lock_type == ILockType::S) {
    auto prev = state.fetch_sub(1);
    M_ASSERT((prev << 32 >> 32)> 0, "Releasing a S lock that is not held");
  } else {
    M_ASSERT(false, "unsupported intention lock type");
  }
  
}






} // arboretum