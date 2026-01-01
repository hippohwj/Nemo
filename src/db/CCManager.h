#ifndef ARBORETUM_SRC_DB_CCMANAGER_H_
#define ARBORETUM_SRC_DB_CCMANAGER_H_

#include <Types.h>
namespace arboretum {
class CCManager {
 public:
  static RC TryEXLock(std::atomic<uint32_t> &state);
  static RC CheckLocked(std::atomic<uint32_t> &state);
  static RC AcquireLock(std::atomic<uint32_t> &state, bool is_exclusive);
  static void ReleaseLock(std::atomic<uint32_t> &state, bool is_exclusive);
  static RC AcquirePhantomIntentionLock(std::atomic<uint32_t> &state, bool is_exclusive);
  static void ReleasePhantomIntentionLock(std::atomic<uint32_t> &state, bool is_exclusive);

  static RC AcquireIntentionLock(std::atomic<uint64_t> &state, ILockType lock_type, std::atomic<uint32_t> &wait_state);
  static void ReleaseIntentionLock(std::atomic<uint64_t> &state, ILockType lock_type);

};
}

#endif //ARBORETUM_SRC_DB_CCMANAGER_H_
