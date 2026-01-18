#ifndef ARBORETUM_SRC_COMMON_COUNTDOWNLATCH_H_
#define ARBORETUM_SRC_COMMON_COUNTDOWNLATCH_H_

#include <mutex>
#include <condition_variable>
using namespace std;

namespace arboretum {

struct CountDownLatch {
  public:
  explicit CountDownLatch(size_t init_count): count_(init_count) {

  }

  void CountDown() {
    unique_lock<mutex> lock(latch_);
    count_--;
    if (count_ == 0) {
      cv_.notify_all();
    }
  }

  void Wait() {
    unique_lock<mutex> lock(latch_);
    cv_.wait(lock, [this]{return this->count_ == 0;});
  }

  private:
  mutex latch_;
  condition_variable cv_;
  size_t count_;
};


}

#endif //ARBORETUM_SRC_COMMON_COUNTDOWNLATCH_H_
