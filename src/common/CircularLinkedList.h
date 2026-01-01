#ifndef ARBORETUM_SRC_COMMON_CIRCULARLINKEDLIST_H_
#define ARBORETUM_SRC_COMMON_CIRCULARLINKEDLIST_H_

namespace arboretum {
// singly linked with tail

template<typename T>
static void ClockInsert(T *& hand, T *& tail, T * new_node) {
  //std::lock_guard<std::mutex> lk(mutex);
  if (!hand) {
    hand = new_node;
    tail = new_node;
    hand->clock_next_ = tail;
    tail->clock_next_ = hand;
  } else {
    tail->clock_next_ = new_node;
    new_node->clock_next_ = hand;
    tail = new_node;
  }
}

template<typename T>
static void ClockRemoveHand(T *& hand, T *& tail, std::atomic<unsigned long>& cnt) {
  M_ASSERT(hand, "hand must be not null!");
  //std::lock_guard<std::mutex> lk(mutex);
  if (hand == tail) {
    // size == 1
    hand = nullptr;
    tail = nullptr;
  } else {
    // size > 1
    hand = hand->clock_next_;
    tail->clock_next_ = hand;
  }
  cnt--;
}

template<typename T>
static void ClockRemoveHand(T *& hand, T *& tail) {
  M_ASSERT(hand, "hand must be not null!");
  //std::lock_guard<std::mutex> lk(mutex);
  if (hand == tail) {
    // size == 1
    hand = nullptr;
    tail = nullptr;
  } else {
    // size > 1
    hand = hand->clock_next_;
    tail->clock_next_ = hand;
  }
}



template<typename T>
static void ClockAdvance(T *& hand, T *& tail) {
  //std::lock_guard<std::mutex> lk(mutex);
  // LOG_DEBUG("XXX clock advanced");
  tail = hand;
  hand = hand->clock_next_;
}
} // arboretum

#endif //ARBORETUM_SRC_COMMON_CIRCULARLINKEDLIST_H_
