#ifndef ARBORETUM_SRC_DB_BUFFER_BUFFERHELPER_H_
#define ARBORETUM_SRC_DB_BUFFER_BUFFERHELPER_H_

namespace arboretum {

/*
 * Buffer state is a single 32-bit variable where following data is combined.
 *
 * - 18 bits refcount
 * - 4 bits usage count
 * - 10 bits of flags
 *
 * Combining these values allows to perform some operations without locking
 * the buffer header, by modifying them together with a CAS loop.
 *
 * The definition of buffer state components is below.
 */
#define BUF_REFCOUNT_ONE 1
#define BUF_REFCOUNT_MASK ((1U << 18) - 1)
#define BUF_USAGECOUNT_MASK 0x003C0000U
#define BUF_USAGECOUNT_ONE (1U << 18)
#define BUF_USAGECOUNT_SHIFT 18
#define BUF_FLAG_MASK 0xFFC00000U


// Get refcount and usage count from buffer state
// refcount: holds the number of PostgreSQL processes currently accessing the associated stored page.
// It is also referred to as pin count. When a process accesses the stored page,
// its refcount must be incremented by 1 (refcount++).
// After accessing the page, its refcount must be decreased by 1 (refcount--).
// When the refcount is zero, i.e. the associated stored page is not currently being accessed,
// the page is unpinned; otherwise it is pinned.
#define BUF_STATE_GET_REFCOUNT(state) ((state) & BUF_REFCOUNT_MASK)
// usage_count:  holds the number of times the associated stored page has been accessed
// since it was loaded into the corresponding buffer pool slot.
#define BUF_STATE_GET_USAGECOUNT(state) (((state) & BUF_USAGECOUNT_MASK) >> BUF_USAGECOUNT_SHIFT)


/*
 * Flags for buffer descriptors
 *
 * Note: BM_TAG_VALID essentially means that there is a buffer hashtable
 * entry associated with the buffer's tag.
 */
#define BM_LOCKED                (1U << 22)    /* buffer header is locked */
#define BM_DIRTY                (1U << 23)    /* data needs writing */
#define BM_VALID                (1U << 24)    /* data is valid */
#define BM_TAG_VALID            (1U << 25)    /* tag is assigned */
#define BM_IO_IN_PROGRESS        (1U << 26)    /* read or write in progress */
#define BM_REPLAYED             (1U << 27)  /* page has been replayed in underlying storage */
#define BM_JUST_DIRTIED            (1U << 28)    /* dirtied since write started */
#define BM_PIN_COUNT_WAITER        (1U << 29)    /* have waiter for sole pin */
#define BM_NOT_IN_INDEX         (1U << 30)    /* can skip deleting index */
#define BM_PERMANENT            (1U << 31)    /* permanent buffer (not unlogged, or init fork) */


/*
 * The maximum allowed value of usage_count represents a tradeoff between
 * accuracy and speed of the clock-sweep buffer management algorithm.  A
 * large value (comparable to NBuffers) would approximate LRU semantics.
 * But it can take as many as BM_MAX_USAGE_COUNT+1 complete cycles of
 * clock sweeps to find a free buffer, so in practice we don't want the
 * value to be very large.
 */
#define BM_MAX_USAGE_COUNT    5
// #define BM_MAX_USAGE_COUNT    1
#define BM_MAX_REF_COUNT 262143

}

#endif //ARBORETUM_SRC_DB_BUFFER_BUFFERHELPER_H_
