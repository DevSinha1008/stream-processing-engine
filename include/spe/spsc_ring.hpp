#pragma once
// spsc_ring.hpp — Lock-free single-producer/single-consumer ring buffer.
//
// Design notes (the things you must be able to defend):
//
// 1. SPSC: exactly one producer thread and one consumer thread. This
//    restriction is what makes a lock-free design simple and fast: each
//    index (head/tail) is written by exactly one thread, so we never need
//    compare-and-swap, only atomic loads/stores with the right ordering.
//
// 2. Memory ordering: the producer publishes an event by writing the slot
//    THEN storing head with memory_order_release. The consumer loads head
//    with memory_order_acquire before reading the slot. Release/acquire
//    forms a synchronizes-with edge: everything written before the release
//    store is visible after the acquire load. Sequential consistency
//    (the default) would also be correct but costs more on x86/ARM because
//    it forbids more reordering than we need.
//
// 3. False sharing: head and tail live on separate cache lines
//    (alignas(64)). If they shared a line, every producer write to head
//    would invalidate the consumer's cached copy of tail and vice versa —
//    "false sharing" — causing constant cache-coherence traffic for
//    logically independent variables.
//
// 4. Power-of-two capacity: index wrap uses `& mask_` instead of `%`,
//    because integer modulo is one of the slowest ALU ops in the hot path.
//
// 5. Index caching (the batched-atomics optimisation, CV bullet 2):
//    In the naive version, every push loads `tail` and every pop loads
//    `head` — an atomic load of a cache line owned by the *other* core,
//    i.e. coherence traffic per event. The optimised version caches the
//    other side's index locally and only re-loads it when the cached view
//    says the buffer looks full/empty. Combined with batch publish
//    (process N events, store the index once), the number of cross-core
//    atomic operations drops by ~the batch factor. That amortisation of
//    synchronisation cost is where the [A]% gain comes from.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>          // std::hardware_destructive_interference_size
#include <type_traits>
#include <vector>

namespace spe {

#ifdef __cpp_lib_hardware_interference_size
inline constexpr std::size_t kCacheLine = std::hardware_destructive_interference_size;
#else
inline constexpr std::size_t kCacheLine = 64;
#endif

// ---------------------------------------------------------------------------
// Naive SPSC ring: correct, lock-free, but does one acquire-load of the
// other side's index per operation. This is the baseline the batched
// version is measured against for the [A]% figure.
// ---------------------------------------------------------------------------
template <typename T>
class SpscRingNaive {
    static_assert(std::is_nothrow_move_assignable_v<T>);
public:
    explicit SpscRingNaive(std::size_t capacity_pow2)
        : mask_(capacity_pow2 - 1), buf_(capacity_pow2) {
        // capacity must be a power of two for mask indexing
        if (capacity_pow2 == 0 || (capacity_pow2 & mask_) != 0)
            throw std::invalid_argument("capacity must be a power of two");
    }

    // Returns false if full (backpressure is the caller's problem).
    bool try_push(T v) noexcept {
        const uint64_t h = head_.load(std::memory_order_relaxed);   // we own head
        const uint64_t t = tail_.load(std::memory_order_acquire);   // other core owns tail
        if (h - t > mask_) return false;                            // full
        buf_[h & mask_] = std::move(v);
        head_.store(h + 1, std::memory_order_release);              // publish
        return true;
    }

    bool try_pop(T& out) noexcept {
        const uint64_t t = tail_.load(std::memory_order_relaxed);   // we own tail
        const uint64_t h = head_.load(std::memory_order_acquire);   // other core owns head
        if (t == h) return false;                                   // empty
        out = std::move(buf_[t & mask_]);
        tail_.store(t + 1, std::memory_order_release);
        return true;
    }

    std::size_t capacity() const noexcept { return mask_ + 1; }

private:
    const uint64_t mask_;
    std::vector<T> buf_;
    alignas(kCacheLine) std::atomic<uint64_t> head_{0}; // producer-owned
    alignas(kCacheLine) std::atomic<uint64_t> tail_{0}; // consumer-owned
};

// ---------------------------------------------------------------------------
// Optimised SPSC ring: index caching + batched publication.
//   - try_push only re-loads `tail` when its cached copy suggests full.
//   - try_pop only re-loads `head` when its cached copy suggests empty.
//   - push_batch/pop_batch move N items with ONE release-store of the index,
//     amortising the synchronisation cost across the batch.
// ---------------------------------------------------------------------------
template <typename T>
class SpscRing {
    static_assert(std::is_nothrow_move_assignable_v<T>);
public:
    explicit SpscRing(std::size_t capacity_pow2)
        : mask_(capacity_pow2 - 1), buf_(capacity_pow2) {
        if (capacity_pow2 == 0 || (capacity_pow2 & mask_) != 0)
            throw std::invalid_argument("capacity must be a power of two");
    }

    bool try_push(T v) noexcept {
        const uint64_t h = head_.load(std::memory_order_relaxed);
        if (h - tail_cache_ > mask_) {                        // looks full: refresh view
            tail_cache_ = tail_.load(std::memory_order_acquire);
            if (h - tail_cache_ > mask_) return false;        // genuinely full
        }
        buf_[h & mask_] = std::move(v);
        head_.store(h + 1, std::memory_order_release);
        return true;
    }

    // Push up to n items from src; returns how many were pushed.
    // ONE atomic release-store regardless of how many items went in.
    std::size_t push_batch(const T* src, std::size_t n) noexcept {
        const uint64_t h = head_.load(std::memory_order_relaxed);
        uint64_t free_slots = capacity() - (h - tail_cache_);
        if (free_slots < n) {
            tail_cache_ = tail_.load(std::memory_order_acquire);
            free_slots = capacity() - (h - tail_cache_);
        }
        const std::size_t m = n < free_slots ? n : free_slots;
        for (std::size_t i = 0; i < m; ++i)
            buf_[(h + i) & mask_] = src[i];
        if (m) head_.store(h + m, std::memory_order_release);  // single publish
        return m;
    }

    bool try_pop(T& out) noexcept {
        const uint64_t t = tail_.load(std::memory_order_relaxed);
        if (t == head_cache_) {                               // looks empty: refresh view
            head_cache_ = head_.load(std::memory_order_acquire);
            if (t == head_cache_) return false;               // genuinely empty
        }
        out = std::move(buf_[t & mask_]);
        tail_.store(t + 1, std::memory_order_release);
        return true;
    }

    // Pop up to n items into dst; returns how many were popped.
    // ONE atomic release-store regardless of batch size.
    std::size_t pop_batch(T* dst, std::size_t n) noexcept {
        const uint64_t t = tail_.load(std::memory_order_relaxed);
        uint64_t avail = head_cache_ - t;
        if (avail < n) {
            head_cache_ = head_.load(std::memory_order_acquire);
            avail = head_cache_ - t;
        }
        const std::size_t m = n < avail ? n : avail;
        for (std::size_t i = 0; i < m; ++i)
            dst[i] = std::move(buf_[(t + i) & mask_]);
        if (m) tail_.store(t + m, std::memory_order_release);  // single publish
        return m;
    }

    std::size_t capacity() const noexcept { return mask_ + 1; }

private:
    const uint64_t mask_;
    std::vector<T> buf_;
    alignas(kCacheLine) std::atomic<uint64_t> head_{0};
    alignas(kCacheLine) uint64_t head_cache_{0};   // consumer's cached view of head
    alignas(kCacheLine) std::atomic<uint64_t> tail_{0};
    alignas(kCacheLine) uint64_t tail_cache_{0};   // producer's cached view of tail
};

} // namespace spe
