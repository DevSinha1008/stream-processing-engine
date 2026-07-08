#pragma once
// mutex_queue.hpp — The baseline for the [Z]x throughput comparison.
//
// A perfectly reasonable, correct bounded queue guarded by a std::mutex.
// This is what "just use a lock" looks like. It is NOT a strawman: it uses
// the same ring storage, power-of-two mask, and API shape as the lock-free
// versions, so the measured difference is attributable to the
// synchronisation strategy, not to unrelated implementation details.
// (If your baseline is artificially bad, your [Z]x figure is dishonest and
// an interviewer will smell it. A fair baseline is the whole point.)

#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <vector>

namespace spe {

template <typename T>
class MutexQueue {
public:
    explicit MutexQueue(std::size_t capacity_pow2)
        : mask_(capacity_pow2 - 1), buf_(capacity_pow2) {
        if (capacity_pow2 == 0 || (capacity_pow2 & mask_) != 0)
            throw std::invalid_argument("capacity must be a power of two");
    }

    bool try_push(T v) {
        std::lock_guard<std::mutex> g(m_);
        if (head_ - tail_ > mask_) return false;
        buf_[head_ & mask_] = std::move(v);
        ++head_;
        return true;
    }

    bool try_pop(T& out) {
        std::lock_guard<std::mutex> g(m_);
        if (tail_ == head_) return false;
        out = std::move(buf_[tail_ & mask_]);
        ++tail_;
        return true;
    }

    std::size_t capacity() const noexcept { return mask_ + 1; }

private:
    const uint64_t mask_;
    std::vector<T> buf_;
    std::mutex m_;
    uint64_t head_{0};
    uint64_t tail_{0};
};

} // namespace spe
